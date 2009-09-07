"""

Flower Components

"""
from Queue import Empty as QueueEmpty
from Queue import Queue
from datetime import datetime
from flower.components import FlowerComponent
from carrot.connection import DjangoBrokerConnection, AMQPConnectionException
from celery import conf
from celery.worker.job import TaskWrapper
from celery.messaging import get_consumer_set
from celery.exceptions import NotRegistered
from celery.utils import retry_over_time
from celery.datastructures import SharedCounter
from celery.backends import default_periodic_status_backend
import traceback
import time
import socket


class AMQPListener(object):
    """Listen for messages received from the AMQP broker and
    move them the the bucket queue for task processing.

    :param bucket_queue: See :attr:`bucket_queue`.
    :param hold_queue: See :attr:`hold_queue`.

    .. attribute:: bucket_queue

        The queue that holds tasks ready for processing immediately.

    .. attribute:: hold_queue

        The queue that holds paused tasks. Reasons for being paused include
        a countdown/eta or that it's waiting for retry.

    .. attribute:: logger

        The logger used.

    """

    def __init__(self, bucket_queue, hold_queue, concurrency, task_callback,
        logger=None):
        self.bucket_queue = bucket_queue
        self.hold_queue = hold_queue
        self.concurrency = concurrency
        self.task_callback = task_callback
        self.logger = logger
        self.amqp_connection = None
        self.task_consumer = None
        self.prefetch_count = SharedCounter(self.concurrency)

    def start(self):
        """Start the consumer.

        If the connection is lost, it tries to re-establish the connection
        over time and restart consuming messages.

        """

        while True:
            self.reset_connection()
            try:
                self.consume_messages()
            except (socket.error, AMQPConnectionException):
                self.logger.error("AMQPListener: Connection to broker lost. "
                                + "Trying to re-establish connection...")

    def consume_messages(self):
        """Consume messages forever (or until an exception is raised)."""
        task_consumer = self.task_consumer

        self.logger.debug("AMQPListener: Starting message consumer...")
        it = task_consumer.iterconsume(limit=None)

        self.logger.debug("AMQPListener: Ready to accept tasks!")

        while True:
            self.task_consumer.qos(prefetch_count=int(self.prefetch_count))
            it.next()

    def stop(self):
        """Stop processing AMQP messages and close the connection
        to the broker."""
        self.close_connection()

    def receive_message(self, message_data, message):
        """The callback called when a new message is received.

        If the message has an ``eta`` we move it to the hold queue,
        otherwise we move it the bucket queue for immediate processing.

        """
        try:
            task = TaskWrapper.from_message(message, message_data,
                                            logger=self.logger)
        except NotRegistered, exc:
            self.logger.error("Unknown task ignored: %s" % (exc))
            return

        eta = message_data.get("eta")
        if eta:
            self.prefetch_count.increment()
            self.logger.info("Got task from broker: %s[%s] eta:[%s]" % (
                    task.task_name, task.task_id, eta))
            self.hold_queue.put((task, eta, self.prefetch_count.decrement))
        else:
            self.logger.info("Got task from broker: %s[%s]" % (
                    task.task_name, task.task_id))
            self.bucket_queue.put(task)

    def close_connection(self):
        """Close the AMQP connection."""
        if self.task_consumer:
            self.task_consumer.close()
            self.task_consumer = None
        if self.amqp_connection:
            self.logger.debug(
                    "AMQPListener: Closing connection to the broker...")
            self.amqp_connection.close()
            self.amqp_connection = None

    def reset_connection(self):
        """Reset the AMQP connection, and reinitialize the
        :class:`carrot.messaging.ConsumerSet` instance.

        Resets the task consumer in :attr:`task_consumer`.

        """
        self.logger.debug(
                "AMQPListener: Re-establishing connection to the broker...")
        self.close_connection()
        self.amqp_connection = self._open_connection()
        self.task_consumer = get_consumer_set(connection=self.amqp_connection)
        self.task_consumer.register_callback(self.receive_message)

    def _open_connection(self):
        """Retries connecting to the AMQP broker over time.

        See :func:`celery.utils.retry_over_time`.

        """

        def _connection_error_handler(exc, interval):
            """Callback handler for connection errors."""
            self.logger.error("AMQP Listener: Connection Error: %s. " % exc
                     + "Trying again in %d seconds..." % interval)

        def _establish_connection():
            """Establish a connection to the AMQP broker."""
            conn = DjangoBrokerConnection()
            connected = conn.connection # Connection is established lazily.
            return conn

        if not conf.AMQP_CONNECTION_RETRY:
            return _establish_connection()

        conn = retry_over_time(_establish_connection, socket.error,
                               errback=_connection_error_handler,
                               max_retries=conf.AMQP_CONNECTION_MAX_RETRIES)
        self.logger.debug("AMQPListener: Connection Established.")
        return conn


class PeriodicWorkController(FlowerComponent):
    """A thread that continuously checks if there are
    :class:`celery.task.PeriodicTask` tasks waiting for execution,
    and executes them. It also finds tasks in the hold queue that is
    ready for execution and moves them to the bucket queue.

    (Tasks in the hold queue are tasks waiting for retry, or with an
    ``eta``/``countdown``.)

    """

    def on_start(self):
        """Do backend-specific periodic task initialization."""
        default_periodic_status_backend.init_periodic_tasks()

    def on_iteration(self):
        logger = self.get_logger()
        logger.debug("PeriodicWorkController: Running periodic tasks...")
        try:
            self.run_periodic_tasks()
        except Exception, exc:
            logger.error(
                "PeriodicWorkController got exception: %s\n%s" % (
                    exc, traceback.format_exc()))
        logger.debug("PeriodicWorkController: Processing hold queue...")
        self.process_hold_queue()
        logger.debug("PeriodicWorkController: Going to sleep...")
        time.sleep(1)

    def run_periodic_tasks(self):
        logger = self.get_logger()
        applied = default_periodic_status_backend.run_periodic_tasks()
        for task, task_id in applied:
            logger.debug(
                "PeriodicWorkController: Periodic task %s applied (%s)" % (
                    task.name, task_id))

    def process_hold_queue(self):
        """Finds paused tasks that are ready for execution and move
        them to the :attr:`bucket_queue`."""
        logger = self.get_logger()
        try:
            logger.debug(
                "PeriodicWorkController: Getting next task from hold queue..")
            task, eta, on_accept = self.hold_queue.get_nowait()
        except QueueEmpty:
            logger.debug("PeriodicWorkController: Hold queue is empty")
            return

        if datetime.now() >= eta:
            logger.debug(
                "PeriodicWorkController: Time to run %s[%s] (%s)..." % (
                    task.task_name, task.task_id, eta))
            on_accept() # Run the accept task callback.
            self.bucket_queue.put(task)
        else:
            logger.debug(
                "PeriodicWorkController: ETA not ready for %s[%s] (%s)..." % (
                    task.task_name, task.task_id, eta))
            self.hold_queue.put((task, eta, on_accept))
