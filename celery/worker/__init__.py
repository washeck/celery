"""

The Multiprocessing Worker Server

Documentation for this module is in ``docs/reference/celery.worker.rst``.

"""
from celery.pool import TaskPool
from celery.worker.controllers import Mediator
from celery.worker.controllers import PeriodicWorkController
from celery.worker.amqp import AMQPListener
from celery.log import setup_logger
from celery import conf
from Queue import Queue

import traceback
import logging

DEFAULT_COMPONENTS = [Mediator, PeriodicWorkController, AMQPListener]


class WorkController(object):
    """Executes tasks waiting in the task queue.

    :param concurrency: see :attr:`concurrency`.
    :param logfile: see :attr:`logfile`.
    :param loglevel: see :attr:`loglevel`.


    .. attribute:: concurrency

        The number of simultaneous processes doing work (default:
        :const:`celery.conf.DAEMON_CONCURRENCY`)

    .. attribute:: loglevel

        The loglevel used (default: :const:`logging.INFO`)

    .. attribute:: logfile

        The logfile used, if no logfile is specified it uses ``stderr``
        (default: :const:`celery.conf.DAEMON_LOG_FILE`).

    .. attribute:: logger

        The :class:`logging.Logger` instance used for logging.

    .. attribute:: is_detached

        Flag describing if the worker is running as a daemon or not.

    .. attribute:: pool

        The :class:`multiprocessing.Pool` instance used.

    .. attribute:: bucket_queue

        The :class:`Queue.Queue` that holds tasks ready for immediate
        processing.

    .. attribute:: hold_queue

        The :class:`Queue.Queue` that holds paused tasks. Reasons for holding
        back the task include waiting for ``eta`` to pass or the task is being
        retried.

    .. attribute:: periodic_work_controller

        Instance of :class:`celery.worker.controllers.PeriodicWorkController`.

    .. attribute:: mediator

        Instance of :class:`celery.worker.controllers.Mediator`.

    .. attribute:: amqp_listener

        Instance of :class:`AMQPListener`.

    """
    loglevel = logging.ERROR
    concurrency = conf.DAEMON_CONCURRENCY
    logfile = conf.DAEMON_LOG_FILE
    components = DEFAULT_COMPONENTS
    _state = None

    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            is_detached=False):

        # Options
        self.loglevel = loglevel or self.loglevel
        self.concurrency = concurrency or self.concurrency
        self.logfile = logfile or self.logfile
        self.is_detached = is_detached
        self.logger = setup_logger(loglevel, logfile)

        # Queues
        self.bucket_queue = Queue()
        self.hold_queue = Queue()

        self.logger.debug("Instantiating thread components...")
        

        # The order is important here;
        #   the first in the list is the first to start,
        # and they must be stopped in reverse order.
        self.pool = TaskPool(self.concurrency, logger=self.logger)
        subcomponents = map(self.load_component, self.components)

        self._components = [self.pool] + subcomponents
    
    def load_component(self, fqn):
        return self.import_component(fqn)(
                        bucket_queue=self.bucket_queue,
                        hold_queue=self.hold_queue,
                        concurrency=self.concurrency,
                        task_callback=self.safe_process_task,
                        logger=self.logger)


    def import_component(self, fqn):
        if isinstance(fqn, basestring):
            mod_name, comp_name = fqn.rsplit(".", 1)
            mod = __import__(mod_name, {}, {}, [comp_name])
            return getattr(mod, comp_name)
        return fqn

    def start(self):
        """Starts the workers main loop."""
        self._state = "RUN"

        try:
            for component in self._components:
                self.logger.debug("Starting thread %s..." % \
                        component.__class__.__name__)
                component.start()
        finally:
            self.stop()

    def safe_process_task(self, task):
        """Same as :meth:`process_task`, but catches all exceptions
        the task raises and log them as errors, to make sure the
        worker doesn't die."""
        try:
            try:
                self.process_task(task)
            except Exception, exc:
                self.logger.critical("Internal error %s: %s\n%s" % (
                                exc.__class__, exc, traceback.format_exc()))
        except (SystemExit, KeyboardInterrupt):
            self.stop()

    def process_task(self, task):
        """Process task by sending it to the pool of workers."""
        task.execute_using_pool(self.pool, self.loglevel, self.logfile)

    def stop(self):
        """Gracefully shutdown the worker server."""
        # shut down the periodic work controller thread
        if self._state != "RUN":
            return

        [component.stop() for component in reversed(self._components)]

        self._state = "STOP"
