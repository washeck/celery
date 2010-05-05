"""

The Multiprocessing Worker Server

"""
import os
import socket
import logging
import traceback
from Queue import Queue
from multiprocessing.util import Finalize

from celery import conf
from celery import registry
from celery import platform
from celery import signals
from celery.log import setup_logger, _hijack_multiprocessing_logger
from celery.beat import EmbeddedClockService
from celery.utils import noop, instantiate

from celery.worker.buckets import TaskBucket
from celery.worker.scheduler import Scheduler


def process_initializer():
    # There seems to a bug in multiprocessing (backport?)
    # when detached, where the worker gets EOFErrors from time to time
    # and the logger is left from the parent process causing a crash.
    _hijack_multiprocessing_logger()

    platform.reset_signal("SIGTERM")
    platform.set_mp_process_title("celeryd")

    # On Windows we need to run a dummy command 'celeryinit'
    # for django to fully initialize after fork()
    if not callable(getattr(os, "fork", None)):
        from django.core.management import execute_manager
        settings_mod = os.environ.get("DJANGO_SETTINGS_MODULE", "settings")
        project_settings = __import__(settings_mod, {}, {}, [''])
        execute_manager(project_settings, argv=['manage.py', 'celeryinit'])

    # This is for windows and other platforms not supporting
    # fork(). Note that init_worker makes sure it's only
    # run once per process.
    from celery.loaders import current_loader
    current_loader().init_worker()


class WorkController(object):
    """Executes tasks waiting in the task queue.

    :param concurrency: see :attr:`concurrency`.
    :param logfile: see :attr:`logfile`.
    :param loglevel: see :attr:`loglevel`.
    :param embed_clockservice: see :attr:`run_clockservice`.
    :param send_events: see :attr:`send_events`.


    .. attribute:: concurrency

        The number of simultaneous processes doing work (default:
        :const:`celery.conf.CELERYD_CONCURRENCY`)

    .. attribute:: loglevel

        The loglevel used (default: :const:`logging.INFO`)

    .. attribute:: logfile

        The logfile used, if no logfile is specified it uses ``stderr``
        (default: :const:`celery.conf.CELERYD_LOG_FILE`).

    .. attribute:: embed_clockservice

        If ``True``, celerybeat is embedded, running in the main worker
        process as a thread.

    .. attribute:: send_events

        Enable the sending of monitoring events, these events can be captured
        by monitors (celerymon).

    .. attribute:: logger

        The :class:`logging.Logger` instance used for logging.

    .. attribute:: pool

        The :class:`multiprocessing.Pool` instance used.

    .. attribute:: ready_queue

        The :class:`Queue.Queue` that holds tasks ready for immediate
        processing.

    .. attribute:: hold_queue

        The :class:`Queue.Queue` that holds paused tasks. Reasons for holding
        back the task include waiting for ``eta`` to pass or the task is being
        retried.

    .. attribute:: schedule_controller

        Instance of :class:`celery.worker.controllers.ScheduleController`.

    .. attribute:: mediator

        Instance of :class:`celery.worker.controllers.Mediator`.

    .. attribute:: listener

        Instance of :class:`CarrotListener`.

    """
    loglevel = logging.ERROR
    concurrency = conf.CELERYD_CONCURRENCY
    logfile = conf.CELERYD_LOG_FILE
    _state = None

    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            send_events=conf.SEND_EVENTS, hostname=None,
            ready_callback=noop, embed_clockservice=False,
            pool_cls=conf.CELERYD_POOL, listener_cls=conf.CELERYD_LISTENER,
            mediator_cls=conf.CELERYD_MEDIATOR,
            eta_scheduler_cls=conf.CELERYD_ETA_SCHEDULER,
            schedule_filename=conf.CELERYBEAT_SCHEDULE_FILENAME):

        # Options
        self.loglevel = loglevel or self.loglevel
        self.concurrency = concurrency or self.concurrency
        self.logfile = logfile or self.logfile
        self.logger = setup_logger(loglevel, logfile)
        self.hostname = hostname or socket.gethostname()
        self.embed_clockservice = embed_clockservice
        self.ready_callback = ready_callback
        self.send_events = send_events
        self._finalize = Finalize(self, self.stop, exitpriority=20)

        # Queues
        if conf.DISABLE_RATE_LIMITS:
            self.ready_queue = Queue()
        else:
            self.ready_queue = TaskBucket(task_registry=registry.tasks)
        self.eta_schedule = Scheduler(self.ready_queue, logger=self.logger)

        self.logger.debug("Instantiating thread components...")

        # Threads + Pool + Consumer
        self.pool = instantiate(pool_cls, self.concurrency,
                                logger=self.logger,
                                initializer=process_initializer)
        self.mediator = instantiate(mediator_cls, self.ready_queue,
                                    callback=self.process_task,
                                    logger=self.logger)
        self.scheduler = instantiate(eta_scheduler_cls,
                                     self.eta_schedule, logger=self.logger)

        self.clockservice = None
        if self.embed_clockservice:
            self.clockservice = EmbeddedClockService(logger=self.logger,
                                    schedule_filename=schedule_filename)

        prefetch_count = self.concurrency * conf.CELERYD_PREFETCH_MULTIPLIER
        self.listener = instantiate(listener_cls,
                                    self.ready_queue,
                                    self.eta_schedule,
                                    logger=self.logger,
                                    hostname=self.hostname,
                                    send_events=self.send_events,
                                    init_callback=self.ready_callback,
                                    initial_prefetch_count=prefetch_count)

        # The order is important here;
        #   the first in the list is the first to start,
        # and they must be stopped in reverse order.
        self.components = filter(None, (self.pool,
                                        self.mediator,
                                        self.scheduler,
                                        self.clockservice,
                                        self.listener))

    def start(self):
        """Starts the workers main loop."""
        self._state = "RUN"

        try:
            for component in self.components:
                self.logger.debug("Starting thread %s..." % \
                        component.__class__.__name__)
                component.start()
        finally:
            self.stop()

    def process_task(self, wrapper):
        """Process task by sending it to the pool of workers."""
        try:
            try:
                wrapper.task.execute(wrapper, self.pool,
                                     self.loglevel, self.logfile)
            except Exception, exc:
                self.logger.critical("Internal error %s: %s\n%s" % (
                                exc.__class__, exc, traceback.format_exc()))
        except (SystemExit, KeyboardInterrupt):
            self.stop()

    def stop(self):
        """Gracefully shutdown the worker server."""
        if self._state != "RUN":
            return

        signals.worker_shutdown.send(sender=self)
        for component in reversed(self.components):
            self.logger.debug("Stopping thread %s..." % (
                              component.__class__.__name__))
            component.stop()

        self.listener.close_connection()
        self._state = "STOP"

    def terminate(self):
        """Not so gracefully shutdown the worker server."""
        if self._state != "RUN":
            return

        signals.worker_shutdown.send(sender=self)
        for component in reversed(self.components):
            self.logger.debug("Terminating thread %s..." % (
                              component.__class__.__name__))
            terminate = getattr(component, "terminate", component.stop)
            terminate()

        self.listener.close_connection()
        self._state = "STOP"
