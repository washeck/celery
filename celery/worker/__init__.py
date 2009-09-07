"""

The Multiprocessing Worker Server

Documentation for this module is in ``docs/reference/celery.worker.rst``.

"""
from flower.worker import WorkController as FlowerWorkController
from celery.log import setup_logger
from celery import conf

DEFAULT_COMPONENTS = ["flower.components.Mediator",
        #"celery.worker.components.PeriodicWorkController",
                      "celery.worker.components.AMQPListener"]



class WorkController(FlowerWorkController):
    """Celery Work Controller"""
    components = DEFAULT_COMPONENTS
    concurrency = conf.DAEMON_CONCURRENCY
    logfile = conf.DAEMON_LOG_FILE

    def __init__(self, *args, **kwargs):
        self.logfile = kwargs.get("logfile", self.logfile)
        self.loglevel = kwargs.get("loglevel", self.loglevel)
        self.logger = setup_logger(self.loglevel, self.logfile)
        super(WorkController, self).__init__(*args, **kwargs)

    def process_task(self, task):
        """Process task by sending it to the pool of workers."""
        task.execute_using_pool(self.pool, self.loglevel, self.logfile)
