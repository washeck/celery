from celery.backends import default_periodic_status_backend
from Queue import Empty as QueueEmpty
from datetime import datetime
import threading
import time


class Mediator(threading.Thread):
    """Thread continuously passing tasks in the queue
    to the pool."""

    def __init__(self, bucket_queues, callback):
        super(Mediator, self).__init__()
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.bucket_queue = bucket_queues
        self.callback = callback

    def run(self):
        while True:
            if self._shutdown.isSet():
                break
            try:
                for bucket_queue in self.bucket_queues.values():
                    task = bucket_queue.get_nowait()
                    except QueueEmpty:
                        pass
                    else:
                        self.callback(task)
        self._stopped.set() # indicate that we are stopped

    def stop(self):
        """Shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done


class PeriodicWorkController(threading.Thread):
    """A thread that continuously checks if there are
    :class:`celery.task.PeriodicTask` tasks waiting for execution,
    and executes them."""

    def __init__(self, bucket_queues, hold_queue):
        super(PeriodicWorkController, self).__init__()
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.hold_queue = hold_queue
        self.bucket_queues = bucket_queue

    def run(self):
        """Run when you use :meth:`Thread.start`"""
        while True:
            if self._shutdown.isSet():
                break
            default_periodic_status_backend.run_periodic_tasks()
            self.process_hold_queue()
            time.sleep(1)
        self._stopped.set() # indicate that we are stopped

    def process_hold_queue(self):
        try:
            task, eta = self.hold_queue.get_nowait()
        except QueueEmpty:
            return
        if datetime.now() >= eta:
            self.bucket_queue[task.task_name].put(task)
        else:
            self.hold_queue.put((task, eta))

    def stop(self):
        """Shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done
