import unittest
import time
import multiprocessing
from Queue import Queue, Empty
from datetime import datetime, timedelta

from flower.components import FlowerComponent
from flower.components import Mediator
from celery.worker.components import PeriodicWorkController


class MockTask(object):
    task_id = 1234
    task_name = "mocktask"

    def __init__(self, value, **kwargs):
        self.value = value


class MyFlowerComponent(FlowerComponent):

    def on_iteration(self):
        import time
        time.sleep(1)


class TestFlowerComponent(unittest.TestCase):

    def test_on_iteration(self):
        self.assertRaises(NotImplementedError,
                FlowerComponent(Queue(), Queue()).on_iteration)

    def test_run(self):
        t = MyFlowerComponent(Queue(), Queue())
        t._shutdown.set()
        t.run()
        self.assertTrue(t._stopped.isSet())

    def test_start_stop(self):
        t = MyFlowerComponent(Queue(), Queue())
        t.start()
        self.assertFalse(t._shutdown.isSet())
        self.assertFalse(t._stopped.isSet())
        t.stop()
        self.assertTrue(t._shutdown.isSet())
        self.assertTrue(t._stopped.isSet())


class TestMediator(unittest.TestCase):

    def test_mediator_start__stop(self):
        bucket_queue = Queue()
        m = Mediator(bucket_queue, lambda t: t)
        m.start()
        self.assertFalse(m._shutdown.isSet())
        self.assertFalse(m._stopped.isSet())
        m.stop()
        m.join()
        self.assertTrue(m._shutdown.isSet())
        self.assertTrue(m._stopped.isSet())

    def test_mediator_on_iteration(self):
        bucket_queue = Queue()
        got = {}

        def mycallback(value):
            got["value"] = value.value

        m = Mediator(bucket_queue, mycallback)
        bucket_queue.put(MockTask("George Constanza"))

        m.on_iteration()

        self.assertEquals(got["value"], "George Constanza")


class TestPeriodicWorkController(unittest.TestCase):

    def test_process_hold_queue(self):
        bucket_queue = Queue()
        hold_queue = Queue()
        m = PeriodicWorkController(bucket_queue, hold_queue, 1, None)
        m.process_hold_queue()

        scratchpad = {}

        def on_accept():
            scratchpad["accepted"] = True

        hold_queue.put((MockTask("task1"),
                        datetime.now() - timedelta(days=1),
                        on_accept))

        m.process_hold_queue()
        self.assertRaises(Empty, hold_queue.get_nowait)
        self.assertTrue(scratchpad.get("accepted"))
        self.assertEquals(bucket_queue.get_nowait().value, "task1")
        tomorrow = datetime.now() + timedelta(days=1)
        hold_queue.put((MockTask("task2"), tomorrow, on_accept))
        m.process_hold_queue()
        self.assertRaises(Empty, bucket_queue.get_nowait)
        value, eta, on_accept = hold_queue.get_nowait()
        self.assertEquals(value.value, "task2")
        self.assertEquals(eta, tomorrow)

    def test_run_periodic_tasks(self):
        bucket_queue = Queue()
        hold_queue = Queue()
        m = PeriodicWorkController(bucket_queue, hold_queue, 1, None)
        m.run_periodic_tasks()

    def test_on_iteration(self):
        bucket_queue = Queue()
        hold_queue = Queue()
        m = PeriodicWorkController(bucket_queue, hold_queue, 1, None)
        m.on_iteration()
