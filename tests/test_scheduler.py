from pprint import pprint
from unittest import TestCase
import random
import msgpack
import pendulum
from scheduler import Scheduler

from processors.msg_processor import MsgProcessor, Stats


class TestMsgProcessor(TestCase):
    def setUp(self) -> None:
        random.seed(0)
        pendulum.set_test_now(pendulum.from_timestamp(0))
        self.scheduler = Scheduler()

    def test_up_time(self):
        pendulum.set_test_now(pendulum.from_timestamp(100_000))
        self.assertEqual("1 day 3 hours 46 minutes 40 seconds", self.scheduler.up_time())

    def test_start_stop_job(self):
        self.scheduler.job_start(1)
        self.assertEqual(True, self.scheduler.jobs[1].running)
        self.scheduler.job_stop(1)
        self.assertEqual(False, self.scheduler.jobs[1].running)
