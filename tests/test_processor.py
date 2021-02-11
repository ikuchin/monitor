from pprint import pprint
from unittest import TestCase
import random
import msgpack
from pendulum import datetime

from processors.msg_processor_base import BaseMsgProcessor, Stats


class TestMsgProcessor(TestCase):
    def setUp(self) -> None:
        random.seed(0)
        self.processor = BaseMsgProcessor(kafka_topics=[])

    def test_process_msg_1(self):
        """
        Processing 1 message and checking 2 metrics
        """
        msg = msgpack.packb({"job_id": 1, "ts": 1234567890, "status": 200, "response_time": 1})

        self.processor.process_msg(msg)

        stats = Stats({200: 1}, 1, 1)

        self.assertEqual(stats, self.processor.running_stats[(1, datetime(2009, 2, 13, 23), "hour")])
        self.assertEqual(
            stats,
            self.processor.running_stats[(1, datetime(2009, 2, 13, 23, 31), "minute")],
        )

    def test_process_msg_1000(self):
        """
        Processing 1000 messages
        """
        for i in range(1000):
            msg = msgpack.packb(
                {
                    "job_id": 1,
                    "ts": 1234567890 + i,
                    "status": random.choice([200, 404, 500]),
                    "response_time": random.random(),
                }
            )

            self.processor.process_msg(msg)

        stats = Stats({404: 325, 500: 309, 200: 366}, 0.0010127930964535237, 0.9996851255769114)
        self.assertEqual(stats, self.processor.running_stats[(1, datetime(2009, 2, 13, 23), "hour")])

    def test_process_msg_non_standart_code(self):
        """
        Processing 1000 messages
        """
        msg = msgpack.packb(
            {
                "job_id": 1,
                "ts": 1234567890,
                "status": 'error "can\'t producer_connect"',
                "response_time": random.random(),
            }
        )

        self.processor.process_msg(msg)

        stats = Stats({"error cant producer_connect": 1}, 0.8444218515250481, 0.8444218515250481)
        self.assertEqual(stats, self.processor.running_stats[(1, datetime(2009, 2, 13, 23), "hour")])
