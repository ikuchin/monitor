import asyncio
from pprint import pprint
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import random
import pendulum
from aiohttp import ClientSession

from monitors.base_monitor import MonitorBase, BaseStats
from monitors.http_monitor import HttpMonitor


class FakeResponse(MagicMock):
    status = "status"

    async def __aenter__(self, *args, **kwargs):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        print(exc_type, exc_val, exc_tb)


class TestMonitor(TestCase):
    def test_base_monitor_creation(self):

        monitor = MonitorBase(job_id=1)

        self.assertEqual(1, monitor.job_id)
        self.assertEqual(1, monitor.job_name)

        monitor = MonitorBase(job_id=2, job_name="test_name")

        self.assertEqual(2, monitor.job_id)
        self.assertEqual("test_name", monitor.job_name)

        print(monitor.dict())

        self.assertDictEqual(
            {
                "job_id": 2,
                "job_name": "test_name",
                "check_period_seconds": 60,
                "stats": {
                    "counts_total": {},
                    "counts_by_hour": {},
                    "counts_by_minute": {},
                    "percent_total": {},
                },
                "number_of_send_messages": 0,
                "running": False,
            },
            monitor.dict(),
        )

    def test_base_monitor_stats(self):
        stats = BaseStats()

        stats.update_counter(pendulum.from_timestamp(0), 200)

        print(stats.dict())

        self.assertDictEqual(
            {
                "counts_total": {200: 1},
                "counts_by_hour": {"1970-01-01T00:00:00+00:00": {200: 1}},
                "counts_by_minute": {"1970-01-01T00:00:00+00:00": {200: 1}},
                "percent_total": {200: "100.00%"},
            },
            stats.dict(),
        )

    def test_http_monitor(self):
        monitor = HttpMonitor(job_id=1, uri="http://localhost", method="get")
        pendulum.set_test_now(pendulum.from_timestamp(0))
        with patch.object(ClientSession, "request", FakeResponse) as mock_method_1:
            with patch.object(MonitorBase, "send_message") as mock_method_2:
                # with patch('monitors.base_monitor.Producer') as mock_method_2:
                asyncio.run(monitor.check())
                self.assertDictContainsSubset(
                    {"job_id": 1, "status": "status", "ts": 0.0},
                    mock_method_2.call_args.args[0],
                )
                self.assertIn("response_time", mock_method_2.call_args.args[0])
