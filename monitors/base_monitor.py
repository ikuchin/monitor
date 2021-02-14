import msgpack

from monitors.base_stats import BaseStats
from settings.defaults import default_streaming_client
# from streaming_client.client_base import ClientBase


class MonitorBase:
    def __init__(self, job_id, job_name=None, check_period_seconds=None, **kwargs):
        self.job_id = job_id
        self.job_name = job_name or job_id
        self.check_period_seconds = check_period_seconds or 60
        self.stats = BaseStats()
        self.number_of_send_messages = 0
        self.streaming_client = kwargs.get('streaming_client') or default_streaming_client
        self.running = False

    async def check(self):
        raise NotImplementedError("Override this method in Monitor implementation")

    async def send_message(self, msg):
        await self.streaming_client.push(
            topic="test",
            value=msgpack.packb(msg),
        )
        self.number_of_send_messages += 1

    def dict(self):
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "check_period_seconds": self.check_period_seconds,
            "stats": self.stats.dict(),
            "number_of_send_messages": self.number_of_send_messages,
            "running": self.running,
        }
