from time import perf_counter

import aiohttp
import pendulum

from monitors.base_monitor import MonitorBase
# from streaming_client.client_kafka_confluent import ClientKafkaConfluent as ClientKafka
from streaming_client.client_kafka_aio import ClientKafkaAio as ClientKafka


class HttpMonitor(MonitorBase):
    def __init__(self, job_id, uri, method, **kwargs):
        super().__init__(job_id, **kwargs)
        self.uti = uri
        self.method = method
        self.streaming_client = ClientKafka()

    async def check(self):
        td = pendulum.now('UTC')
        msg = {
            "job_id": self.job_id,
            "response_time": 0,
            "status": None,
            "ts": td.timestamp()
        }

        try:
            async with aiohttp.ClientSession() as session:
                tic = perf_counter()
                async with session.request(self.method, self.uti) as response:
                    response_time = perf_counter() - tic
                    msg["response_time"] = response_time
                    msg["status"] = response.status
        except BaseException as e:
            msg["status"] = str(e)

        self.stats.update_counter(td, msg["status"])
        await self.send_message(msg)

