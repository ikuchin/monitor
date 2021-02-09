import asyncio
from time import perf_counter

import aiohttp
import pendulum

from monitors.base_monitor import BaseMonitor


class HttpMonitor(BaseMonitor):
    def __init__(self, job_id, uri, method, **kwargs):
        super().__init__(job_id, **kwargs)
        self.uti = uri
        self.method = method

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
        self.send_message(msg)


if __name__ == '__main__':
    asyncio.run(HttpMonitor(None, "http://127.0.0.1:8082", "get").check())

