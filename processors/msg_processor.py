"""
Kafka message processor
"""
import asyncio
import atexit
import json
import logging
from collections import defaultdict
from typing import List

import msgpack
import aioschedule as schedule
import pendulum

from db import DB
from settings.defaults import default_streaming_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

db = DB()

# This table is used to fix values that we can't currently correctly upsert to DB
translation_table = str.maketrans({"'": "", '"': ""})


class Stats:  # ToDo: should be replaced by MetricTemplate/MetricInstance
    def __init__(self, counter: dict = None, response_time_min=None, response_time_max=None):
        self.counter = defaultdict(int)
        self.response_time_min = response_time_min or float("inf")
        self.response_time_max = response_time_max or 0

        if counter:
            self.counter.update(counter)

    def update(self, status, response_time: float):
        # If "status" is an error message, it may contain characters that will prevent correct DB update
        if isinstance(status, str):
            status = status.translate(translation_table)

        self.counter[status] += 1

        self.response_time_min = min(self.response_time_min, response_time)
        self.response_time_max = max(self.response_time_max, response_time)

    def __repr__(self):
        return f"{dict(self.counter)}, {self.response_time_min}, {self.response_time_max}"

    def __eq__(self, other):
        if isinstance(other, Stats):
            return all(
                [
                    self.counter == other.counter,
                    self.response_time_max == other.response_time_max,
                    self.response_time_min == other.response_time_min,
                ]
            )
        else:
            return False


class MsgProcessor:
    """
    Processor should know about Jobs it's working with. It probably load jobs and the start...
    """

    def __init__(self, topics: List[str], **kwargs):  # ToDo: MsgProcessor should not have kafka_topics as it's param
        self.consumer = None
        self.topics = topics
        self.metrics = [
            {"granularity": "minute"},
            {"granularity": "hour"},
        ]
        self.streaming_client = kwargs.get('streaming_client') or default_streaming_client
        self.number_of_received_messages = 0
        self.running_stats = defaultdict(Stats)
        self.keep_running = True

        self.upload_metric_stats_period = 10

    async def loop(self):
        await self.streaming_client.connect()
        await self.streaming_client.subscribe(self.topics)

        if self.upload_metric_stats_period > 0:
            schedule.every(self.upload_metric_stats_period).seconds.do(self.upload_stats)

        messages = self.streaming_client.message_iterator()
        while self.keep_running:
            try:
                msg = await messages.__anext__()
            except StopAsyncIteration:
                return

        # async for msg in self.streaming_client:
            self.process_msg(msg)

            await schedule.run_pending()
            await asyncio.sleep(0)  # switch to next coroutine

    def process_msg(self, msg_raw):
        """
        "Processing Message" - means passing message to all metric instances (processors)

        :param msg_raw:
        :return:
        """

        msg = msgpack.unpackb(msg_raw)
        self.number_of_received_messages += 1
        job_id = msg['job_id']

        # ToDo: metrics should be specified for job

        for metric in self.metrics:
            ts = pendulum.from_timestamp(msg["ts"]).replace(microsecond=0)

            if metric["granularity"] == "minute":
                ts = ts.replace(second=0)
            elif metric["granularity"] == "hour":
                ts = ts.replace(minute=0, second=0)

            self.running_stats[(msg["job_id"], ts, metric["granularity"])].update(
                status=msg["status"], response_time=msg["response_time"]
            )

    async def upload_stats(self):
        """
        ToDo: this function is total garbage, find a way to implement it better without the use of ORM :(

        :return:
        """
        for k, v in self.running_stats.items():
            (job_id, ts, granularity) = k
            print("Upserting record", job_id, ts, granularity, v)
            data_update_statement = ", ', ',".join(
                [f""" '"{k}":', COALESCE(stats.data->>'{k}','0')::int + {v}""" for k, v in v.counter.items()]
            )

            status, records = db.execute_query(
                f"""
                INSERT INTO stats (job_id, ts, granularity, response_time_min, response_time_max, data)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT on constraint stats_job_id_ts_granularity_key
                DO UPDATE SET data = stats.data || CONCAT('{{', {data_update_statement}, '}}')::jsonb;
                """,
                [
                    job_id,
                    ts,
                    granularity,
                    v.response_time_min,
                    v.response_time_max,
                    json.dumps(v.counter),
                ],
                auto_commit=True,
            )
        self.running_stats.clear()
