"""
Kafka message processor
"""
import atexit
import json
import logging
from collections import defaultdict
from typing import List

import msgpack
import pendulum

from db import DB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

db = DB()

translation_table = str.maketrans({"'": "", '"': ""})


class Stats:
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


class BaseMsgProcessor:
    def __init__(self, kafka_topics: List[str]):  # ToDo: BaseMsgProcessor should not have kafka_topics as it's param
        self.consumer = None
        self.kafka_topics = kafka_topics
        self.metrics = [
            {"granularity": "minute"},
            {"granularity": "hour"},
        ]
        self.number_of_received_messages = 0
        self.running_stats = defaultdict(Stats)

    async def connect(self):
        atexit.register(self.disconnect)
        await self.subscribe()
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

    async def subscribe(self):
        raise NotImplementedError()

    async def loop(self):
        raise NotImplementedError()

    def process_msg(self, msg_raw):
        """
        "Processing Message" - means passing message to all metric instances (processors)

        :param msg_raw:
        :return:
        """

        msg = msgpack.unpackb(msg_raw)
        self.number_of_received_messages += 1

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
