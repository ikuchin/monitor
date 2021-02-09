"""
Kafka message processor

"""
import atexit
import json
import logging
from collections import defaultdict
from typing import List

from db import DB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

logger.info('Connection')
db = DB()

translation_table = str.maketrans({"'": '', '"': ''})


class Stats:
    def __init__(self):
        self.counter = defaultdict(int)
        self.response_time_min = float('inf')
        self.response_time_max = 0

    def update(self, status, response_time: float):
        # If "status" is an error message, it may contain characters that will prevent correct DB update
        if isinstance(status, str):
            status = status.translate(translation_table)

        self.counter[status] += 1

        self.response_time_min = min(self.response_time_min, response_time)
        self.response_time_max = max(self.response_time_max, response_time)

    def __repr__(self):
        return f"{dict(self.counter)} {self.response_time_min} {self.response_time_max}"


class BaseMsgProcessor:
    def __init__(self, kafka_topics: List[str]):
        self.consumer = None
        self.kafka_topics = kafka_topics
        self.metrics = [
            {"granularity": "minute"},
            {"granularity": "hour"},
        ]
        self.number_of_received_messages = 0
        self.running_stats = defaultdict(Stats)

        atexit.register(self.disconnect)

    async def connect(self):
        await self.subscribe()
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

    async def subscribe(self):
        raise NotImplementedError()

    async def loop(self):
        raise NotImplementedError()

    async def upload_stats(self):
        for k, v in self.running_stats.items():
            (job_id, ts, granularity) = k
            print("Upserting record", job_id, ts, granularity, v)
            data_update_statement = ", ', ',".join([
                f''' '"{k}":', COALESCE(stats.data->>'{k}','0')::int + {v}'''
                for k, v in v.counter.items()
            ])

            status, records = db.execute_query(
                f'''
                INSERT INTO stats (job_id, ts, granularity, response_time_min, response_time_max, data)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT on constraint stats_job_id_ts_granularity_key
                DO UPDATE SET data = stats.data || CONCAT('{{', {data_update_statement}, '}}')::jsonb;
                ''',
                [job_id, ts, granularity, v.response_time_min, v.response_time_max, json.dumps(v.counter)],
                auto_commit=True
            )
        self.running_stats.clear()
