"""
Kafka message processor

"""
import logging
from abc import ABC

import aioschedule as schedule
import msgpack
import pendulum
from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context

from processors.msg_processor_base import BaseMsgProcessor
from settings import (kafka_bootstrap_servers, kafka_ssl_ca_location,
                      kafka_ssl_certificate_location, kafka_ssl_key_location)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

logger.info('Connection')

context = create_ssl_context(
    cafile=kafka_ssl_ca_location,
    certfile=kafka_ssl_certificate_location,
    keyfile=kafka_ssl_key_location
)


class AioKafkaMsgProcessor(BaseMsgProcessor, ABC):
    """
    AioKafla library can be slightly slower than Confluent Kafka.
    But because it's asynchronous its works very well with current implementation.
    """
    async def connect(self):
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=kafka_bootstrap_servers,
            security_protocol="SSL",
            ssl_context=context,

            auto_offset_reset="latest",  # earliest - for oldest not committed message, latest - for new messages
            client_id="test-client-1",
            group_id="test-group-1",
        )
        await self.subscribe()

    async def subscribe(self):
        self.consumer.subscribe(self.kafka_topics)

    def disconnect(self):
        if self.consumer:
            self.consumer.stop()

    async def loop(self):
        await self.connect()
        await self.consumer.start()

        schedule.every(10).seconds.do(self.upload_stats)
        async for msg in self.consumer:
            msg = msgpack.unpackb(msg.value)
            print(f'Received message: {msg}')
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

            await schedule.run_pending()
