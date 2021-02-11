"""
Kafka message processor

"""
import asyncio
import logging
from abc import ABC

import aioschedule as schedule
import msgpack
import pendulum
from confluent_kafka import Consumer

from processors.msg_processor_base import BaseMsgProcessor
from settings import (
    kafka_bootstrap_servers,
    kafka_ssl_ca_location,
    kafka_ssl_certificate_location,
    kafka_ssl_key_location,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class ConfluentKafkaMsgProcessor(BaseMsgProcessor, ABC):
    """
    Confluent Kafka library can show slightly better performance than AIOKafka on high loads.
    But because of it's blocking nature it doesn't work well when consumer and producer working in the same thread.
    """

    async def connect(self):
        self.consumer = Consumer(
            {
                "bootstrap.servers": kafka_bootstrap_servers,
                "security.protocol": "SSL",
                "ssl.ca.location": kafka_ssl_ca_location,
                "ssl.certificate.location": kafka_ssl_certificate_location,
                "ssl.key.location": kafka_ssl_key_location,
                # "enable.auto.commit": True,  # this is default
                "auto.offset.reset": "latest",  # earliest - for oldest not committed message, latest - for new messages
                "client.id": "test-client-1",
                "group.id": "test-group-1",
            }
        )
        await self.subscribe()

    async def subscribe(self):
        self.consumer.subscribe(self.kafka_topics)

    def disconnect(self):
        if self.consumer:
            self.consumer.close()

    async def loop(self):
        await self.connect()
        await self.subscribe()

        schedule.every(10).seconds.do(self.upload_stats)
        while True:
            msg = self.consumer.poll(0.1)  # This is a blocking call

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            self.process_msg(msg.value())

            await schedule.run_pending()
            await asyncio.sleep(0)  # this is the way to switch to next coroutine
