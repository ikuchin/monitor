import atexit
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from streaming_client.client_base import ClientBase

from settings import (
    kafka_bootstrap_servers,
    kafka_ssl_ca_location,
    kafka_ssl_certificate_location,
    kafka_ssl_key_location,
)


class ClientKafkaAio(ClientBase):
    def __init__(self):
        self.kafka_topics = ["test"]
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.consumer_connected = False
        self.producer: Optional[AIOKafkaProducer] = None
        self.producer_connected = False

        atexit.register(self.disconnect)

    def ssl_context(self):
        return create_ssl_context(
            cafile=kafka_ssl_ca_location,
            certfile=kafka_ssl_certificate_location,
            keyfile=kafka_ssl_key_location,
        )

    async def producer_connect(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            security_protocol="SSL" if self.ssl_context() else None,
            ssl_context=self.ssl_context(),
        )
        await self.producer.start()
        self.producer_connected = True

    async def consumer_connect(self):
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=kafka_bootstrap_servers,
            security_protocol="SSL" if self.ssl_context() else None,
            ssl_context=self.ssl_context(),
            auto_offset_reset="latest",
            client_id="test-client-1",
            group_id="test-group-1",
        )
        await self.consumer.start()
        self.consumer_connected = True
        await self.subscribe()

    async def subscribe(self):
        self.consumer.subscribe(self.kafka_topics)

    async def disconnect(self):
        """
        Flush any pending messages and close open Consumer/Producer
        :return:
        """
        if self.consumer_connected:
            await self.consumer.stop()
            self.consumer_connected = False
        if self.producer_connected:
            await self.producer.stop()
            self.producer_connected = False

    async def pull(self):
        """
        Get one message from the stream
        """
        if not self.consumer_connected:
            await self.consumer_connect()
        return await self.consumer.getone()

    async def push(self, topic, value):
        """
        Pushing (producing) message to specified topic
        """
        if not self.producer_connected:
            await self.producer_connect()
        await self.producer.send_and_wait(topic=topic, value=value)
