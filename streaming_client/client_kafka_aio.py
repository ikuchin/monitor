import atexit
from typing import Optional, List

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
    def __init__(self, kafka_topics=None, consumer_group_id=None, consumer_offset=None):
        super().__init__()
        self.default_kafka_topics = kafka_topics or ["test"]
        self.kafka_consumer: Optional[AIOKafkaConsumer] = None
        self.kafka_producer: Optional[AIOKafkaProducer] = None
        self.kafka_consumer_group_id = consumer_group_id or "test-group-1"
        self.kafka_consumer_offset = consumer_offset or "latest"

        # atexit.register(self.disconnect)

    def ssl_context(self):
        return create_ssl_context(
            cafile=kafka_ssl_ca_location,
            certfile=kafka_ssl_certificate_location,
            keyfile=kafka_ssl_key_location,
        )

    async def producer(self):
        """
        Performing lazy connection, establishing connection only if needed
        :return:
        """
        if self.kafka_producer is None:
            self.kafka_producer = AIOKafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                security_protocol="SSL" if self.ssl_context() else None,
                ssl_context=self.ssl_context(),
            )
            await self.kafka_producer.start()
        return self.kafka_producer

    async def consumer(self):
        """
        Performing lazy connection, establishing connection only if needed
        :return:
        """
        if self.kafka_consumer is None:
            self.kafka_consumer = AIOKafkaConsumer(
                bootstrap_servers=kafka_bootstrap_servers,
                security_protocol="SSL" if self.ssl_context() else None,
                ssl_context=self.ssl_context(),
                auto_offset_reset=self.kafka_consumer_offset,
                client_id="test-client-1",
                group_id=self.kafka_consumer_group_id,
            )
            await self.kafka_consumer.start()
        return self.kafka_consumer

    async def subscribe(self, kafka_topics: Optional[List[str]] = None):
        consumer = await self.consumer()
        consumer.subscribe(kafka_topics or self.default_kafka_topics)

    async def disconnect(self):
        """
        Flush any pending messages and close open Consumer/Producer
        :return:
        """
        if self.kafka_consumer and not self.kafka_consumer._closed:
            await self.kafka_consumer.stop()
        if self.kafka_producer and not self.kafka_producer._closed:
            await self.kafka_producer.stop()

    async def pull(self, topic=None):
        """
        Get one message from the stream
        """
        consumer = await self.consumer()
        msg = await consumer.getone()
        return msg.value

    async def push(self, topic, value):
        """
        Pushing (producing) message to specified topic
        """
        producer = await self.producer()
        await producer.send_and_wait(topic=topic, value=value)

    # def __iter__(self):
    #     raise NotImplementedError()
    #
    # async def __aiter__(self):
    #     consumer = await self.consumer()
    #     async for msg in consumer:
    #         yield msg.value

    async def message_iterator(self):
        consumer = await self.consumer()
        async for msg in consumer:
            yield msg.value
