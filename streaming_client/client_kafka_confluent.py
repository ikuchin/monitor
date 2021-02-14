import asyncio
import atexit
from typing import Optional, List

from confluent_kafka import Producer, Consumer

from settings import (
    kafka_bootstrap_servers,
    kafka_ssl_ca_location,
    kafka_ssl_certificate_location,
    kafka_ssl_key_location,
)
from streaming_client.client_base import ClientBase


connection_params = {
    "bootstrap.servers": kafka_bootstrap_servers,
    "security.protocol": "SSL",
    "ssl.ca.location": kafka_ssl_ca_location,
    "ssl.certificate.location": kafka_ssl_certificate_location,
    "ssl.key.location": kafka_ssl_key_location,
}


class ClientKafkaConfluent(ClientBase):
    def __init__(self, kafka_topics=None, consumer_group_id=None, consumer_offset=None):
        super().__init__()
        self.default_kafka_topics = kafka_topics or ["test"]
        self.kafka_producer: Optional[Producer] = None
        self.kafka_consumer: Optional[Consumer] = None
        self.kafka_consumer_group_id = consumer_group_id or "test-group-1"
        self.kafka_consumer_offset = consumer_offset or "latest"

        # atexit.register(self.disconnect)  # ToDo: check if connections were properly closed

    def producer(self) -> Producer:
        if self.kafka_producer is None:
            self.kafka_producer = Producer(connection_params)
        return self.kafka_producer

    def consumer(self) -> Consumer:
        if self.kafka_consumer is None:
            self.kafka_consumer = Consumer(
                dict(connection_params, **{
                    # "enable.auto.commit": True,  # this is default
                    "auto.offset.reset": self.kafka_consumer_offset,
                    # earliest - for oldest not committed message, latest - for new messages
                    "client.id": "test-client-1",
                    "group.id": self.kafka_consumer_group_id,
                })
            )
        return self.kafka_consumer

    async def subscribe(self, kafka_topics: Optional[List[str]] = None):
        self.consumer().subscribe(kafka_topics or self.default_kafka_topics)

    async def disconnect(self):
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer = None
        if self.kafka_consumer:
            self.kafka_consumer.close()
            self.kafka_consumer = None

    async def push(self, topic, value):
        self.producer().produce(topic=topic, value=value)

    async def pull(self):
        # ToDo: This is a blocking call. Not good for asyncio. Should run it in ThreadPoolExecutor
        msg = self.consumer().poll()

        return msg.value()

    async def message_iterator(self):
        consumer = self.consumer()

        while True:
            await asyncio.sleep(0)
            # ToDo: This is a blocking call. Not good for asyncio. Should run it in ThreadPoolExecutor
            if self.kafka_consumer:
                msg = consumer.poll(0.1)
            else:
                break

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            yield msg.value()
