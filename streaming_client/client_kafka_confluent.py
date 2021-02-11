import atexit

from confluent_kafka import Producer

from settings import (kafka_bootstrap_servers, kafka_ssl_ca_location,
                      kafka_ssl_certificate_location, kafka_ssl_key_location)
from streaming_client.client_base import ClientBase


class ClientKafkaConfluent(ClientBase):
    def __init__(self):
        self.producer = None
        self.producer_connected = False

    def connect(self):
        self.producer = Producer({
            "bootstrap.servers": kafka_bootstrap_servers,
            "security.protocol": "SSL",
            "ssl.ca.location": kafka_ssl_ca_location,
            "ssl.certificate.location": kafka_ssl_certificate_location,
            "ssl.key.location": kafka_ssl_key_location
        })
        self.producer_connected = True
        atexit.register(self.disconnect)

    def disconnect(self):
        self.producer.flush()
        self.producer_connected = False

    def push(self, topic, value):
        if not self.producer_connected:
            self.connect()
        self.producer.produce(topic=topic, value=value)
