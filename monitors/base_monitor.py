import atexit

import msgpack
from confluent_kafka import Producer

from monitors.base_stats import BaseStats
from settings import (kafka_bootstrap_servers, kafka_ssl_ca_location,
                      kafka_ssl_certificate_location, kafka_ssl_key_location)

producer = Producer({
    "bootstrap.servers": kafka_bootstrap_servers,
    "security.protocol": "SSL",
    "ssl.ca.location": kafka_ssl_ca_location,
    "ssl.certificate.location": kafka_ssl_certificate_location,
    "ssl.key.location": kafka_ssl_key_location
})
atexit.register(producer.flush)


class BaseMonitor:
    def __init__(self, job_id, job_name=None, check_period_seconds=None, **kwargs):
        self.job_id = job_id
        self.job_name = job_name or job_id
        self.check_period_seconds = check_period_seconds or 60
        self.stats = BaseStats()
        self.number_of_send_messages = 0
        self.kafka_producer = producer
        self.running = False

    async def check(self):
        raise NotImplementedError("Override this method in Monitor implementation")

    def send_message(self, msg):
        self.kafka_producer.produce(
            topic="test",  # ToDo: Should be configurable parameter
            value=msgpack.packb(msg),
            # callback=delivery_report
        )
        self.number_of_send_messages += 1

    def dict(self):
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "check_period_seconds": self.check_period_seconds,
            "stats": self.stats.dict(),
            "number_of_send_messages": self.number_of_send_messages,
            "running": self.running,
        }
