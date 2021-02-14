"""
This integration tests should be run during build only. Working streaming service and database is required.
"""
import asyncio
from pendulum import datetime, now
from unittest import TestCase, skipIf

import msgpack

from processors.msg_processor import MsgProcessor, Stats
from streaming_client.client_kafka_aio import ClientKafkaAio

try:
    from streaming_client.client_kafka_confluent import ClientKafkaConfluent
except ImportError:
    ClientKafkaConfluent = None


class TestsStreamingClients(TestCase):
    """
    ToDo: create kafka topic with random name for test purposes and delete it at teardown
    """
    async def async_msg_send_receive(self, client_1, client_2=None):
        client_2 = client_2 or client_1

        msg_original = f'confluent {now().timestamp()}'.encode()

        await client_1.push("integration", msg_original)

        await client_2.subscribe(["integration"])
        msg_received = await client_2.pull()
        await client_1.disconnect()
        if client_1 is not client_2:
            await client_2.disconnect()

        return msg_original, msg_received

    @skipIf(ClientKafkaConfluent is None, "ConfluentKafka library not found")
    def test_confluent_streaming_client(self):
        client_1 = ClientKafkaConfluent(kafka_topics=["integration"], consumer_offset="earliest")

        msg_original, msg_received = asyncio.run(self.async_msg_send_receive(client_1))

        self.assertEqual(msg_original, msg_received)

    def test_aiokafka_streaming_client(self):
        client_1 = ClientKafkaAio(kafka_topics=["integration"], consumer_offset="earliest")

        msg_original, msg_received = asyncio.run(self.async_msg_send_receive(client_1))

        self.assertEqual(msg_original, msg_received)

    @skipIf(ClientKafkaConfluent is None, "ConfluentKafka library not found")
    def test_aiokafka_to_confluent(self):
        client_1 = ClientKafkaAio(kafka_topics=["integration"], consumer_offset="earliest")
        client_2 = ClientKafkaConfluent(kafka_topics=["integration"], consumer_offset="earliest")

        msg_original, msg_received = asyncio.run(self.async_msg_send_receive(client_1, client_2))

        self.assertEqual(msg_original, msg_received)

    @skipIf(ClientKafkaConfluent is None, "ConfluentKafka library not found")
    def test_confluent_to_aiokafka(self):
        client_1 = ClientKafkaConfluent(kafka_topics=["integration"], consumer_offset="earliest")
        client_2 = ClientKafkaAio(kafka_topics=["integration"], consumer_offset="earliest")

        msg_original, msg_received = asyncio.run(self.async_msg_send_receive(client_1, client_2))

        self.assertEqual(msg_original, msg_received)


class TestsMsgProcessor(TestCase):
    async def async_msg_processor_run(self, streaming_client):
        await streaming_client.push("integration", msgpack.packb(
            {"job_id": 1, "ts": 1, "granularity": "hour", "response_time": 1, "status": 200}))
        await streaming_client.push("integration", msgpack.packb(
            {"job_id": 1, "ts": 2, "granularity": "hour", "response_time": 1, "status": 200}))
        await streaming_client.push("integration", msgpack.packb(
            {"job_id": 1, "ts": 3, "granularity": "hour", "response_time": 1, "status": 200}))
        await streaming_client.push("integration", msgpack.packb(
            {"job_id": 1, "ts": 4, "granularity": "hour", "response_time": 1, "status": 200}))

        processor = MsgProcessor(topics=["integration"], streaming_client=streaming_client)

        # Disable metrics stats upload
        processor.upload_metric_stats_period = 0

        task = asyncio.create_task(processor.loop())

        while processor.number_of_received_messages < 4:
            await asyncio.sleep(0.1)

        processor.keep_running = False

        await streaming_client.disconnect()
        await asyncio.gather(task)

        return processor.running_stats

    def test_msg_processor_with_aio_kafka(self):
        streaming_client = ClientKafkaAio(kafka_topics=["integration"], consumer_offset="earliest")

        result = asyncio.run(self.async_msg_processor_run(streaming_client))

        self.assertEqual(Stats({200: 4}, 1, 1), result[(1, datetime(1970, 1, 1), 'minute')])
        self.assertEqual(Stats({200: 4}, 1, 1), result[(1, datetime(1970, 1, 1), 'hour')])

    def test_msg_processor_with_confluent_kafka(self):
        streaming_client = ClientKafkaConfluent(kafka_topics=["integration"], consumer_offset="earliest")

        result = asyncio.run(self.async_msg_processor_run(streaming_client))

        self.assertEqual(Stats({200: 4}, 1, 1), result[(1, datetime(1970, 1, 1), 'minute')])
        self.assertEqual(Stats({200: 4}, 1, 1), result[(1, datetime(1970, 1, 1), 'hour')])
