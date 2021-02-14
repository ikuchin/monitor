from streaming_client.client_kafka_aio import ClientKafkaAio

default_streaming_client = ClientKafkaAio(kafka_topics=["integration"], consumer_offset="earliest")