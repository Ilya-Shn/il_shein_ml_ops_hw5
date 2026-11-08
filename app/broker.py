import json
import os

from kafka import KafkaConsumer, KafkaProducer


BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")


def producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, allow_nan=False).encode(),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
    )


def consumer(topic, group):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BROKERS,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=1,
    )


def decode(message):
    return json.loads(message.value.decode())
