"""Kafka configuration and connection management."""

import os
from kafka import KafkaConsumer, KafkaProducer
import json
import yaml


def load_config():
    """Load configuration from config.yaml."""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)


def get_kafka_consumer() -> KafkaConsumer:
    """
    Create and return Kafka consumer.

    Returns:
        KafkaConsumer: Kafka consumer instance
    """
    config = load_config()
    kafka_config = config['kafka']

    bootstrap_servers = os.getenv(
        'KAFKA_BOOTSTRAP_SERVERS',
        kafka_config['bootstrap_servers']
    )

    topic = kafka_config['topic']
    group_id = kafka_config['group_id']
    auto_offset_reset = kafka_config.get('auto_offset_reset', 'earliest')

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000  # 10 seconds timeout
    )

    print(f"Connected to Kafka at {bootstrap_servers}")
    print(f"Subscribed to topic: {topic}")

    return consumer


def get_kafka_producer() -> KafkaProducer:
    """
    Create and return Kafka producer.

    Returns:
        KafkaProducer: Kafka producer instance
    """
    config = load_config()
    kafka_config = config['kafka']

    bootstrap_servers = os.getenv(
        'KAFKA_BOOTSTRAP_SERVERS',
        kafka_config['bootstrap_servers']
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Kafka producer connected at {bootstrap_servers}")

    return producer
