#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv()

TOPIC = "events-processed"

admin = AdminClient({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": os.environ["KAFKA_CA"],
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": os.environ["KAFKA_USERNAME"],
    "sasl.password": os.environ["KAFKA_PASSWORD"],
})

metadata = admin.list_topics(timeout=15)

if TOPIC in metadata.topics:
    print(f"Топик '{TOPIC}' уже существует")
else:
    futures = admin.create_topics([
        NewTopic(
            topic=TOPIC,
            num_partitions=3,
            replication_factor=3,
            config={
                "cleanup.policy": "delete",
                "retention.ms": "604800000",
                "segment.bytes": "268435456",
            },
        )
    ])
    futures[TOPIC].result(timeout=30)
    print(f"Создан топик '{TOPIC}'")
