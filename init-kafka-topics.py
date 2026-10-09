#!/usr/bin/env python3
import os
import sys
from pathlib import Path

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
USERNAME = os.environ["KAFKA_USERNAME"]
PASSWORD = os.environ["KAFKA_PASSWORD"]
CA_PATH = os.environ["KAFKA_CA"]

TOPIC = os.environ.get("KAFKA_TOPIC", "messages")
PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "3"))
REPLICATION_FACTOR = int(os.environ.get("KAFKA_REPLICATION_FACTOR", "3"))

CLEANUP_POLICY = os.environ.get("KAFKA_CLEANUP_POLICY", "delete")
RETENTION_MS = int(os.environ.get("KAFKA_RETENTION_MS", str(7 * 24 * 60 * 60 * 1000)))
SEGMENT_BYTES = int(os.environ.get("KAFKA_SEGMENT_BYTES", str(256 * 1024 * 1024)))
RETENTION_BYTES = int(os.environ.get("KAFKA_RETENTION_BYTES", "-1"))

TOPIC_CONFIG = {
    "cleanup.policy": CLEANUP_POLICY,
    "retention.ms": str(RETENTION_MS),
    "segment.bytes": str(SEGMENT_BYTES),
}

if RETENTION_BYTES >= 0:
    TOPIC_CONFIG["retention.bytes"] = str(RETENTION_BYTES)


def admin_client() -> AdminClient:
    return AdminClient({
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": CA_PATH,
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD,
    })


def apply_config(admin: AdminClient) -> None:
    resource = ConfigResource(
        ConfigResource.Type.TOPIC,
        TOPIC,
        set_config=TOPIC_CONFIG,
    )

    futures = admin.alter_configs([resource])
    futures[resource].result(timeout=30)
    print(f"Конфигурация топика '{TOPIC}' применена: {TOPIC_CONFIG}")


def main() -> None:
    if not Path(CA_PATH).is_file():
        sys.exit(f"CA-сертификат не найден: {CA_PATH}")

    admin = admin_client()
    metadata = admin.list_topics(timeout=15)

    if TOPIC in metadata.topics:
        print(f"Топик '{TOPIC}' уже существует.")
    else:
        topic = NewTopic(
            topic=TOPIC,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            config=TOPIC_CONFIG,
        )

        futures = admin.create_topics([topic])
        futures[TOPIC].result(timeout=30)
        print(
            f"Создан топик '{TOPIC}': "
            f"partitions={PARTITIONS}, replication_factor={REPLICATION_FACTOR}"
        )

    apply_config(admin)


if __name__ == "__main__":
    main()
