#!/usr/bin/env python3
import json
import os
import sys

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC", "events-processed")
GROUP_ID = os.getenv("KAFKA_PROCESSED_CONSUMER_GROUP", "processed-topic-verifier-v1")


def error_callback(error):
    print(f"Kafka error: {error}", file=sys.stderr)


def main():
    required_vars = [
        "KAFKA_BOOTSTRAP",
        "KAFKA_CA",
        "KAFKA_USERNAME",
        "KAFKA_PASSWORD",
    ]

    missing = [name for name in required_vars if not os.getenv(name)]
    if missing:
        sys.exit(
            "В .env отсутствуют обязательные переменные: "
            + ", ".join(missing)
        )

    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": os.environ["KAFKA_CA"],
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.environ["KAFKA_USERNAME"],
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "error_cb": error_callback,
    })

    consumer.subscribe([TOPIC])

    print(f"Consumer group: {GROUP_ID}")
    print(f"Ожидание сообщений из топика '{TOPIC}'...")
    print("Для остановки нажмите Ctrl+C.\n")

    try:
        while True:
            message = consumer.poll(timeout=3.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka message error: {message.error()}", file=sys.stderr)
                continue

            key = (
                message.key().decode("utf-8")
                if message.key() is not None
                else None
            )

            raw_value = message.value().decode("utf-8")

            try:
                value = json.loads(raw_value)
                payload = json.dumps(
                    value,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            except json.JSONDecodeError:
                payload = raw_value

            print("=" * 80)
            print(f"topic:     {message.topic()}")
            print(f"partition: {message.partition()}")
            print(f"offset:    {message.offset()}")
            print(f"key:       {key}")
            print("value:")
            print(payload)

            consumer.commit(message=message, asynchronous=False)
            print("offset committed")

    except KeyboardInterrupt:
        print("\nОстановка consumer...")

    except KafkaException as exc:
        print(f"\nKafkaException: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        consumer.close()


if __name__ == "__main__":
    main()

