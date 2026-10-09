#!/usr/bin/env python3
import os

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from dotenv import load_dotenv

load_dotenv()

TOPIC = os.environ["KAFKA_TOPIC"]
GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP", "avro-consumer")


def error_callback(err):
    print(f"Kafka error: {err}")


consumer = AvroConsumer(
    {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "group.id": GROUP_ID,
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": os.environ["KAFKA_CA"],
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.environ["KAFKA_USERNAME"],
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "schema.registry.url": os.environ["SCHEMA_REGISTRY_URL"],
        "schema.registry.basic.auth.credentials.source": "SASL_INHERIT",
        "schema.registry.ssl.ca.location": os.environ["KAFKA_CA"],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "error_cb": error_callback,
    }
)

consumer.subscribe([TOPIC])
print(f"Ожидание Avro-сообщений из '{TOPIC}'... Ctrl+C для выхода.")

try:
    while True:
        try:
            msg = consumer.poll(timeout=3.0)
        except SerializerError as exc:
            print(f"Ошибка десериализации: {exc}")
            break

        if msg is None:
            continue

        if msg.error():
            print(f"Ошибка сообщения: {msg.error()}")
            continue

        print(
            f"Получено: partition={msg.partition()} offset={msg.offset()} "
            f"key={msg.key()} value={msg.value()}"
        )
        consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
