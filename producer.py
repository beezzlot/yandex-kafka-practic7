#!/usr/bin/env python3
import os

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from dotenv import load_dotenv

load_dotenv()

TOPIC = os.environ["KAFKA_TOPIC"]

KEY_SCHEMA = """
{
  "namespace": "practice",
  "name": "EventKey",
  "type": "record",
  "fields": [
    {"name": "event_id", "type": "string"}
  ]
}
"""

VALUE_SCHEMA = """
{
  "namespace": "practice",
  "name": "EventValue",
  "type": "record",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "payload", "type": "string"},
    {"name": "created_at", "type": "string"}
  ]
}
"""


def delivery_report(err, msg):
    if err:
        print(f"Ошибка доставки: {err}")
    else:
        print(
            f"Доставлено: topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


producer = AvroProducer(
    {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": os.environ["KAFKA_CA"],
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.environ["KAFKA_USERNAME"],
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "on_delivery": delivery_report,
        "schema.registry.url": os.environ["SCHEMA_REGISTRY_URL"],
        "schema.registry.basic.auth.credentials.source": "SASL_INHERIT",
        "schema.registry.ssl.ca.location": os.environ["KAFKA_CA"],
    },
    default_key_schema=avro.loads(KEY_SCHEMA),
    default_value_schema=avro.loads(VALUE_SCHEMA),
)

for i in range(3):
    from datetime import datetime, timezone

    event_id = f"event-{i + 1}"

    producer.produce(
        topic=TOPIC,
        key={"event_id": event_id},
        value={
            "event_id": event_id,
            "payload": f"test message {i + 1}",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )

producer.flush(15)
