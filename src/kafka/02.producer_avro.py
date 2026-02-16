from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import yaml
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from dataclasses_avroschema import AvroModel

# 설정 로드
SETTINGS_PATH = Path(__file__).parent / "settings.yml"
with open(SETTINGS_PATH, encoding="utf-8") as f:
    config = yaml.safe_load(f)

KAFKA_CONF = config["kafka"]
TOPIC_NAME = config["topics"]["names"]["avro"]


@dataclass
class Value(AvroModel):
    name: str
    age: int
    email: str


def delivery_report(err, msg):
    """
    메시지 전송 성공/실패 여부를 보고합니다.
    """
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    print(
        f"User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
    )


def produce_avro_message():
    """
    Avro 메시지를 Kafka 토픽으로 전송합니다.
    """
    schema_registry_conf = {"url": KAFKA_CONF["schema_registry_url"]}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_schema = Value.avro_schema()
    string_serializer = StringSerializer("utf_8")
    avro_serializer = AvroSerializer(schema_registry_client, avro_schema)

    producer_conf = {
        "bootstrap.servers": KAFKA_CONF["bootstrap_servers"],
    }
    producer = Producer(producer_conf)

    key = str(uuid4())
    value = {"name": "Alice", "age": 30, "email": "alice@example.com"}

    producer.produce(
        topic=TOPIC_NAME,
        key=string_serializer(key),
        value=avro_serializer(
            value,
            SerializationContext(TOPIC_NAME, MessageField.VALUE),
        ),
        on_delivery=delivery_report,
    )
    producer.flush()


if __name__ == "__main__":
    produce_avro_message()
