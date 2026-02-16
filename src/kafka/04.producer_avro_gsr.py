from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import boto3
import yaml
from aws_schema_registry import SchemaRegistryClient
from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.serde import KafkaSerializer
from confluent_kafka import Producer
from dataclasses_avroschema import AvroModel

# 설정 로드
SETTINGS_PATH = Path(__file__).parent / "settings.yml"
with open(SETTINGS_PATH, encoding="utf-8") as f:
    config = yaml.safe_load(f)

KAFKA_CONF = config["kafka"]
AWS_CONF = config["aws"]
TOPIC_NAME = config["topics"]["names"]["avro_gsr"]


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


def check_glue_schema_registry():
    """
    AWS Glue Schema Registry를 생성합니다.
    """
    registry_name = AWS_CONF["registry_name"]
    session = boto3.Session(profile_name=AWS_CONF["profile_name"], region_name=AWS_CONF["region_name"])
    glue_client = session.client("glue")

    try:
        glue_client.get_registry(RegistryId={"RegistryName": registry_name})
        print(f"Registry '{registry_name}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        try:
            glue_client.create_registry(RegistryName=registry_name, Description="Glue Schema Registry")
            print(f"Registry '{registry_name}' created successfully.")
        except Exception as e:
            print(f"Failed to create registry. {e}")
            raise
    except Exception as e:
        print(f"Failed to get registry. {e}")
        raise


def produce_avro_gsr_message():
    """
    AWS Glue Schema Registry를 사용하여 Avro 메시지를 Kafka 토픽으로 전송합니다.
    """
    session = boto3.Session(profile_name=AWS_CONF["profile_name"], region_name=AWS_CONF["region_name"])
    glue_client = session.client("glue")
    client = SchemaRegistryClient(glue_client, registry_name=AWS_CONF["registry_name"])
    serializer = KafkaSerializer(client)

    producer_conf = {
        "bootstrap.servers": KAFKA_CONF["bootstrap_servers"],
    }
    producer = Producer(producer_conf)

    key = str(uuid4())
    value = {"name": "Alice", "age": 30, "email": "alice@example.com"}

    producer.produce(
        topic=TOPIC_NAME,
        key=key,
        value=serializer.serialize(
            topic=TOPIC_NAME,
            data_and_schema=(value, AvroSchema(Value.avro_schema())),
        ),
        on_delivery=delivery_report,
    )
    producer.flush()


if __name__ == "__main__":
    check_glue_schema_registry()
    produce_avro_gsr_message()
