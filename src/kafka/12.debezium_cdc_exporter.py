import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path

import yaml
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Gauge, start_http_server

# 설정 로드
SETTINGS_PATH = Path(__file__).parent / "settings.yml"
with open(SETTINGS_PATH, encoding="utf-8") as f:
    config = yaml.safe_load(f)

KAFKA_CONF = config["kafka"]
DEBEZIUM_CONF = config["debezium"]

# --- Prometheus 지표 정의 ---
DEBEZIUM_SOURCE_TIMESTAMP = Gauge(
    "debezium_source_timestamp_seconds", "Source record timestamp from the database", ["topic"]
)
DEBEZIUM_PROCESSING_TIMESTAMP = Gauge(
    "debezium_processing_timestamp_seconds", "Debezium processing timestamp", ["topic"]
)
SCHEMA_FRESHNESS = Gauge("schema_freshness_seconds", "Time since the last schema update", ["prefix", "schema"])

# --- Kafka 설정 (환경 변수 처리) ---
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", KAFKA_CONF["bootstrap_servers"])
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY", KAFKA_CONF["schema_registry_url"])
SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", str(DEBEZIUM_CONF["scrape_interval"])))
TIMEOUT_LIMIT = int(os.environ.get("TIMEOUT_LIMIT", str(DEBEZIUM_CONF["timeout_limit"])))
DEBEZIUM_PATTERN = re.compile("connect-(configs|offsets|status)")


def find_latest_message_by_topics(bootstrap_servers):
    """
    클러스터 내 모든 토픽 중 내장 토픽(__)을 제외하고,
    각 토픽의 최신 메시지를 가져옵니다.
    """
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "multi-topic-checker",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(conf)
    all_partitions_to_check = []
    try:
        cluster_metadata = consumer.list_topics(timeout=20)
        for topic_name, topic_meta in cluster_metadata.topics.items():
            if topic_name.startswith("_") or DEBEZIUM_PATTERN.search(topic_name):
                continue  # 내장 토픽 제외

            for p_id in topic_meta.partitions.keys():
                all_partitions_to_check.append(TopicPartition(topic_name, p_id))

        if not all_partitions_to_check:
            print("No partitions to check.")
            return None

        partitions_to_assign = []
        for tp in all_partitions_to_check:
            try:
                low, high = consumer.get_watermark_offsets(tp, timeout=5)

                if low == high:
                    continue  # noqa: E701
                if high > 0:
                    tp.offset = high - 1
                    partitions_to_assign.append(tp)

            except KafkaException as e:
                print(f"Failed to get watermark offsets for partition {tp}: {e}")
                continue

        if not partitions_to_assign:
            print("No messages found in any topic.")
            return None

        consumer.assign(partitions_to_assign)

        # 3. 상태 관리: 아직 데이터를 받지 못한 파티션 세트
        waiting_for_partitions = {(tp.topic, tp.partition) for tp in partitions_to_assign}
        latest_messages_by_topic = {}
        start_time = time.time()

        while waiting_for_partitions and (time.time() - start_time < TIMEOUT_LIMIT):
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue  # noqa: E701

            tp_key = (msg.topic(), msg.partition())
            if tp_key in waiting_for_partitions:
                waiting_for_partitions.discard(tp_key)

            # PARTITION_EOF나 기타 에러가 나도 이미 discard 했으므로 continue
            if msg.error():
                continue

            # 4. 데이터 필터링 (AVRO 유형 체크 등)
            if msg.value() is None or msg.value()[0] != 0:
                continue

            # 5. 모든 조건을 통과한 '유효한 최신 메시지' 저장
            topic = msg.topic()
            if (
                topic not in latest_messages_by_topic
                or msg.timestamp()[1] > latest_messages_by_topic[topic].timestamp()[1]
            ):
                latest_messages_by_topic[topic] = msg

        return latest_messages_by_topic

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    finally:
        consumer.close()


def update_metrics():
    """
    Debezium 지연 시간 관련 메트릭을 주기적으로 업데이트합니다.
    """
    now = datetime.now(UTC)
    print(f"Updating metrics...{now.strftime('%Y-%m-%d %H:%M:%S.%f')}")

    schema_registry_conf = {"url": SCHEMA_REGISTRY}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_deserializer = AvroDeserializer(schema_registry_client)

    latest_msg_overall = find_latest_message_by_topics(BOOTSTRAP_SERVERS)
    if not latest_msg_overall:
        print("No messages found.")

    topic_metrics = {}
    schema_metrics = {}
    for topic, msg in latest_msg_overall.items():
        # 모든 토픽이 AVRO를 사용하지 않고, Debezium 포맷이 아니기 때문에 구분

        message_value = avro_deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
        if message_value is None:
            continue

        if topic.count(".") != 2:
            continue

        # UTC 기준 datetime 객체 생성
        if "source" in message_value and "ts_ms" in message_value:
            prefix, schema, table = topic.split(".")
            source_ts = datetime.fromtimestamp(message_value["source"]["ts_ms"] / 1000, UTC)
            if isinstance(message_value["ts_ms"], str):
                debezium_ts = datetime.fromisoformat(message_value["ts_ms"]).replace(tzinfo=UTC)
            else:
                debezium_ts = datetime.fromtimestamp(message_value["ts_ms"] / 1000.0, UTC)

            # 지연 시간 계산
            latency = (debezium_ts - source_ts).total_seconds()
            freshness = max((now - debezium_ts).total_seconds(), 0.0)

            # 스키마 별 신선도 계산
            freshness_key = f"{prefix}.{schema}"
            if freshness_key not in schema_metrics:
                schema_metrics[freshness_key] = freshness
            else:
                schema_metrics[freshness_key] = min(freshness, schema_metrics[freshness_key])

            # Prometheus Gauge 값 설정
            DEBEZIUM_SOURCE_TIMESTAMP.labels(topic=topic).set(source_ts.timestamp())
            DEBEZIUM_PROCESSING_TIMESTAMP.labels(topic=topic).set(debezium_ts.timestamp())
            SCHEMA_FRESHNESS.labels(prefix=prefix, schema=schema).set(schema_metrics[freshness_key])

            topic_metrics[topic] = {
                "source_ts": source_ts,
                "debezium_ts": debezium_ts,
                "latency": latency,
                "freshness": freshness,
            }

    topic_metrics = sorted(
        topic_metrics.items(), key=lambda x: (x[0].rsplit(".", 1)[0], x[1]["freshness"]), reverse=False
    )
    for topic, metric in topic_metrics:
        debezium_ts = metric["debezium_ts"]
        latency = metric["latency"]
        freshness = metric["freshness"]
        print(
            f"Topic: {topic}, D: {debezium_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}, L: {latency:.3f}s, F: {freshness:.3f}s"
        )

    for schema_key in sorted(schema_metrics.keys()):
        print(f"Schema: {schema_key}, F: {schema_metrics[schema_key]:.3f}s")


if __name__ == "__main__":
    start_http_server(8000)
    print("Prometheus exporter started on port 8000")
    while True:
        update_metrics()
        time.sleep(SCRAPE_INTERVAL)
