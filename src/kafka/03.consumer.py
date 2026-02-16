import json
from pathlib import Path

import yaml
from confluent_kafka import Consumer, KafkaException

# 설정 로드
SETTINGS_PATH = Path(__file__).parent / "settings.yml"
with open(SETTINGS_PATH, encoding="utf-8") as f:
    config = yaml.safe_load(f)

KAFKA_CONF = config["kafka"]
CONSUMER_CONF = config["consumer"]
TOPIC_NAME = config["topics"]["names"]["json"]


def stats_cb(stats_json_str):
    """
    Kafka 클라이언트 통계 콜백 함수
    """
    stats_json = json.loads(stats_json_str)
    print(stats_json)


def print_assignment(consumer, partitions):
    """
    파티션 할당 시 호출되는 콜백 함수
    """
    print("Assignment:", partitions)


def consume_json_messages():
    """
    Kafka 토픽에서 JSON 메시지를 소비합니다.
    """
    consumer_conf = {
        "bootstrap.servers": KAFKA_CONF["bootstrap_servers"],
        "group.id": CONSUMER_CONF["group_id"],
        "session.timeout.ms": CONSUMER_CONF["session_timeout_ms"],
        "auto.offset.reset": CONSUMER_CONF["auto_offset_reset"],
        "enable.auto.offset.store": False,
        "stats_cb": stats_cb,
        "statistics.interval.ms": 600_000,
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics=[TOPIC_NAME], on_assign=print_assignment)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # 정상 메시지 처리
                print(f"%% {msg.topic()} [{msg.partition()}] at offset {msg.offset()} with key {msg.key()}:")
                print(msg.value().decode("utf-8"))
                # 오프셋 저장
                consumer.store_offsets(msg)

    except KeyboardInterrupt:
        print("%% Aborted by user")

    finally:
        # 컨슈머 종료 및 오프셋 커밋
        consumer.close()


if __name__ == "__main__":
    consume_json_messages()
