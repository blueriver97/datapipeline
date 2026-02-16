import json
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, KafkaException

# 설정 로드
SETTINGS_PATH = Path(__file__).parent / "settings.yml"
with open(SETTINGS_PATH, encoding="utf-8") as f:
    config = yaml.safe_load(f)

KAFKA_CONF = config["kafka"]
OUTPUT_FILE = Path("topic_consumers.json")


def export_topic_consumers():
    """
    Kafka 토픽별 구독 컨슈머 그룹을 조회하여
    JSON 파일(topic_consumers.json)로 저장합니다.
    """
    try:
        client = AdminClient({"bootstrap.servers": KAFKA_CONF["bootstrap_servers"]})

        # 토픽 목록 가져오기
        topics = client.list_topics(timeout=10).topics
        topic_names = sorted(topics.keys())

        # 컨슈머 그룹 목록 가져오기
        consumer_groups = client.list_consumer_groups().result().valid

        # 토픽 → 컨슈머 그룹 매핑 초기화
        consumer_dict: dict[str, list] = {topic: [] for topic in topic_names}

        # 각 컨슈머 그룹 상세 조회
        for consumer in consumer_groups:
            try:
                consumer_desc = client.describe_consumer_groups(group_ids=[consumer.group_id])
                desc = consumer_desc[consumer.group_id].result()

                group_id = desc.group_id
                for member in desc.members:
                    if member.assignment:
                        for tp in member.assignment.topic_partitions:
                            if tp.topic in consumer_dict:
                                consumer_dict[tp.topic].append(group_id)

            except KafkaException as e:
                print(f"[WARN] Failed to describe group {consumer.group_id}: {e}")

        # 컨슈머 그룹 리스트 정렬
        sorted_result = {topic: sorted(groups) for topic, groups in consumer_dict.items()}

        # JSON 파일 저장
        OUTPUT_FILE.write_text(
            json.dumps(sorted_result, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        print(f"[INFO] Export completed: {OUTPUT_FILE.resolve()}")

    except Exception as e:
        error_msg = f"[ERROR] Unexpected error: {e}"
        print(error_msg)
        OUTPUT_FILE.write_text(json.dumps({"error": error_msg}, indent=2), encoding="utf-8")


if __name__ == "__main__":
    export_topic_consumers()
