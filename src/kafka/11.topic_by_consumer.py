import json
from pathlib import Path

import yaml
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

# 설정 로드
SETTINGS_PATH = Path(__file__).parent / "settings.yml"
with open(SETTINGS_PATH, encoding="utf-8") as f:
    config = yaml.safe_load(f)

KAFKA_CONF = config["kafka"]
OUTPUT_FILE = Path("consumer_topics.json")


def export_consumer_subscriptions():
    """
    Kafka 컨슈머 그룹별 구독 토픽 목록을 조회하여
    JSON 파일(consumer_topics.json)로 저장합니다.
    """
    consumer_subscriptions = {}

    try:
        # 1. AdminClient 인스턴스 생성
        print("Connecting to Kafka brokers...")
        admin_client = AdminClient({"bootstrap.servers": KAFKA_CONF["bootstrap_servers"]})

        # 2. 모든 컨슈머 그룹 ID 목록 가져오기
        print("Fetching consumer groups...")
        list_groups_future = admin_client.list_consumer_groups(request_timeout=30.0)
        list_groups_result = list_groups_future.result()

        if not list_groups_result.valid:
            print("No active consumer groups found.")
            OUTPUT_FILE.write_text("{}", encoding="utf-8")
            return

        group_ids = [group.group_id for group in list_groups_result.valid]

        # 3. 각 컨슈머 그룹의 상세 정보 조회
        print(f"Describing {len(group_ids)} consumer groups...")
        describe_groups_futures_dict = admin_client.describe_consumer_groups(group_ids, request_timeout=10.0)

        # 4. 결과 파싱 및 딕셔너리에 저장
        for group_id, future in describe_groups_futures_dict.items():
            try:
                group_description = future.result()
                subscribed_topics = set()

                if group_description.members:
                    for member in group_description.members:
                        if member.assignment:
                            for tp in member.assignment.topic_partitions:
                                subscribed_topics.add(tp.topic)

                # 토픽 알파벳 정렬
                consumer_subscriptions[group_description.group_id] = sorted(subscribed_topics)

            except KafkaException as e:
                consumer_subscriptions[group_id] = [f"Error describing group: {e}"]

        # 5. consumer_group 키도 알파벳 순으로 정렬하여 저장
        sorted_result = {group: consumer_subscriptions[group] for group in sorted(consumer_subscriptions.keys())}

        # JSON 파일로 저장 (UTF-8, pretty print)
        OUTPUT_FILE.write_text(json.dumps(sorted_result, indent=2, ensure_ascii=False), encoding="utf-8")

        print(f"Export completed: {OUTPUT_FILE.resolve()}")

    except KafkaException as e:
        error_msg = f"A Kafka-related error occurred: {e}"
        print(error_msg)
        OUTPUT_FILE.write_text(json.dumps({"error": error_msg}, indent=2), encoding="utf-8")
    except Exception as e:
        error_msg = f"An unexpected error occurred: {e}"
        print(error_msg)
        OUTPUT_FILE.write_text(json.dumps({"error": error_msg}, indent=2), encoding="utf-8")


if __name__ == "__main__":
    export_consumer_subscriptions()
