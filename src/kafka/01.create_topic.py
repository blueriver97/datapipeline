import json
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, KafkaException, NewTopic


def create_topics(client: AdminClient, topics: list[str], partitions=1, replication_factor=3) -> dict:
    """
    Kafka 토픽 생성
    :param client: AdminClient 객체
    :param topics: 생성할 토픽 이름 리스트
    :param partitions: 파티션 수
    :param replication_factor: 복제 수
    :return: {topic_name: result 메시지 또는 에러 문자열}
    """
    result_dict: dict[str, str] = {}

    if not topics:
        print("[WARN] No topics provided for creation.")
        return result_dict

    # NewTopic 객체 생성
    new_topics = [
        NewTopic(
            topic=name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config={"min.insync.replicas": str(max(1, replication_factor - 1))},
        )
        for name in topics
    ]

    # 토픽 생성 요청
    futures = client.create_topics(new_topics)

    # 결과 처리
    for topic, fut in futures.items():
        try:
            fut.result()  # 성공 시 None 반환
            result_dict[topic] = "Created successfully"
        except KafkaException as e:
            result_dict[topic] = f"KafkaException: {e}"
        except Exception as e:
            result_dict[topic] = f"Error: {e}"

    # 결과 JSON 파일 저장
    OUTPUT_FILE.write_text(json.dumps(result_dict, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Topic creation results saved to {OUTPUT_FILE.resolve()}")

    return result_dict


if __name__ == "__main__":
    # 설정 로드
    SETTINGS_PATH = Path(__file__).parent / "settings.yml"
    with open(SETTINGS_PATH, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    KAFKA_CONF = config["kafka"]
    TOPIC_CONF = config["topics"]
    OUTPUT_FILE = Path("topic_create_result.json")

    client = AdminClient({"bootstrap.servers": KAFKA_CONF["bootstrap_servers"]})

    # 설정 파일에서 토픽 이름 가져오기
    topics_to_create = list(TOPIC_CONF["names"].values())

    create_topics(
        client,
        topics_to_create,
        partitions=TOPIC_CONF["default_partitions"],
        replication_factor=TOPIC_CONF["default_replication_factor"],
    )
