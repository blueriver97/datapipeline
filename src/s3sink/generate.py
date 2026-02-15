import json
import os
import shutil
from pathlib import Path

import boto3
import yaml
from botocore.exceptions import ProfileNotFound


def load_settings(settings_path: str) -> dict:
    with open(settings_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def prepare_directory(output_dir: str) -> None:
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)


def __get_connector_base_config(connector_name: str, common_config: dict) -> dict:
    return {
        "name": connector_name,
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "topics": ",".join(common_config["topics"]),
        "topics.dir": common_config["topics_dir"],
        "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
        "flush.size": str(common_config["flush_size"]),
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "s3.compression.type": "none",
        "behavior.on.null.values": "ignore",
        "timestamp.extractor": "Record",
        "locale": "ko_KR",
        "timezone": "Asia/Seoul",
    }


def __get_s3_config(credentials, common_config) -> dict:
    return {
        "s3.bucket.name": common_config["bucket_name"],
        "s3.region": "ap-northeast-2",
        "aws.access.key.id": credentials.access_key,
        "aws.secret.access.key": credentials.secret_key,
    }


def __get_partitioner_config(common_config: dict) -> dict:
    return {
        "partitioner.class": common_config["partitioner"]["class"],
        "rotate.schedule.interval.ms": str(common_config["partitioner"]["rotate_schedule_interval_ms"]),
    }


def __get_error_handling_config(common_config: dict) -> dict:
    return {
        "errors.tolerance": "none",
        "errors.deadletterqueue.topic.name": common_config["dead_letter_topic"],
        "errors.deadletterqueue.context.headers.enable": "true",
        "errors.deadletterqueue.topic.replication.factor": "-1",
    }


def __get_converter_config(common_config: dict) -> dict:
    config = {}
    serialization = common_config.get("serialization", "AVRO")
    sr_config = common_config.get("schema_registry", {})
    sr_url = sr_config.get("url")

    if serialization == "AVRO":
        converter_class = "io.confluent.connect.avro.AvroConverter"
    elif serialization == "JSON_SCHEMA":
        converter_class = "io.confluent.connect.json.JsonSchemaConverter"
    else:
        converter_class = "org.apache.kafka.connect.json.JsonConverter"

    config.update(
        {
            "key.converter": converter_class,
            "value.converter": converter_class,
        }
    )

    if serialization in ["AVRO", "JSON_SCHEMA"]:
        sr_settings = {
            "key.converter.schema.registry.url": sr_url,
            "value.converter.schema.registry.url": sr_url,
            "schema.compatibility": sr_config.get("compatibility", "NONE"),
        }
        config.update(sr_settings)
    else:
        # 일반 JSON인 경우 스키마 포함 여부를 false로 설정 (성능 최적화)
        config.update(
            {
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
            }
        )

    # 4. Header Converter (메타데이터용으로 고정)
    config["header.converter"] = "org.apache.kafka.connect.storage.StringConverter"

    return config


def get_aws_credentials(aws_config: dict):
    profile = aws_config.get("profile", "default")
    override_access_key = aws_config.get("override_access_key")
    override_secret_key = aws_config.get("override_secret_key")
    creds = None

    # 1. Override 설정이 있으면 우선 사용
    if override_access_key and override_secret_key:
        print("Using overridden AWS credentials from settings.")
        session = boto3.Session(aws_access_key_id=override_access_key, aws_secret_access_key=override_secret_key)
        return session.get_credentials()

    # 2. 프로필 사용 시도
    if profile:
        try:
            session = boto3.Session(profile_name=profile)
            creds = session.get_credentials()
            if creds:
                print(f"Using AWS profile: {profile}")
                return creds
        except ProfileNotFound:
            print(f"Warning: AWS profile '{profile}' not found.")
        except Exception as e:
            print(f"Warning: Failed to load profile '{profile}': {e}")

    if not creds:
        raise ValueError("No AWS credentials found. Please configure profile, override keys.")

    return creds


def generate(common_config: dict, credentials, output_dir: str):
    connector_name = f"s3sink-{common_config['server_name']}-connector"

    config = {}
    config.update(__get_connector_base_config(connector_name, common_config))
    config.update(__get_s3_config(credentials, common_config))
    config.update(__get_partitioner_config(common_config))
    # config.update(__get_error_handling_config(common_config))
    config.update(__get_converter_config(common_config))

    output_file = Path(output_dir) / f"{connector_name}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"Generated config: {output_file}")


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    settings_path = current_dir / "settings.yml"

    try:
        settings = load_settings(str(settings_path))
    except FileNotFoundError:
        print(f"Settings file not found at {settings_path}")
        raise
    except yaml.YAMLError as e:
        print(f"Error parsing settings file: {e}")
        raise

    s3sink_config = settings.get("s3sink", {})
    output_dir = s3sink_config.get("output_dir", "s3sink-configs")
    aws_config = s3sink_config.get("aws", {})
    common_config = s3sink_config.get("common", {})

    prepare_directory(output_dir)

    try:
        credentials = get_aws_credentials(aws_config)
    except Exception as e:
        print(f"Error getting AWS credentials: {e}")
        raise

    try:
        generate(common_config, credentials, output_dir)
    except Exception as e:
        print(f"Error generating connector config: {e}")
        raise
