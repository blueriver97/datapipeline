import datetime
import json
import string
from dataclasses import dataclass, field
from pathlib import Path

import hvac
from config import AppConfig, ConnectionConfig
from custom_type import ConnectorType, SerializationType, TransformType


@dataclass
class DBZConfig:
    connector: ConnectorType = ConnectorType.MYSQL
    serialization: SerializationType = SerializationType.AVRO
    kafka: str = "local"
    database: str = "local-mysql"
    use_schema_registry: bool = True
    use_ssl: bool = False
    use_sasl: bool = False
    use_error_tolerance: bool = False
    transform_rules: list[TransformType] = field(default_factory=list)


TRANSFORM_CONFIG_MAPPING = {
    TransformType.ignoreReadOp: {
        "type": "io.debezium.transforms.Filter",
        "condition": "value.op != 'r'",
        "language": "jsr223.groovy",
        "topic.regex": "^(${topic_prefix}.*)\\..*",
    },
    TransformType.unwrap: {
        "type": "io.debezium.transforms.ExtractNewRecordState",
        "add.fields": "op,db,table,ts_ms",
        "delete.handling.mode": "rewrite",
        "drop.tombstones": "true",
    },
    TransformType.tsConvertor: {
        "type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
        "field": "ts_ms",
        "format": "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "target.type": "string",
    },
    TransformType.formatField: {
        "type": "com.hunet.kafka.connect.transforms.FormatField$Value",
        "name": "__db_table",
        "fields": "__db,__table",
        "format": "$1_bronze.$2",
    },
    TransformType.replaceValue: {
        "type": "com.hunet.kafka.connect.transforms.ReplaceValue$Value",
        "field": "__op",
        "replace": "c:I,u:U,d:D",
    },
    TransformType.replaceField: {
        "type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
        "renames": "__ts_ms:last_applied_date",
    },
    TransformType.hashKey: {
        "type": "com.hunet.kafka.connect.transforms.HashKey",
        "field": "id_iceberg",
        "delimiter": "|",
        "algorithm": "md5",
    },
}


def __get_connector_class(app: AppConfig, debezium: DBZConfig) -> dict:
    result = {}
    if debezium.connector == ConnectorType.MYSQL:
        table_set = set([f"{app.database}.{table}" for table in app.tables])
        result.update(
            {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.include.list": app.database,
                "table.include.list": ",".join(table_set),
                "topic.prefix": app.prefix,
            }
        )
    elif debezium.connector == ConnectorType.SQLSERVER:
        table_set = set([f"{app.database}.dbo.{table}" for table in app.tables])
        result.update(
            {
                "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
                "database.encrypt": "false",
                "database.names": app.database,
                "table.include.list": ",".join(table_set),
                "topic.prefix": app.prefix,
            }
        )
    else:
        raise ValueError(f"Unsupported connector type: {debezium.connector}")

    return result


def __get_kafka_config(app: AppConfig, debezium: DBZConfig) -> dict:
    kafka_connection = app.kafka[debezium.kafka]
    return {
        "schema.history.internal.kafka.bootstrap.servers": ",".join(kafka_connection.bootstrap_servers),
        "schema.history.internal.kafka.topic": f"schemahistory.{app.prefix}.{app.database}",
        "schema.history.internal.store.only.captured.tables.ddl": "false",
        "schema.history.internal.kafka.recovery.poll.interval.ms": "1000",
    }


def __get_schema_registry_config(app: AppConfig, debezium: DBZConfig) -> dict:
    result = {}
    kafka_connection = app.kafka[debezium.kafka]

    if debezium.use_schema_registry:
        if debezium.serialization == SerializationType.AVRO:
            result.update(
                {
                    "key.converter": "io.confluent.connect.avro.AvroConverter",
                    "key.converter.schema.registry.url": str(kafka_connection.schema_registry),
                    "value.converter": "io.confluent.connect.avro.AvroConverter",
                    "value.converter.schema.registry.url": str(kafka_connection.schema_registry),
                }
            )
        if debezium.serialization == SerializationType.JSON:
            result.update(
                {
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schema.registry.url": str(kafka_connection.schema_registry),
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schema.registry.url": str(kafka_connection.schema_registry),
                }
            )
    else:
        result.update(
            {
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "true",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "true",
            }
        )

    return result


def __get_ssl_config(app: AppConfig, debezium: DBZConfig) -> dict:
    result = {}
    if debezium.use_ssl:
        result.update(
            {
                "schema.history.internal.producer.security.protocol": "SSL",
                "schema.history.internal.producer.ssl.truststore.location": "/etc/kafka/secrets/truststore.jks",
                "schema.history.internal.producer.ssl.truststore.password": "changeit",
                "schema.history.internal.producer.ssl.keystore.location": "/etc/kafka/secrets/kafka.keystore.jks",
                "schema.history.internal.producer.ssl.keystore.password": "changeit",
                "schema.history.internal.producer.ssl.key.password": "changeit",
                "schema.history.internal.consumer.security.protocol": "SSL",
                "schema.history.internal.consumer.ssl.truststore.location": "/etc/kafka/secrets/truststore.jks",
                "schema.history.internal.consumer.ssl.truststore.password": "changeit",
                "schema.history.internal.consumer.ssl.keystore.location": "/etc/kafka/secrets/kafka.keystore.jks",
                "schema.history.internal.consumer.ssl.keystore.password": "changeit",
                "schema.history.internal.consumer.ssl.key.password": "changeit",
            }
        )
    return result


def __get_sasl_config(app: AppConfig, debezium: DBZConfig) -> dict:
    result = {}
    kafka_connection = app.kafka[debezium.kafka]
    if debezium.use_sasl:
        result.update(
            {
                "schema.history.internal.consumer.security.protocol": "SASL_PLAINTEXT",
                "schema.history.internal.consumer.sasl.mechanism": "PLAIN",
                "schema.history.internal.consumer.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_connection.username}' password='{kafka_connection.password}';'",
                "schema.history.internal.producer.security.protocol": "SASL_PLAINTEXT",
                "schema.history.internal.producer.sasl.mechanism": "PLAIN",
                "schema.history.internal.producer.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_connection.username}' password='{kafka_connection.password}';",
            }
        )
    return result


def __get_error_handling_config(app: AppConfig, config: DBZConfig) -> dict:
    result = {}
    if config.use_error_tolerance:
        result.update(
            {
                "errors.tolerance": "all",
                "errors.retry.timeout": "60000",
                "errors.retry.delay.max.ms": "3000",
                "errors.deadletterqueue.topic.name": f"deadletter.{app.prefix}.{app.database}",
                "errors.deadletterqueue.context.headers.enable": "true",
            }
        )

    return result


def __get_transforms(app: AppConfig, debezium: DBZConfig) -> dict:
    result = {}
    template_context = {"topic_prefix": app.prefix}
    if debezium.transform_rules:
        result.update({"transforms": ",".join(debezium.transform_rules)})

    for transform in debezium.transform_rules:
        subconfig = {}
        config = TRANSFORM_CONFIG_MAPPING.get(transform)
        if config is None:
            continue

        for key, value in config.items():
            # i.e) transforms.unwrap.type = io.debezium.transforms.ExtractNewRecordState
            config_key = f"transforms.{transform.name}.{key}"

            if isinstance(value, str):
                value = interpolate(value, template_context)
                # print(f"{config_key} = {value}")
                subconfig[config_key] = value

        result.update(subconfig)
    return result


def interpolate(value: str, context: dict[str, str]) -> str:
    try:
        return string.Template(value).safe_substitute(context)
    except Exception:
        return value


def generate(app: AppConfig, debezium: DBZConfig):
    if not app.tables:
        raise ValueError("source is not defined in settings.yaml")

    client = hvac.Client(url=str(app.vault.url))
    client.auth.userpass.login(username=app.vault.username, password=app.vault.password)
    secret = client.secrets.kv.v2.read_secret_version(
        mount_point=app.vault.mount_point, path=app.vault.path[debezium.database], raise_on_deleted_version=False
    )

    if secret:
        connection = ConnectionConfig(**secret["data"]["data"])
    else:
        raise ValueError(
            f"Failed to get secret from vault (mount_point={app.vault.mount_point}, path={debezium.database})"
        )

    connector_name = f"{connection.name}-{app.database}-connector"
    database_server_id = f"{datetime.datetime.now().strftime('%f')}"

    connector = {
        "name": connector_name,
        "tasks.max": "1",
        "database.server.id": database_server_id,
        "database.hostname": connection.host,
        "database.port": connection.port,
        "database.user": connection.user,
        "database.password": connection.password,
        "heartbeat.interval.ms": "60000",
        "tombstones.on.delete": "false",
        "use.nongraceful.disconnect": "true",
        "include.schema.changes": "true",
        "include.schema.comments": "true",
        "topic.creation.default.replication.factor": "-1",
        "topic.creation.default.partitions": "-1",
        "snapshot.mode": "no_data",
    }

    connector.update(__get_connector_class(app, debezium))
    connector.update(__get_kafka_config(app, debezium))
    connector.update(__get_schema_registry_config(app, debezium))
    connector.update(__get_ssl_config(app, debezium))
    connector.update(__get_sasl_config(app, debezium))
    # connector.update(__get_error_handling_config(app, debezium))
    connector.update(__get_transforms(app, debezium))

    connector.update(
        {
            "time.precision.mode": "adaptive_time_microseconds",
            "decimal.handling.mode": "precise",
            "converters": "boolean",
            "boolean.type": "io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter",
        }
    )

    return connector


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    settings_path = current_dir / "settings.yml"

    try:
        app_config = AppConfig.load(str(settings_path))
    except Exception as e:
        print(f"Warning: Failed to load AppConfig from {settings_path}: {e}")
        raise

    debezium_config = DBZConfig(
        connector=ConnectorType.MYSQL,
        serialization=SerializationType.AVRO,
        kafka="local",
        database="local-mysql",
        use_schema_registry=True,
        use_ssl=False,
        use_sasl=False,
        use_error_tolerance=True,
        transform_rules=[],
    )

    try:
        connector = generate(app_config, debezium_config)
        output_file = f"{connector.get('name')}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(connector, f, ensure_ascii=False, indent=2)
        print(f"Generated connector config: {output_file}")
    except Exception as e:
        print(f"Error generating connector config: {e}")
        raise
