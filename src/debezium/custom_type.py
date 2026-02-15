from enum import Enum


class TransformType(str, Enum):
    tsConvertor = "tsConvertor"
    unwrap = "unwrap"
    ignoreReadOp = "ignoreReadOp"
    formatField = "formatField"
    replaceValue = "replaceValue"
    replaceField = "replaceField"
    hashKey = "hashKey"


class ConnectorType(str, Enum):
    MYSQL = "mysql"
    SQLSERVER = "sqlserver"


class SerializationType(str, Enum):
    AVRO = "avro"
    JSON = "json"
