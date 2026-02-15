## AVRO 포맷에서의 CHAR 타입 필드 차이

### MySQL Connector

- char 타입 길이만큼의 공백 패딩 없이 문자열 길이로 인식

```
{
	"id": 17,
	"DATE1": 19559,
	"integer1": 1904927416,
	"char36": "Always let camera fish boy."
}
```

### SQL Server Connector

- char 타입 길이만큼의 공백 패딩이 생김

```
{
	"id": 28,
	"DATE1": 19690,
	"integer1": 436119622,
	"char36": "Rich sell trial local.              "
}
```

## Issue 이력

### Schema Registry 스키마 호환성 이슈 (Backward Compatibility)

Stack Trace는 오류 발생 순서의 역순이므로 맨 아래 나타난 오류가 최초 발생 오류가 됨

```
Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible with an earlier schema for subject "local_mysql.store.tb_lower-value"...
Caused by: io.confluent.kafka.schemaregistry.exceptions.IncompatibleSchemaException: [{errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type inside a union field at path '/fields/0/type/1' in the old schema', additionalInfo:'reader union lacking writer type: RECORD'}...
; error code: 409
```

- 상황: Debezium이 local_mysql.store.tb_lower-value 토픽에 대해 새로운 Avro 스키마를 등록하려고 Schema Registry에 HTTP 요청을 보냄
- 문제: Schema Registry가 이 요청을 "HTTP 409 Conflict" 오류로 거부함
- 사유: 새 스키마가 기존 스키마(v2)와 호환되지 않음(IncompatibleSchemaException). 로그 상세 내역을 보면 boolean1 필드의 타입이 기존 int (connect.type: "int16")에서 새롭게 boolean으로 변경되면서, BACKWARD 호환성 규칙을 위반함
- 해결: Schema Registry의 스키마를 지워서 호환성 이슈를 없애거나, 스키마 제약조건을 `Backward`에서 `None`으로 변경하여 처리
