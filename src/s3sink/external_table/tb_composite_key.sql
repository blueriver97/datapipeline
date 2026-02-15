CREATE EXTERNAL TABLE IF NOT EXISTS `raw_data`.`tb_composite_key_cdc` (
  `before` STRUCT<
    `id`: INT,
    `char36`: STRING,
    `varchar36`: STRING,
    `integer1`: INT,
    `integer2`: BIGINT,
    `unsigned_int`: BIGINT,
    `float1`: FLOAT,
    `double1`: DOUBLE,
    `decimal1`: DECIMAL(12, 8),
    `boolean1`: BOOLEAN,
    `BLOB1`: BINARY,
    `TEXT1`: STRING,
    `DATE1`: INT,
    `TIME1`: BIGINT,
    `DATETIME1`: BIGINT,
    `CREATE_DATETIME`: BIGINT,
    `UPDATE_TIMESTAMP`: STRING
  >,
  `after` STRUCT<
    `id`: INT,
    `char36`: STRING,
    `varchar36`: STRING,
    `integer1`: INT,
    `integer2`: BIGINT,
    `unsigned_int`: BIGINT,
    `float1`: FLOAT,
    `double1`: DOUBLE,
    `decimal1`: DECIMAL(12, 8),
    `boolean1`: BOOLEAN,
    `BLOB1`: BINARY,
    `TEXT1`: STRING,
    `DATE1`: INT,
    `TIME1`: BIGINT,
    `DATETIME1`: BIGINT,
    `CREATE_DATETIME`: BIGINT,
    `UPDATE_TIMESTAMP`: STRING
  >,
  `source` STRUCT<
    `version`: STRING,
    `connector`: STRING,
    `name`: STRING,
    `ts_ms`: BIGINT,
    `snapshot`: STRING,
    `db`: STRING,
    `sequence`: STRING,
    `ts_us`: BIGINT,
    `ts_ns`: BIGINT,
    `table`: STRING,
    `server_id`: BIGINT,
    `gtid`: STRING,
    `file`: STRING,
    `pos`: BIGINT,
    `row`: INT,
    `thread`: BIGINT,
    `query`: STRING
  >,
  `op` STRING,
  `ts_ms` BIGINT,
  `ts_us` BIGINT,
  `ts_ns` BIGINT
)
PARTITIONED BY (
  `year` STRING,
  `month` STRING,
  `day` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://blueriver-datalake/data/topics/local.store.TB_COMPOSITE_KEY/'
TBLPROPERTIES (
  'classification'='parquet',
  'parquet.mergerSchema'='true',
  'projection.enabled' = 'true',
  'projection.year.type' = 'date',
  'projection.year.range' = '2025,NOW',
  'projection.year.format' = 'yyyy',
  'projection.month.type' = 'integer',
  'projection.month.range' = '1,12',
  'projection.month.digits' = '2',
  'projection.day.type' = 'integer',
  'projection.day.range' = '1,31',
  'projection.day.digits' = '2',
  'storage.location.template' = 's3://blueriver-datalake/data/topics/local.store.TB_COMPOSITE_KEY/year=${year}/month=${month}/day=${day}/'
);
