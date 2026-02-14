import os

import hvac
import mysql.connector
import pandas as pd
import yaml

"""
# Install MySQL Driver on MacOS
pip install mysql-connector-python
"""

MYSQL_QUERY_TEMPLATE = """
SELECT INFO_COL.TABLE_SCHEMA                                           AS DB_NAME /*데이터베이스 이름*/
     , INFO_COL.TABLE_NAME                                             AS TABLE_NAME /*테이블 이름*/
     , IF(KEY_COLS.ORDINAL_POSITION = 1, INFO_TBL.TABLE_COMMENT, NULL) AS TABLE_COMMENT /*테이블 주석*/
     , INFO_COL.ORDINAL_POSITION                                       AS ORDINAL_POSITION /*컬럼 순서*/
     , INFO_COL.COLUMN_NAME                                            AS COLUMN_NAME /*컬럼 이름*/
     , INFO_COL.COLUMN_TYPE                                            AS COLUMN_TYPE /*컬럼 타입*/
     , INFO_COL.CHARACTER_SET_NAME                                     AS CHARACTER_SET /* 컬럼의 문자셋 */
     , INFO_COL.COLLATION_NAME                                         AS COLLATION_NAME /* 컬럼의 정렬 방식 */
     , INFO_COL.DATA_TYPE                                              AS DATA_TYPE /*데이터 타입*/
     , IFNULL(CAST(INFO_COL.CHARACTER_MAXIMUM_LENGTH AS CHAR),
              CONCAT(CAST(NUMERIC_PRECISION AS CHAR), ',',
                     IFNULL(CAST(NUMERIC_SCALE AS CHAR), '0')))        AS COLUMN_LENGTH /*컬럼 길이*/
     , INFO_COL.COLUMN_DEFAULT                                         AS COLUMN_DEFAULT /*컬럼의 기본값*/
     , INFO_COL.IS_NULLABLE                                            AS IS_NULLABLE /*NULL 허용 여부*/
     , IF(UQ_CONSTRAINT.COLUMN_NAME IS NOT NULL, 'YES', 'NO')          AS IS_UNIQUE /*UNIQUE 여부*/
     , IF(INFO_COL.EXTRA LIKE '%auto_increment%', 'YES', 'NO')         AS IS_AUTO_INCREMENT /*AUTO_INCREMENT 여부*/
     , INFO_COL.EXTRA                                                  AS COLUMN_EXTRA /*제약 조건*/
     , IF(KEY_COLS.COLUMN_NAME IS NULL, 'NO', 'YES')                   AS PK /*주 키 여부*/
     , INFO_COL.COLUMN_COMMENT                                         AS COLUMN_COMMENT /*컬럼 코멘트 (설명)*/
     , IF(KEY_COLS.ORDINAL_POSITION = 1, INFO_TBL.CREATE_TIME, NULL)   AS CREATE_TIME /*생성 시간*/
     , IF(KEY_COLS.ORDINAL_POSITION = 1, INFO_TBL.UPDATE_TIME, NULL)   AS UPDATE_TIME /*최종 수정 시간*/
     , IF(KEY_COLS.ORDINAL_POSITION = 1, INFO_TBL.TABLE_ROWS, NULL)    AS 'ROW'/*레코드 개수*/
#      , IF(KEY_COLS.ORDINAL_POSITION = 1, INFO_TBL.DATA_LENGTH + INFO_TBL.INDEX_LENGTH, NULL) AS 'SIZE'/*테이블 크기*/
FROM information_schema.TABLES AS INFO_TBL
         INNER JOIN information_schema.COLUMNS AS INFO_COL
                    ON INFO_TBL.TABLE_SCHEMA = INFO_COL.TABLE_SCHEMA
                        AND INFO_TBL.TABLE_NAME = INFO_COL.TABLE_NAME
         LEFT JOIN (SELECT *
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE CONSTRAINT_NAME = 'PRIMARY') AS KEY_COLS
                   ON INFO_COL.TABLE_SCHEMA = KEY_COLS.TABLE_SCHEMA
                       AND INFO_COL.TABLE_NAME = KEY_COLS.TABLE_NAME
                       AND INFO_COL.COLUMN_NAME = KEY_COLS.COLUMN_NAME
         LEFT JOIN (SELECT COLUMN_NAME, TABLE_SCHEMA, TABLE_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE CONSTRAINT_NAME = 'UNIQUE') AS UQ_CONSTRAINT
                   ON INFO_COL.TABLE_SCHEMA = UQ_CONSTRAINT.TABLE_SCHEMA
                       AND INFO_COL.TABLE_NAME = UQ_CONSTRAINT.TABLE_NAME
                       AND INFO_COL.COLUMN_NAME = UQ_CONSTRAINT.COLUMN_NAME
WHERE CONCAT_WS('.', INFO_COL.TABLE_SCHEMA, INFO_COL.TABLE_NAME) IN ({tables})
ORDER BY INFO_COL.TABLE_SCHEMA, INFO_COL.TABLE_NAME, INFO_COL.ORDINAL_POSITION;
"""


def load_settings(settings_path="settings.yml"):
    with open(settings_path) as f:
        return yaml.safe_load(f)


def get_db_credentials(vault_config, mysql_config):
    client = hvac.Client(url=vault_config["url"])
    client.auth.userpass.login(username=vault_config["username"], password=vault_config["password"])
    secret = client.read(mysql_config["secret_path"])["data"]["data"]
    return secret


def generate_specification(settings):
    tables = settings.get("tables", [])
    if not tables:
        print("No tables specified in settings.")
        return

    vault_config = settings["vault"]
    mysql_config = settings["mysql"]
    output_config = mysql_config["output"]

    try:
        db_secret = get_db_credentials(vault_config, mysql_config)
    except Exception as e:
        print(f"Failed to retrieve credentials from Vault: {e}")
        return

    db_host = mysql_config.get("override_host", db_secret.get("host", "localhost"))
    db_port = mysql_config.get("override_port", db_secret.get("port", 3306))
    db_user = db_secret["user"]
    db_password = db_secret["password"]

    dfs = list()
    databases = dict()
    for line in tables:
        parts = line.split(".")
        if len(parts) != 2:
            print(f"Invalid table format: {line}. Expected schema.table")
            continue
        schema, table = parts
        if schema not in databases:
            databases[schema] = list()
        databases[schema].append(table)

    for database, table_names in databases.items():
        table_list = [f"'{database}.{t}'" for t in table_names]
        table_str = ",".join(table_list)

        try:
            connection = mysql.connector.connect(
                host=db_host, port=db_port, user=db_user, passwd=db_password, db=database
            )
            with connection.cursor() as cursor:
                cursor.execute(MYSQL_QUERY_TEMPLATE.format(tables=table_str))
                rows = cursor.fetchall()
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    dfs.append(pd.DataFrame([list(row) for row in rows], columns=columns))

            connection.close()
        except Exception as e:
            print(f"Error processing database {database}: {e}")

    if dfs:
        table_specification = pd.concat(dfs)
        output_filename = output_config.get("specification_filename", "mysql-tables.xlsx")
        table_specification.to_excel(output_filename)
        print(f"Specification saved to {output_filename}")

        for _, row in table_specification[table_specification["PK"] == "YES"].iterrows():
            print(f"{row['DB_NAME']}.{row['TABLE_NAME']}.{row['COLUMN_NAME']}")
    else:
        print("No data retrieved.")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        generate_specification(settings)
    else:
        print(f"Settings file not found at {settings_path}")
