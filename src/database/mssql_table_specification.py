import os

import hvac
import pandas as pd
import pyodbc
import yaml

"""
# Install ODBC 17 Driver on MacOS

/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql17 msodbcsql18 mssql-tools
"""

# SQL Query Template
MSSQL_QUERY_TEMPLATE = """
    WITH
    /*테이블 명세서*/
        TBL_SPEC       AS (SELECT SYS_TBLS.object_id /*테이블 ID*/
                                , SYS_TBLS.create_date /*테이블 생성 시간*/
                                , SYS_TBLS.modify_date /*테이블 최종 수정 시간*/
                                , SYS_COLS.column_id /*컬럼 ID*/
                                , INFO_COL.TABLE_CATALOG /*카탈로그 이름*/
                                , INFO_COL.TABLE_NAME /*테이블 이름*/
                                , INFO_COL.ORDINAL_POSITION /*컬럼의 순서*/
                                , INFO_COL.COLUMN_NAME /*컬럼 이름*/
                                , INFO_COL.CHARACTER_MAXIMUM_LENGTH /*최대 문자 길이*/
                                , INFO_COL.NUMERIC_PRECISION /*숫자의 정밀도*/
                                , INFO_COL.NUMERIC_SCALE /*숫자의 소수 자릿수*/
                                , INFO_COL.COLUMN_DEFAULT /*컬럼의 기본값*/
                                , INFO_COL.DATA_TYPE /*데이터 타입*/
                                , INFO_COL.IS_NULLABLE /*NULL 허용 여부*/
                                , INFO_COL.COLLATION_NAME /*정렬 방식 (문자셋 포함)*/
                           FROM sys.tables AS SYS_TBLS
                                    INNER JOIN sys.columns AS SYS_COLS
                                               ON SYS_TBLS.object_id = SYS_COLS.object_id
                                    INNER JOIN INFORMATION_SCHEMA.COLUMNS AS INFO_COL
                                               ON SYS_TBLS.name = INFO_COL.TABLE_NAME
                                                   AND SYS_COLS.name = INFO_COL.COLUMN_NAME
                           WHERE SYS_TBLS.schema_id = 1)
    /*테이블 주석*/
       , TBL_COMMENT   AS (SELECT OBJECT_ID(objname)           AS TABLE_ID /*테이블 ID*/
                                , CAST(value AS NVARCHAR(255)) AS VALUE /*테이블 주석*/
                           FROM sys.FN_LISTEXTENDEDPROPERTY(NULL
                               , 'USER'
                               , 'DBO'
                               , 'TABLE'
                               , NULL
                               , NULL
                               , NULL))
    /*테이블 레코드 개수*/
       , TBL_ROW       AS (SELECT DISTINCT SYS_PS.object_id/*테이블 ID*/
                                         , SUM(SYS_PS.row_count) AS ROW_COUNT /*레코드 개수*/
                           FROM sys.dm_db_partition_stats AS SYS_PS
                           WHERE index_id IN (0, 1)
                           GROUP BY object_id)
    /*PK 컬럼 목록*/
       , KEY_COLS      AS (SELECT DISTINCT TABLE_NAME /*테이블 이름*/
                                         , COLUMN_NAME /*PK 컬럼 이름*/
                           FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                           WHERE (CONSTRAINT_NAME LIKE 'pk_%')
                              OR (CONSTRAINT_NAME LIKE 'cpk_%')
                              OR (CONSTRAINT_NAME LIKE 'npk_%')
                              OR (CONSTRAINT_NAME LIKE '%_pk')
                              OR (CONSTRAINT_NAME LIKE 'pk%_'))
    /*컬럼 주석*/
       , SYS_EXTPROP   AS (SELECT DISTINCT class
                                         , class_desc
                                         , major_id
                                         , minor_id
                                         , CAST(VALUE AS NVARCHAR(255)) AS VALUE
                           FROM sys.EXTENDED_PROPERTIES AS SYS_EXTPROP
                           WHERE class_desc = 'OBJECT_OR_COLUMN'
                             AND NAME = 'MS_Description')

    /*UNIQUE 제약 조건*/
       , UQ_CONSTRAINT AS (SELECT TABLE_NAME, COLUMN_NAME
                           FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                           WHERE CONSTRAINT_NAME LIKE 'UQ_%'
                              OR CONSTRAINT_NAME LIKE '%_UQ')

    /* AUTO_INCREMENT (IDENTITY) 컬럼 */
       , IS_IDENTITY   AS (SELECT TABLE_NAME, COLUMN_NAME
                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1)

    SELECT TBL_SPEC.TABLE_CATALOG                                         AS DB_NAME /*카탈로그 이름*/
         , TBL_SPEC.TABLE_NAME                                            AS TABLE_NAME /*테이블 이름*/
         , IIF(TBL_SPEC.ORDINAL_POSITION = 1, TBL_COMMENT.VALUE, NULL)    AS TABLE_COMMENT /*테이블 코멘트 (설명)*/
         , TBL_SPEC.ORDINAL_POSITION                                      AS ORDINAL_POSITION /*컬럼 순서*/
         , TBL_SPEC.COLUMN_NAME                                           AS COLUMN_NAME /*컬럼 이름*/
         , NULL                                                           AS COLUMN_TYPE /*컬럼 타입*/
         , NULL                                                           AS CHARACTER_SET /* 컬럼의 문자셋 */
         , TBL_SPEC.COLLATION_NAME                                        AS COLLATION_NAME /*정렬 방식 (문자셋 포함)*/
         , TBL_SPEC.DATA_TYPE                                             AS DATA_TYPE /*데이터 타입*/
         , ISNULL(CAST(TBL_SPEC.CHARACTER_MAXIMUM_LENGTH AS VARCHAR),
                  CAST(TBL_SPEC.NUMERIC_PRECISION AS VARCHAR) + ',' +
                  CAST(TBL_SPEC.NUMERIC_SCALE AS VARCHAR))                AS COLUMN_LENGTH /*컬럼 길이*/
         , TBL_SPEC.COLUMN_DEFAULT                                        AS COLUMN_DEFAULT /*컬럼의 기본값*/
         , TBL_SPEC.IS_NULLABLE                                           AS IS_NULLABLE /*NULL 허용 여부*/
         , IIF(UQ_CONSTRAINT.COLUMN_NAME IS NOT NULL, 'YES', 'NO')        AS IS_UNIQUE /*UNIQUE 여부*/
         , IIF(IS_IDENTITY.COLUMN_NAME IS NOT NULL, 'YES', 'NO')          AS IS_AUTO_INCREMENT /*AUTO_INCREMENT 여부*/
         , NULL                                                           AS COLUMN_EXTRA /*컬럼 EXTRA*/
         , IIF(KEY_COLS.COLUMN_NAME IS NULL, 'NO', 'YES')                 AS PK /*주 키 여부*/
         , SYS_EXTPROP.VALUE                                              AS COLUMN_COMMENT /*컬럼 코멘트 (설명)*/
         , IIF(TBL_SPEC.ORDINAL_POSITION = 1, TBL_SPEC.create_date, NULL) AS CREATE_TIME
         , IIF(TBL_SPEC.ORDINAL_POSITION = 1, TBL_SPEC.modify_date, NULL) AS UPDATE_TIME
         , IIF(TBL_SPEC.ORDINAL_POSITION = 1, TBL_ROW.row_count, NULL)    AS ROW
    -- SELECT COUNT(1)
    FROM TBL_SPEC
             LEFT JOIN SYS_EXTPROP
                       ON TBL_SPEC.object_id = SYS_EXTPROP.major_id
                           AND TBL_SPEC.column_id = SYS_EXTPROP.minor_id
             LEFT JOIN TBL_COMMENT
                       ON TBL_SPEC.object_id = TBL_COMMENT.TABLE_ID
             LEFT JOIN KEY_COLS
                       ON TBL_SPEC.TABLE_NAME = KEY_COLS.TABLE_NAME
                           AND TBL_SPEC.COLUMN_NAME = KEY_COLS.COLUMN_NAME
             LEFT JOIN TBL_ROW
                       ON TBL_SPEC.object_id = TBL_ROW.object_id
             LEFT JOIN UQ_CONSTRAINT
                       ON TBL_SPEC.TABLE_NAME = UQ_CONSTRAINT.TABLE_NAME
                           AND TBL_SPEC.COLUMN_NAME = UQ_CONSTRAINT.COLUMN_NAME
             LEFT JOIN IS_IDENTITY
                       ON TBL_SPEC.TABLE_NAME = IS_IDENTITY.TABLE_NAME
                           AND TBL_SPEC.COLUMN_NAME = IS_IDENTITY.COLUMN_NAME
    WHERE TBL_SPEC.TABLE_NAME IN ({tables})
    ORDER BY TBL_SPEC.TABLE_NAME, TBL_SPEC.ORDINAL_POSITION;
    """


def load_settings(settings_path="settings.yml"):
    with open(settings_path) as f:
        return yaml.safe_load(f)


def get_db_credentials(vault_config, mssql_config):
    client = hvac.Client(url=vault_config["url"])
    client.auth.userpass.login(username=vault_config["username"], password=vault_config["password"])
    secret = client.read(mssql_config["secret_path"])["data"]["data"]
    return secret


def _handle_geometry(geometry_value):
    return geometry_value


def generate_specification(settings):
    tables = settings.get("tables", [])
    if not tables:
        print("No tables specified in settings.")
        return

    vault_config = settings["vault"]
    mssql_config = settings["mssql"]
    output_config = mssql_config["output"]

    try:
        db_secret = get_db_credentials(vault_config, mssql_config)
    except Exception as e:
        print(f"Failed to retrieve credentials from Vault: {e}")
        return

    # Override host/port from settings if needed, or use what's in Vault
    db_host = mssql_config.get("override_host", db_secret.get("host", "localhost"))
    db_port = mssql_config.get("override_port", db_secret.get("port", 1433))
    db_user = db_secret["user"]
    db_password = db_secret["password"]

    dfs = list()
    databases: dict[str, list] = dict()
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
        table_list = [f"'{t}'" for t in table_names]
        table_str = ",".join(table_list)

        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={db_host},{db_port};"
            f"DATABASE={database};"
            f"UID={db_user};"
            f"PWD={db_password};"
            "Encrypt=no;"
            "Connect Timeout=3600;"
        )

        try:
            connection = pyodbc.connect(connection_string)
            connection.add_output_converter(-150, _handle_geometry)
            cursor = connection.cursor()

            query = MSSQL_QUERY_TEMPLATE.format(tables=table_str)
            cursor.execute(query)

            rows = cursor.fetchall()
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                dfs.append(pd.DataFrame([list(row) for row in rows], columns=columns))

            cursor.close()
            connection.close()
        except Exception as e:
            print(f"Error processing database {database}: {e}")

    if dfs:
        table_specification = pd.concat(dfs)
        output_filename = output_config.get("filename", "mssql-tables.xlsx")
        table_specification.to_excel(output_filename, engine="xlsxwriter")
        print(f"Specification saved to {output_filename}")

        # PK Check Logic
        table_info = dict()
        for _, row in table_specification.iterrows():
            schema = row["DB_NAME"]
            table = row["TABLE_NAME"]
            column = row["COLUMN_NAME"]
            k = f"{schema}.{table}"

            if k not in table_info:
                table_info[k] = 0

            if row["PK"] == "YES":
                table_info[k] += 1
                print(f"{schema}.{table}.{column}")  # Optional: print PK columns

        print("-------------------------")
        print("Tables without Primary Key:")
        for k, v in table_info.items():
            if v == 0:
                print(k)
    else:
        print("No data retrieved.")


if __name__ == "__main__":
    # Assuming settings.yml is in the same directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        generate_specification(settings)
    else:
        print(f"Settings file not found at {settings_path}")
