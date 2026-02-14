import os
import re
from collections import defaultdict

import pandas as pd
import yaml
from tqdm import tqdm

# MySQL Reserved Keyword and Functions
MYSQL_RESERVED_WORD = (
    "ACCESSIBLE,ADD,ALL,ALTER,ANALYZE,AND,AS,ASC,ASENSITIVE,BEFORE,BETWEEN,BIGINT,BINARY,"
    "BLOB,BOTH,BY,CALL,CASCADE,CASE,CHANGE,CHAR,CHARACTER,CHECK,COLLATE,COLUMN,CONDITION,"
    "CONSTRAINT,CONTINUE,CONVERT,CREATE,CROSS,CUBE,CUME_DIST,CURRENT_DATE,CURRENT_TIME,"
    "CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND,"
    "DAY_MINUTE,DAY_SECOND,DEC,DECIMAL,DECLARE,DEFAULT,DELAYED,DELETE,DENSE_RANK,DESC,"
    "DESCRIBE,DETERMINISTIC,DISTINCT,DISTINCTROW,DIV,DOUBLE,DROP,DUAL,EACH,ELSE,ELSEIF,"
    "EMPTY,ENCLOSED,ESCAPED,EXCEPT,EXISTS,EXIT,EXPLAIN,FALSE,FETCH,FIRST_VALUE,FLOAT,"
    "FLOAT4,FLOAT8,FOR,FORCE,FOREIGN,FROM,FULLTEXT,FUNCTION,GENERATED,GET,GRANT,GROUP,"
    "GROUPING,GROUPS,HAVING,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,"
    "IGNORE,IN,INDEX,INFILE,INNER,INOUT,INSENSITIVE,INSERT,INT,INT1,INT2,INT3,INT4,INT8,"
    "INTEGER,INTERSECT,INTERVAL,INTO,IO_AFTER_GTIDS,IO_BEFORE_GTIDS,IS,ITERATE,JOIN,"
    "JSON_TABLE,KEY,KEYS,KILL,LAG,LAST_VALUE,LATERAL,LEAD,LEADING,LEAVE,LEFT,LIKE,LIMIT,"
    "LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,"
    "LOW_PRIORITY,MASTER_BIND,MASTER_SSL_VERIFY_SERVER_CERT,MATCH,MAXVALUE,MEDIUMBLOB,"
    "MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODIFIES,NATURAL,"
    "NOT,NO_WRITE_TO_BINLOG,NTH_VALUE,NTILE,NULL,NUMERIC,OF,ON,OPTIMIZE,OPTIMIZER_COSTS,"
    "OPTION,OPTIONALLY,OR,ORDER,OUT,OUTER,OUTFILE,OVER,PARTITION,PERCENT_RANK,PRECISION,"
    "PRIMARY,PROCEDURE,PURGE,RANGE,RANK,READ,READS,READ_WRITE,REAL,RECURSIVE,REFERENCES,"
    "REGEXP,RELEASE,RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RETURN,REVOKE,RIGHT,"
    "RLIKE,ROW,ROWS,ROW_NUMBER,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SELECT,SENSITIVE,SEPARATOR,"
    "SET,SHOW,SIGNAL,SMALLINT,SPATIAL,SPECIFIC,SQL,SQLEXCEPTION,SQLSTATE,SQLWARNING,"
    "SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SSL,STARTING,STORED,STRAIGHT_JOIN,"
    "SYSTEM,TABLE,TERMINATED,THEN,TINYBLOB,TINYINT,TINYTEXT,TO,TRAILING,TRIGGER,TRUE,UNDO,"
    "UNION,UNIQUE,UNLOCK,UNSIGNED,UPDATE,USAGE,USE,USING,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,"
    "VALUES,VARBINARY,VARCHAR,VARCHARACTER,VARYING,VIRTUAL,WHEN,WHERE,WHILE,WINDOW,WITH,"
    "WRITE,XOR,YEAR_MONTH,ZEROFILL"
).split(",")
BACKTIC_PTN = re.compile(r"[가-힣()\[\]#~ ]+")


def load_settings(settings_path="settings.yml"):
    with open(settings_path) as f:
        return yaml.safe_load(f)


def get_parse_name(name):
    name = str(name).strip()
    if BACKTIC_PTN.findall(name) or name.isdigit() or name.upper() in MYSQL_RESERVED_WORD:
        return f"`{name}`"
    else:
        return name


def convert_type_mssql_to_mysql(col_type, col_length):
    col_type = str(col_type).lower()
    col_length = str(col_length) if col_length is not None else ""

    change = False
    CONVERT_TYPE = {
        "money": "DECIMAL(19,4)",
        "smalldatetime": "DATETIME",
        "datetime2": "DATETIME",
        "bit": "TINYINT(1)",
        "nvarchar": "VARCHAR",
        "ntext": "TEXT",
        "uniqueidentifier": "VARCHAR(64)",
        "nchar": "CHAR",
        "hierarchyid": "VARCHAR(100)",
        "tinyint": "TINYINT UNSIGNED",
        "xml": "LONGTEXT",
        "numeric": "NUMERIC",
        "decimal": "DECIMAL",
    }

    if not col_length:
        col_length = ""
        change = True
    elif "char" in col_type and col_length == "-1":
        col_type = "text"
        col_length = ""
        change = True
    elif "binary" in col_type and col_length == "-1":
        col_type = "BLOB"
        col_length = ""
        change = True
    elif "hierarchyid" == col_type:
        col_length = ""
        change = True
    elif col_length.isdigit():
        if int(col_length) >= 1000:
            col_type = "text"
            col_length = ""
            change = True
        else:
            col_length = f"({col_length})"
    elif col_type in ["numeric", "decimal"] and "," in col_length:
        col_length = f"({col_length})"
    else:
        col_length = ""

    final_type = CONVERT_TYPE.get(col_type, col_type.upper())
    return change, f"{final_type}{col_length}".strip()


def get_column_null(is_null):
    return "NOT NULL" if is_null == "NO" else "NULL"


def get_column_unique(is_unique):
    return "UNIQUE" if is_unique == "YES" else ""


def get_column_autoincrement(is_auto):
    return "AUTO_INCREMENT" if is_auto == "YES" else ""


def get_column_pk(is_pk):
    return True if is_pk == "YES" else False


def write_file(path, contents):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(contents)


def generate_ddl(settings):
    ddl_config = settings.get("convert_ddl", {})
    input_file = ddl_config.get("input_file", "mssql-tables.xlsx")
    output_dir = ddl_config.get("output_dir", "convert_ddl")

    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}")
        return

    print(f"Reading from {input_file}...")
    try:
        sheet = pd.read_excel(input_file, dtype=str, na_filter=False)
    except Exception as e:
        print(f"Error reading excel file: {e}")
        return

    table_col_def = defaultdict(list)
    change_col = defaultdict(list)
    table_pk_col = defaultdict(list)
    table_comments = dict()

    print("Processing columns...")
    for i, column in tqdm(sheet.iterrows(), total=sheet.shape[0]):
        db_name = column["DB_NAME"]
        table_name = get_parse_name(column["TABLE_NAME"])
        column_name = get_parse_name(column["COLUMN_NAME"])

        changed, column_type = convert_type_mssql_to_mysql(column["DATA_TYPE"], column["COLUMN_LENGTH"])
        column_null = get_column_null(column["IS_NULLABLE"])
        column_unique = get_column_unique(column["IS_UNIQUE"])
        column_autoincrement = get_column_autoincrement(column["IS_AUTO_INCREMENT"])

        key = f"{db_name}.{table_name}"

        # Table comment from the first column
        if str(column["ORDINAL_POSITION"]) == "1":
            table_comments[key] = column["TABLE_COMMENT"] if column["TABLE_COMMENT"] else ""

        # PK Column
        if get_column_pk(column["PK"]):
            table_pk_col[key].append(column_name)

        # Column comment
        column_comment = column["COLUMN_COMMENT"] if column["COLUMN_COMMENT"] else ""

        # Construct column definition
        col_def_parts = [column_name, column_type, column_null]
        if column_unique:
            col_def_parts.append(column_unique)
        if column_autoincrement:
            col_def_parts.append(column_autoincrement)

        col_def_parts.append(f"COMMENT '{column_comment}'")

        table_col_def[key].append(" ".join(col_def_parts) + ",")

        if changed:
            change_col[key].append(f"{column_name}\t{column['DATA_TYPE']}({column['COLUMN_LENGTH']})\t{column_type}")

    print(f"Generating DDL files in {output_dir}...")
    for key, columns in tqdm(table_col_def.items()):
        schema, table = key.split(".")
        # Remove backticks for filename if present, though usually schema/table in key might not have them if not added
        # But get_parse_name adds them. Let's strip them for filename.
        clean_table = table.replace("`", "")

        columns_stmt = "\n\t".join(columns)
        pk_stmt = ""

        if table_pk_col[key]:
            pk_list = ",".join(table_pk_col[key])
            pk_stmt = f",\n\tPRIMARY KEY ({pk_list})"

        # Remove last comma from last column definition if no PK
        if not pk_stmt:
            columns_stmt = columns_stmt.rstrip(",")

        comment_value = table_comments.get(key, "")

        ddl = f"CREATE TABLE IF NOT EXISTS {key}\n(\n\t{columns_stmt}{pk_stmt}\n) COMMENT = '{comment_value}';\n"

        output_path = os.path.join(output_dir, schema, f"{clean_table}.sql")
        write_file(output_path, ddl)

        if key in change_col:
            for col in change_col[key]:
                print(f"Type Changed: {schema}\t{table}\t{col}")

    print("DDL generation complete.")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        generate_ddl(settings)
    else:
        print(f"Settings file not found at {settings_path}")
