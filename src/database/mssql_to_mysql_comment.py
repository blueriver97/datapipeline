import os

import pandas as pd
import yaml


def load_settings(settings_path="settings.yml"):
    with open(settings_path) as f:
        return yaml.safe_load(f)


def load_and_clean_excel(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None
    df = pd.read_excel(file_path)
    # Replace NaN with None for easier handling
    return df.where(pd.notna(df), None)


def get_default_value(default_value):
    if default_value is None:
        return ""

    default_str = str(default_value)
    if default_str.isdigit():
        return f"DEFAULT {default_str}"
    if "now" in default_str.lower() or "current_timestamp" in default_str.lower():
        return f"DEFAULT {default_str}"

    # If it's a string literal, wrap in quotes
    return f"DEFAULT '{default_str}'"


def generate_alter_queries_by_sequence(mssql_group, mysql_group):
    """
    Generates ALTER queries assuming columns are in the same sequence.
    """
    queries = []

    # Ensure both groups are sorted by ordinal position
    if "ORDINAL_POSITION" in mssql_group.columns:
        mssql_group = mssql_group.sort_values("ORDINAL_POSITION")
    if "ORDINAL_POSITION" in mysql_group.columns:
        mysql_group = mysql_group.sort_values("ORDINAL_POSITION")

    for i, ((_, src), (_, dest)) in enumerate(zip(mssql_group.iterrows(), mysql_group.iterrows()), start=1):
        queries.extend(_create_comment_query(i, src, dest))

    return queries


def generate_alter_queries_by_name(mssql_group, mysql_group):
    """
    Generates ALTER queries by matching column names.
    """
    queries = []

    # Create a map for mysql columns for easier lookup
    # Normalize column names to lower case for comparison if needed, but let's stick to exact match first
    mysql_cols = {row.COLUMN_NAME: row for _, row in mysql_group.iterrows()}

    # Sort mssql group to process in order (optional but good for consistent output)
    if "ORDINAL_POSITION" in mssql_group.columns:
        mssql_group = mssql_group.sort_values("ORDINAL_POSITION")

    for i, (_, src) in enumerate(mssql_group.iterrows(), start=1):
        col_name = src.COLUMN_NAME
        if col_name in mysql_cols:
            dest = mysql_cols[col_name]
            queries.extend(_create_comment_query(i, src, dest))
        else:
            # Try case-insensitive match?
            # For now, strict match.
            pass

    return queries


def _create_comment_query(index, src, dest):
    queries = []
    column_comment = src.COLUMN_COMMENT
    table_comment = src.TABLE_COMMENT

    is_null = "NULL" if dest.IS_NULLABLE == "YES" else "NOT NULL"
    is_unique = "UNIQUE" if dest.IS_UNIQUE == "YES" else ""
    is_ai = "AUTO_INCREMENT" if dest.IS_AUTO_INCREMENT == "YES" else ""
    default_val = get_default_value(dest.COLUMN_DEFAULT)

    charset = ""
    if dest.CHARACTER_SET:
        charset = f"CHARACTER SET {dest.CHARACTER_SET}"

    collate = ""
    if dest.COLLATION_NAME:
        collate = f"COLLATE {dest.COLLATION_NAME}"

    # Table comment (only once per table)
    if index == 1 and table_comment:
        queries.append(f"ALTER TABLE {dest.DB_NAME}.{dest.TABLE_NAME} COMMENT = '{table_comment}';")

    # Column comment
    if column_comment:
        # MySQL MODIFY COLUMN syntax:
        # MODIFY [COLUMN] col_name column_definition [FIRST | AFTER col_name]
        # column_definition: data_type [NOT NULL | NULL] [DEFAULT default_value] [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY] [COMMENT 'string']
        # The order of attributes matters.
        # Standard order: data_type [charset] [collate] [NOT NULL | NULL] [DEFAULT] [AUTO_INCREMENT] [UNIQUE] [COMMENT]

        stmt = f"ALTER TABLE {dest.DB_NAME}.{dest.TABLE_NAME} MODIFY COLUMN {dest.COLUMN_NAME} {dest.COLUMN_TYPE} "

        if charset:
            stmt += f"{charset} "
        if collate:
            stmt += f"{collate} "

        if is_null:
            stmt += f"{is_null} "

        if default_val:
            stmt += f"{default_val} "

        if is_ai:
            stmt += f"{is_ai} "

        if is_unique:
            stmt += f"{is_unique} "

        stmt += f"COMMENT '{column_comment}';"

        queries.append(stmt)

    return queries


def save_queries_to_file(output_dir, db_name, table_name, queries):
    if not queries:
        return

    filename = f"{db_name}_{table_name}.sql"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(queries))

    print(f"Generated {file_path}")


def generate_comments(settings):
    comment_config = settings.get("comment", {})
    export_config = comment_config.get("export", {})
    mysql_config = export_config.get("mysql", {})

    mssql_file = comment_config.get("mssql_file", "mssql-tables.xlsx")
    mysql_file = comment_config.get("mysql_file", "mysql-tables.xlsx")

    output_dir = mysql_config.get("output_dir", "comment_scripts")
    match_by = mysql_config.get("match_by", "name")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Loading MSSQL spec from {mssql_file}...")
    mssql_df = load_and_clean_excel(mssql_file)
    print(f"Loading MySQL spec from {mysql_file}...")
    mysql_df = load_and_clean_excel(mysql_file)

    if mssql_df is None or mysql_df is None:
        print("Could not load input files.")
        return

    grouped_mssql = mssql_df.groupby(["DB_NAME", "TABLE_NAME"])
    grouped_mysql = mysql_df.groupby(["DB_NAME", "TABLE_NAME"])

    for key in grouped_mssql.groups:
        if key in grouped_mysql.groups:
            mssql_group = grouped_mssql.get_group(key)
            mysql_group = grouped_mysql.get_group(key)

            if match_by == "sequence":
                queries = generate_alter_queries_by_sequence(mssql_group, mysql_group)
            else:
                queries = generate_alter_queries_by_name(mssql_group, mysql_group)

            if queries:
                save_queries_to_file(output_dir, key[0], key[1], queries)


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        # You can change this to 'sequence' if you prefer the old behavior
        generate_comments(settings)
    else:
        print(f"Settings file not found at {settings_path}")
