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


def generate_alter_queries(mssql_group, mysql_group):
    queries = []

    # Ensure both groups are sorted by ordinal position to match columns correctly
    # Assuming the excel files are already sorted or have ORDINAL_POSITION column
    if "ORDINAL_POSITION" in mssql_group.columns:
        mssql_group = mssql_group.sort_values("ORDINAL_POSITION")
    if "ORDINAL_POSITION" in mysql_group.columns:
        mysql_group = mysql_group.sort_values("ORDINAL_POSITION")

    # Iterate through rows. We assume 1-to-1 mapping by order or name.
    # Using zip might be risky if column counts differ or order differs.
    # Let's try to match by column name if possible, or fall back to zip.

    # Create a map for mysql columns for easier lookup
    mysql_cols = {row.COLUMN_NAME: row for _, row in mysql_group.iterrows()}  # noqa: F841

    first_row = True  # noqa: F841
    for _, mssql_row in mssql_group.iterrows():
        col_name = mssql_row.COLUMN_NAME  # noqa: F841

        # Find corresponding MySQL column
        # Note: Column names might have changed case or slight differences?
        # The original script used zip, implying strict order match.
        # Let's stick to name matching if possible, or order if names differ significantly?
        # The original script: zip(mssql.iterrows(), mysql.iterrows())
        # This implies the rows are perfectly aligned. Let's try to respect that logic but be safer if possible.
        # If we use zip, we must ensure both dataframes have same number of rows and order.

        # Let's use the original logic of zipping for now, assuming the generation process
        # created aligned excel files.
        pass

    # Re-implementing the loop using zip as per original script logic
    # But we need to handle cases where lengths differ?
    # The original script assumes perfect alignment.

    for i, ((_, src), (_, dest)) in enumerate(zip(mssql_group.iterrows(), mysql_group.iterrows()), start=1):
        # src is mssql, dest is mysql

        column_comment = src.COLUMN_COMMENT
        table_comment = src.TABLE_COMMENT

        # If no comments to add, skip?
        # The original script skips if BOTH are empty.
        # But we might want to apply column comment even if table comment is empty.

        # Construct parts of the ALTER TABLE MODIFY COLUMN statement
        # We need to preserve existing attributes of the MySQL column

        is_null = "NULL" if dest.IS_NULLABLE == "YES" else "NOT NULL"

        # Unique constraint is usually handled by index, but can be part of column def in some contexts
        # MODIFY COLUMN syntax in MySQL:
        # MODIFY [COLUMN] col_name column_definition [FIRST | AFTER col_name]

        # column_definition: data_type [NOT NULL | NULL] [DEFAULT default_value] [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY] [COMMENT 'string']

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
        if i == 1 and table_comment:
            queries.append(f"ALTER TABLE {dest.DB_NAME}.{dest.TABLE_NAME} COMMENT = '{table_comment}';")

        # Column comment
        if column_comment:
            # We need to reconstruct the full column definition to modify it just to add a comment
            # This is risky if we miss some attribute (like charset, collation, etc.)
            # The original script attempts to reconstruct it.

            # dest.COLUMN_TYPE usually contains "varchar(100)" or "int(11)" etc.

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

    # Create directory structure: output_dir/db_name/table_name.sql or just output_dir/db_name_table_name.sql?
    # Original script: comment/db_name_table_name.sql

    filename = f"{db_name}_{table_name}.sql"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(queries))

    print(f"Generated {file_path}")


def generate_comments(settings):
    # Paths from settings or defaults
    mssql_config = settings.get("mssql", {})
    mysql_config = settings.get("mysql", {})

    mssql_file = mssql_config.get("output", {}).get("specification_filename", "mssql-tables.xlsx")
    mysql_file = mysql_config.get("output", {}).get("specification_filename", "mysql-tables.xlsx")

    # Output directory for comment sqls
    # Not explicitly in settings, let's default to 'comment_scripts' or similar, or use a new setting
    output_dir = "comment_scripts"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Loading MSSQL spec from {mssql_file}...")
    mssql_df = load_and_clean_excel(mssql_file)
    print(f"Loading MySQL spec from {mysql_file}...")
    mysql_df = load_and_clean_excel(mysql_file)

    if mssql_df is None or mysql_df is None:
        print("Could not load input files.")
        return

    # Group by DB and Table
    # Note: The DB names might differ between MSSQL and MySQL (e.g. prod-sqlserver vs prod-datapipeline schemas)
    # But the original script groups by DB_NAME and TABLE_NAME.
    # If the migration preserved table names but changed DB names, this grouping might fail to match if DB_NAME is different.
    # However, the original script assumes they match or the excel files have aligned DB_NAMEs?
    # Actually, in the previous scripts, we saw that we query specific tables.
    # If the DB name in the excel file comes from the source DB, they might differ.
    # Let's assume the user handles this or the excel files have compatible names.
    # Or maybe we should group only by TABLE_NAME if DBs are different?
    # The original script uses: grouped_mssql_df.groups and checks if key in grouped_mysql_df.groups
    # key is (DB_NAME, TABLE_NAME).

    grouped_mssql = mssql_df.groupby(["DB_NAME", "TABLE_NAME"])
    grouped_mysql = mysql_df.groupby(["DB_NAME", "TABLE_NAME"])

    for key in grouped_mssql.groups:
        # key is (db_name, table_name)
        # If MySQL has a different DB name, this won't match.
        # But let's stick to the original logic.

        if key in grouped_mysql.groups:
            mssql_group = grouped_mssql.get_group(key)
            mysql_group = grouped_mysql.get_group(key)

            queries = generate_alter_queries(mssql_group, mysql_group)

            if queries:
                save_queries_to_file(output_dir, key[0], key[1], queries)
        else:
            # Try to find by table name only if exact match fails?
            # For now, just log it.
            # print(f"Table {key} found in MSSQL but not in MySQL spec.")
            pass


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        generate_comments(settings)
    else:
        print(f"Settings file not found at {settings_path}")
