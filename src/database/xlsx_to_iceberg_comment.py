import glob
import os

import pandas as pd
import yaml
from pyspark.sql import SparkSession


def load_settings(settings_path="settings.yml"):
    with open(settings_path) as f:
        return yaml.safe_load(f)


def load_and_clean_excel(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None
    df = pd.read_excel(file_path)
    return df.where(pd.notna(df), None)


def generate_alter_spark_queries(sql_df):
    queries = []
    # Use a set to track if table comment has been added to avoid duplicates if iterating rows
    # But the logic below adds it for every row if table_comment exists?
    # The original script:
    # if table_comment: queries.append(...)
    # This adds the table comment ALTER statement for EVERY column row if table comment is present.
    # This seems redundant but harmless (idempotent).
    # However, let's optimize to add it only once.

    table_comment_added = False

    for _, src in sql_df.iterrows():
        column_comment = src.COLUMN_COMMENT or ""
        table_comment = src.TABLE_COMMENT or ""

        # Clean comments
        column_comment = str(column_comment).replace("'", "")
        table_comment = str(table_comment).replace("'", "")

        if not table_comment_added and table_comment:
            # Assuming DB_NAME and TABLE_NAME are consistent in the group
            db_name = src.DB_NAME.lower()
            table_name = src.TABLE_NAME.lower()
            queries.append(
                f"ALTER TABLE {db_name}_bronze.{table_name} SET TBLPROPERTIES ('comment' = '{table_comment.strip()}');"
            )
            table_comment_added = True

        # Column comment
        # Note: Iceberg supports updating column comments via ALTER TABLE ... ALTER COLUMN ... COMMENT ...
        if column_comment:
            db_name = src.DB_NAME.lower()
            table_name = src.TABLE_NAME.lower()
            col_name = src.COLUMN_NAME
            queries.append(
                f"ALTER TABLE {db_name}_bronze.{table_name} ALTER COLUMN {col_name} COMMENT '{column_comment.strip()}';"
            )

    return queries


def save_queries_to_file(output_dir, db_name, table_name, queries):
    if not queries:
        return

    filename = f"{db_name.lower()}_bronze.{table_name.lower()}.sql"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(queries))


def generate_iceberg_comments(settings):
    mysql_config = settings.get("mysql", {})
    mysql_file = mysql_config.get("output", {}).get("specification_filename", "mysql-tables.xlsx")

    output_dir = "iceberg_comment_scripts"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Loading MySQL spec from {mysql_file}...")
    sql_df = load_and_clean_excel(mysql_file)

    if sql_df is None:
        return

    grouped_sql_df = sql_df.groupby(["DB_NAME", "TABLE_NAME"])

    for key in grouped_sql_df.groups:
        mysql_group = grouped_sql_df.get_group(key)
        spark_queries = generate_alter_spark_queries(mysql_group)

        if spark_queries:
            save_queries_to_file(output_dir, key[0], key[1], spark_queries)

    return output_dir


def apply_comments_to_iceberg(settings, script_dir):
    # Spark Session Configuration
    # This assumes the environment is set up correctly for Spark execution
    # Adjust catalog name and warehouse path as needed or move to settings
    comment_config = settings.get("comment", {})
    iceberg_config = comment_config.get("iceberg", {})

    if not iceberg_config:
        print("No Iceberg configuration found in settings.")
        return

    AWS_PROFILE = iceberg_config.get("profile", "default")
    CATALOG = iceberg_config.get("catalog", "glue_catalog")
    ICEBERG_S3_ROOT_PATH = iceberg_config.get("warehouse", "s3a://blueriver-datalake/iceberg")
    # Check if AWS_PROFILE is needed, maybe from env or settings
    try:
        spark = (
            SparkSession.builder.appName("Apply Iceberg Comments")
            .config("spark.sql.defaultCatalog", CATALOG)
            .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_S3_ROOT_PATH)
            .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", True)
            .config("spark.yarn.appMasterEnv.AWS_PROFILE", AWS_PROFILE)
            .config("spark.executorEnv.AWS_PROFILE", AWS_PROFILE)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
            )
            .config("spark.sql.caseSensitive", True)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )
    except Exception as e:
        print(f"Failed to create Spark session: {e}")
        return

    files = sorted(glob.glob(os.path.join(script_dir, "*.sql")))
    print(f"Found {len(files)} SQL files to apply.")

    for file in files:
        try:
            with open(file, encoding="utf-8") as f:
                sql_lines = f.readlines()

            # Extract table name from filename to check existence
            # Filename format: db_bronze.table.sql
            filename = os.path.basename(file)
            schema_table = filename.rsplit(".", 1)[0]  # remove .sql

            if not spark.catalog.tableExists(schema_table):
                print(f"Table not found: {schema_table} ({file})")
                continue

            print(f"Applying comments to {schema_table}...")
            for stmt in sql_lines:
                stmt = stmt.strip()
                if stmt:
                    spark.sql(stmt)

            print(f"Comments applied to {file}")

        except Exception as e:
            print(f"Error processing {file}: {e}")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)

        # Step 1: Generate SQL scripts
        script_dir = generate_iceberg_comments(settings)

        # Step 2: Apply SQL scripts (Optional, can be commented out if only generation is needed)
        # Note: Running Spark locally might require significant setup (jars, aws creds, etc.)
        # Uncomment the line below to run if environment is ready.
        # apply_comments_to_iceberg(settings, script_dir)

        print(f"SQL scripts generated in {script_dir}. Run apply_comments_to_iceberg() to execute.")
    else:
        print(f"Settings file not found at {settings_path}")
