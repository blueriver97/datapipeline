import os

import hvac
import pandas as pd
import pyodbc
import yaml

# SQL Query Template
SPACEUSED_TABLE_DDL = """
IF OBJECT_ID('tempdb..#SpaceUsed') IS NOT NULL DROP TABLE #SpaceUsed;
CREATE TABLE #SpaceUsed
(
    name       NVARCHAR(128),
    rows       CHAR(20),
    reserved   VARCHAR(18),
    data       VARCHAR(18),
    index_size VARCHAR(18),
    unused     VARCHAR(18),
    db_name    NVARCHAR(128)
);
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


def generate_spaceused(settings):
    tables = settings.get("tables", [])
    if not tables:
        print("No tables specified in settings.")
        return

    vault_config = settings["vault"]
    mssql_config = settings["mssql"]
    output_config = mssql_config.get("output", {})

    try:
        db_secret = get_db_credentials(vault_config, mssql_config)
    except Exception as e:
        print(f"Failed to retrieve credentials from Vault: {e}")
        return

    db_host = mssql_config.get("override_host", db_secret.get("host", "localhost"))
    db_port = mssql_config.get("override_port", db_secret.get("port", 1433))
    db_user = db_secret["user"]
    db_password = db_secret["password"]

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

    # Construct the full SQL script
    # We will run one big script that creates the temp table,
    # then for each database, USE it, insert spaceused info, and update the db_name column.
    full_sql_script = SPACEUSED_TABLE_DDL

    for database, table_names in databases.items():
        # Switch context to the specific database
        full_sql_script += f"\nUSE [{database}];\n"

        for table in table_names:
            # Insert spaceused for each table
            # sp_spaceused returns a result set, so we use INSERT ... EXEC
            full_sql_script += (
                f"INSERT INTO #SpaceUsed(name, rows, reserved, data, index_size, unused) EXEC sp_spaceused '{table}';\n"
            )

        # Mark the rows we just inserted with the current database name
        # We update rows where db_name is NULL (which are the ones just inserted)
        full_sql_script += f"UPDATE #SpaceUsed SET db_name = '{database}' WHERE db_name IS NULL;\n"

    # Finally select all data
    full_sql_script += "\nSELECT * FROM #SpaceUsed;"

    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={db_host},{db_port};"
        "DATABASE=master;"  # Connect to master initially
        f"UID={db_user};"
        f"PWD={db_password};"
        "Encrypt=no;"
        "Connect Timeout=3600;"
    )

    try:
        connection = pyodbc.connect(connection_string)
        connection.add_output_converter(-150, _handle_geometry)
        cursor = connection.cursor()

        # Execute the script
        # Note: pyodbc might not handle multiple batches in one go perfectly if they return multiple result sets.
        # But here we are only interested in the final SELECT.
        # However, INSERT EXEC might produce row counts.
        # Let's try executing. If it fails due to multiple batches, we might need to split.
        # But typically for a script like this, we can iterate through result sets to find the one we want.

        cursor.execute(full_sql_script)

        rows = None
        columns = None

        # Iterate to find the result set from the final SELECT
        while True:
            try:
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    # We found a result set. Is it the one we want?
                    # The temp table has columns: name, rows, reserved, data, index_size, unused, db_name
                    # Let's check if columns match roughly what we expect or just take the last one.
                    # In this script, the only SELECT that returns data is the last one.
                    # sp_spaceused returns data but we are capturing it with INSERT INTO ... EXEC, so it shouldn't return to client.
                    break
            except Exception:
                pass

            if not cursor.nextset():
                break

        if rows is not None and columns is not None:
            table_spaceused = pd.DataFrame([list(row) for row in rows], columns=columns)

            # Clean up data (remove ' KB' and convert to int)
            for col in ["reserved", "data", "index_size", "unused"]:
                if col in table_spaceused.columns:
                    table_spaceused[col] = table_spaceused[col].apply(
                        lambda x: int(str(x).replace("KB", "").strip()) if x else 0
                    )

            output_filename = output_config.get("spaceused_filename", "mssql-spaceused.xlsx")  # Default name
            # If there is a specific config for spaceused output, we could use it.
            # But settings.yml only has 'output' under mssql which is used for table spec.
            # Let's just use a default or derive from settings if needed.
            # For now, hardcoded as in the original notebook or similar.

            table_spaceused.to_excel(output_filename, index=False)
            print(f"Space used information saved to {output_filename}")
        else:
            print("No data returned from the query.")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error executing SQL Server spaceused script: {e}")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        generate_spaceused(settings)
    else:
        print(f"Settings file not found at {settings_path}")
