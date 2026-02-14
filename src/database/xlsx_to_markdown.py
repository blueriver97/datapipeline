import glob
import os

import pandas as pd
import yaml


def load_settings(settings_path="settings.yml"):
    with open(settings_path) as f:
        return yaml.safe_load(f)


def find_files(dir_path: str, file_ptn: str) -> list:
    """지정된 경로와 패턴에 맞는 모든 파일을 찾습니다."""
    return [f for f in glob.glob(os.path.join(dir_path, file_ptn), recursive=True) if os.path.isfile(f)]


def process_dataframe(df: pd.DataFrame, output_dir: str) -> None:
    """데이터프레임을 처리하여 마크다운 파일로 저장합니다."""
    # Group by DB_NAME and TABLE_NAME
    grouped_df = df.groupby(["DB_NAME", "TABLE_NAME"])

    # We will write to one markdown file per DB_NAME
    # To avoid keeping too many file descriptors open, we can collect content in memory or append to files.
    # Let's collect content in a dictionary: db_name -> list of markdown strings
    db_content: dict[str, list[str]] = {}

    for (db_name, table_name), table_df in grouped_df:
        # Ensure db_name is a string for dictionary key
        db_name_str = str(db_name)

        # Get table level info from the first row
        first_row = table_df.iloc[0]
        table_comment = str(first_row.get("TABLE_COMMENT", "") or "")

        # Format the header
        # ### [Schema] TableName / TableComment
        title_comment = f" / {table_comment}" if table_comment else ""
        header = f"### {db_name}.{table_name}{title_comment}\n\n"

        if table_comment:
            header += f"{table_comment}\n\n"

        # Select and rename columns for the markdown table
        # Expected columns in input: ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, PK, COLUMN_COMMENT
        # Output columns: SEQ, NAME, TYPE, PK, COMMENT

        # Ensure columns exist
        cols_to_use = ["ORDINAL_POSITION", "COLUMN_NAME", "DATA_TYPE", "PK", "COLUMN_COMMENT"]
        # Filter only existing columns
        cols_to_use = [c for c in cols_to_use if c in table_df.columns]

        markdown_df = table_df[cols_to_use].copy()

        # Replace PK values: YES -> V, NO -> empty
        if "PK" in markdown_df.columns:
            markdown_df["PK"] = markdown_df["PK"].replace({"NO": "", "YES": "V"})

        # Rename columns
        rename_map = {
            "ORDINAL_POSITION": "SEQ",
            "COLUMN_NAME": "NAME",
            "DATA_TYPE": "TYPE",
            "COLUMN_COMMENT": "COMMENT",
        }
        markdown_df = markdown_df.rename(columns=rename_map)

        # Convert to markdown table
        # index=False to hide the pandas index
        md_table = markdown_df.to_markdown(index=False)

        full_content = header + md_table + "\n\n"

        if db_name_str not in db_content:
            db_content[db_name_str] = []
        db_content[db_name_str].append(full_content)

    # Write to files
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for db_name, contents in db_content.items():
        filename = f"{db_name}.md"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"# Database Specification: {db_name}\n\n")
            f.write("".join(contents))
        print(f"Generated {filepath}")


def generate_markdown(settings):
    # We can process both MSSQL and MySQL specs if available
    mssql_config = settings.get("mssql", {})
    mysql_config = settings.get("mysql", {})

    mssql_file = mssql_config.get("output", {}).get("specification_filename", "mssql-tables.xlsx")
    mysql_file = mysql_config.get("output", {}).get("specification_filename", "mysql-tables.xlsx")

    output_dir = "markdown_specs"

    files_to_process = []
    if os.path.exists(mssql_file):
        files_to_process.append(mssql_file)
    if os.path.exists(mysql_file):
        files_to_process.append(mysql_file)

    if not files_to_process:
        print("No specification files found to process.")
        return

    for file in files_to_process:
        print(f"Processing {file}...")
        try:
            df = pd.read_excel(file).fillna("")
            process_dataframe(df, output_dir)
        except Exception as e:
            print(f"Error processing {file}: {e}")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        generate_markdown(settings)
    else:
        print(f"Settings file not found at {settings_path}")
