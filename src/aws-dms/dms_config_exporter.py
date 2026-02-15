import glob
import json
import os
import shutil

import boto3
import pandas as pd
import yaml


def load_settings(settings_path="settings.yml"):
    with open(settings_path) as f:
        return yaml.safe_load(f)


def prepare_directory(output_dir: str) -> None:
    """Prepares the output directory by removing it if it exists and creating a new one."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)


def get_dms_client(aws_config):
    """Creates a boto3 DMS client using the specified profile and region."""
    session = boto3.Session(
        profile_name=aws_config.get("profile", "default"), region_name=aws_config.get("region", "ap-northeast-2")
    )
    return session.client("dms")


def fetch_replication_tasks(client):
    """Fetches all replication tasks from AWS DMS."""
    tasks = []
    paginator = client.get_paginator("describe_replication_tasks")
    for page in paginator.paginate():
        tasks.extend(page.get("ReplicationTasks", []))
    return tasks


def parse_tasks(tasks: list) -> pd.DataFrame:
    """Parses replication tasks into a DataFrame."""
    if not tasks:
        return pd.DataFrame()

    task_info = []
    for task in tasks:
        task_info.append(
            {
                "replication_task_identifier": task.get("ReplicationTaskIdentifier"),
                "replication_task_arn": task.get("ReplicationTaskArn"),
                "source_endpoint_arn": task.get("SourceEndpointArn"),
                "target_endpoint_arn": task.get("TargetEndpointArn"),
                "migration_type": task.get("MigrationType"),
                "table_mappings": task.get("TableMappings"),  # This is a JSON string
            }
        )
    return pd.DataFrame(task_info)


def find_files(directory: str, pattern: str) -> list:
    """Finds all files matching the pattern in the directory."""
    return [f for f in glob.glob(os.path.join(directory, pattern), recursive=True) if os.path.isfile(f)]


def export_dms_configs(settings):
    aws_config = settings.get("aws", {})
    dms_config = settings.get("dms", {})
    output_dir = dms_config.get("output_dir", "dms-config")

    print(f"Connecting to AWS DMS (Profile: {aws_config.get('profile')})...")
    try:
        client = get_dms_client(aws_config)
    except Exception as e:
        print(f"Failed to create DMS client: {e}")
        return

    print("Fetching replication tasks...")
    try:
        tasks = fetch_replication_tasks(client)
    except Exception as e:
        print(f"Error fetching tasks: {e}")
        return

    print(f"Found {len(tasks)} tasks.")
    task_df = parse_tasks(tasks)

    if task_df.empty:
        print("No tasks found.")
        return

    print(f"Exporting configurations to {output_dir}...")
    prepare_directory(output_dir)

    for _, row in task_df.iterrows():
        task_name = row["replication_task_identifier"]
        table_mappings_str = row["table_mappings"]

        if table_mappings_str:
            try:
                # Parse the JSON string to object
                mapping_obj = json.loads(table_mappings_str)

                # Save formatted JSON
                file_path = os.path.join(output_dir, f"{task_name}.json")
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(mapping_obj, f, ensure_ascii=False, indent=2)

                # print(f"Exported {task_name}")
            except json.JSONDecodeError:
                print(f"Failed to parse table mappings for task: {task_name}")
        else:
            print(f"No table mappings found for task: {task_name}")

    print("Export complete.")


def parse_dms_configs(settings):
    dms_config = settings.get("dms", {})
    input_dir = dms_config.get("output_dir", "dms-config")
    output_file = dms_config.get("parsed_output_file", "dms-config.xlsx")

    if not os.path.exists(input_dir):
        print(f"Input directory not found: {input_dir}")
        return

    print(f"Scanning for JSON files in {input_dir}...")
    files = sorted(find_files(input_dir, "**/*.json"))

    if not files:
        print("No JSON files found.")
        return

    parsed_data = []

    for file in files:
        try:
            with open(file, encoding="utf-8") as f:
                content = json.load(f)
                rules = content.get("rules", [])
        except json.JSONDecodeError:
            print(f"Error decoding JSON file: {file}")
            continue
        except Exception as e:
            print(f"Error reading file {file}: {e}")
            continue

        filename = os.path.basename(file)
        # Remove extension for cleaner name if desired, but original kept extension
        # name = os.path.splitext(filename)[0]
        name = filename

        # We are interested in specific rule types:
        # 1. transformation -> rename -> schema (to find the target schema name)
        # 2. selection -> include (tables included)
        # 3. selection -> exclude (tables excluded)

        target_schema = None
        selection_rules = []

        for rule in rules:
            rule_type = rule.get("rule-type")
            rule_action = rule.get("rule-action")
            rule_target = rule.get("rule-target")

            # Check for schema rename transformation
            if rule_type == "transformation" and rule_action == "rename" and rule_target == "schema":
                target_schema = rule.get("value")

            # Check for selection rules
            elif rule_type == "selection":
                if rule_action in ["include", "exclude"]:
                    selection_rules.append(rule)

        # Process selection rules
        for rule in selection_rules:
            object_locator = rule.get("object-locator", {})
            schema_name = object_locator.get("schema-name")
            table_name = object_locator.get("table-name")

            # If schema is 'dbo' (common in MSSQL), we might want to use the target schema name if available
            final_schema = schema_name
            if schema_name == "dbo" and target_schema:
                final_schema = target_schema

            parsed_data.append(
                {
                    "file_name": name,
                    "rule_type": rule.get("rule-type"),
                    "action": rule.get("rule-action"),
                    "schema": final_schema,
                    "table": table_name,
                }
            )

    if parsed_data:
        parsed_df = pd.DataFrame(parsed_data)
        print(f"Saving parsed configuration to {output_file}...")
        parsed_df.to_excel(output_file, index=False)
        print("Done.")
    else:
        print("No relevant DMS rules found.")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        export_dms_configs(settings)
        parse_dms_configs(settings)
    else:
        print(f"Settings file not found at {settings_path}")
