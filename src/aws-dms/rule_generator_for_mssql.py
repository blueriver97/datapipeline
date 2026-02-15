import datetime
import json
import os
from collections import defaultdict

import yaml


def load_settings(settings_path="settings.yml"):
    with open(settings_path) as f:
        return yaml.safe_load(f)


def get_selection_include_table_rule(rule_name, rule_seq, schema, table):
    return {
        "rule-type": "selection",
        "rule-id": str(rule_seq),
        "rule-name": rule_name,
        "object-locator": {
            "schema-name": schema,
            "table-name": table,
        },
        "rule-action": "include",
        "filters": [],
    }


def get_transformation_remove_column_rule(rule_name, rule_seq, schema, table, column):
    return {
        "rule-type": "transformation",
        "rule-id": str(rule_seq),
        "rule-name": rule_name,
        "rule-target": "column",
        "object-locator": {
            "schema-name": schema,
            "table-name": table,
            "column-name": column,
        },
        "rule-action": "remove-column",
    }


def get_transformation_schema_rename_rule(rule_name, rule_seq, old_schema, new_schema):
    return {
        "rule-type": "transformation",
        "rule-id": str(rule_seq),
        "rule-name": rule_name,
        "rule-target": "schema",
        "object-locator": {
            "schema-name": old_schema,
        },
        "rule-action": "rename",
        "value": new_schema,
    }


def get_transformation_table_rename_lower_case_rule(rule_name, rule_seq, schema, table):
    return {
        "rule-type": "transformation",
        "rule-id": str(rule_seq),
        "rule-name": rule_name,
        "rule-target": "table",
        "object-locator": {
            "schema-name": schema,
            "table-name": table,
        },
        "rule-action": "convert-lowercase",
    }


def get_transformation_add_column_rule(rule_name, rule_seq, schema, table, new_column):
    return {
        "rule-type": "transformation",
        "rule-id": str(rule_seq),
        "rule-name": rule_name,
        "rule-target": "column",
        "object-locator": {
            "schema-name": schema,
            "table-name": table,
        },
        "rule-action": "add-column",
        "value": new_column,
        "expression": "$AR_H_COMMIT_TIMESTAMP",
        "data-type": {
            "type": "datetime",
            "precision": 6,
        },
    }


def generate_dms_rules(settings):
    mssql_config = settings.get("mssql_rules", {})

    # Load lists from settings
    # Expected format in settings.yml:
    # mssql_rules:
    #   tables:
    #     - db.schema.table
    #   except_tables:
    #     - db.schema.table
    #   except_columns:
    #     - db.schema.table.column
    #   lower_case_tables:
    #     - db.schema.table

    tables_list = list(set(mssql_config.get("tables") or []))
    except_tables = list(set(mssql_config.get("except_tables") or []))
    except_columns = list(set(mssql_config.get("except_columns") or []))
    lower_case_tables = list(set(mssql_config.get("lower_case_tables") or []))

    output_dir = mssql_config.get("output_dir", "dms_rules")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 포함/제외할 컬럼 처리
    # 구조: target_columns[db.schema.table][column] = True (포함) / False (제외)
    # False인 경우에만 remove-column을 사용하여 제거

    # Transformation 규칙은 컬럼을 수정합니다.
    # 표준 액션: add-column, remove-column, rename 등.
    target_columns = defaultdict(dict)

    for line in except_columns:
        if "." in line:
            db_schema_table, column = line.rsplit(".", 1)
            target_columns[db_schema_table][column] = False

    # Group tables by database
    # Structure: target[database][table_name] = columns_dict or None
    target = defaultdict(dict)

    for line in tables_list:
        parts = line.split(".")
        if len(parts) == 3:
            database, schema, table = parts

            # Reconstruct key to match target_columns keys
            key = f"{database}.{schema}.{table}"
            target[database][table] = target_columns.get(key)

    date_str = datetime.datetime.now().strftime("%Y%m%d")

    for database, tables_dict in target.items():
        total_rule = []
        global_seq = 0

        # Sort tables for consistent output
        sorted_tables = sorted(tables_dict.keys())

        for table_name in sorted_tables:
            # Original script assumes schema is 'dbo' for rule generation
            schema_name = "dbo"
            full_name = f"{database}.{schema_name}.{table_name}"

            if full_name in except_tables:
                continue

            global_seq += 1
            # Selection rule
            total_rule.append(
                get_selection_include_table_rule(f"include_table_{global_seq}", global_seq, schema_name, table_name)
            )

            # Add CDC column
            global_seq += 1
            total_rule.append(
                get_transformation_add_column_rule(
                    f"add_column_cdc_dt_{global_seq}", global_seq, schema_name, table_name, "cdc_dt"
                )
            )

            # Lowercase table name
            if full_name in lower_case_tables:
                global_seq += 1
                total_rule.append(
                    get_transformation_table_rename_lower_case_rule(
                        f"table_rename_lower_case_{global_seq}", global_seq, schema_name, table_name
                    )
                )

            # Column rules
            columns = tables_dict.get(table_name)
            if columns:
                for column_name, is_include in columns.items():
                    global_seq += 1
                    if is_include:
                        # DMS는 기본적으로 포함
                        pass
                    else:
                        total_rule.append(
                            get_transformation_remove_column_rule(
                                f"remove_column_{column_name}_{global_seq}",
                                global_seq,
                                schema_name,
                                table_name,
                                column_name,
                            )
                        )

        # Schema rename rule (last)
        global_seq += 1
        total_rule.append(
            get_transformation_schema_rename_rule(f"schema_rename_dbo_to_{database}", global_seq, "dbo", database)
        )

        final_form = {"rules": total_rule}

        output_file = os.path.join(output_dir, f"{date_str}-{database}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(final_form, f, ensure_ascii=False, indent=2)

        print(f"Generated {output_file}")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        generate_dms_rules(settings)
    else:
        print(f"Settings file not found at {settings_path}")
