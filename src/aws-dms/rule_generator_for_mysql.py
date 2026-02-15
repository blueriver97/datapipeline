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


def get_selection_exclude_table_rule(rule_name, rule_seq, schema, table):
    return {
        "rule-type": "selection",
        "rule-id": str(rule_seq),
        "rule-name": rule_name,
        "object-locator": {
            "schema-name": schema,
            "table-name": table,
        },
        "rule-action": "exclude",
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


def generate_dms_rules(settings):
    mysql_config = settings.get("rules", {})

    # 설정 파일에서 목록 로드
    # 예상 형식 (settings.yml):
    # mysql_rules:
    #   tables:
    #     - db.table
    #   except_tables:
    #     - db.table
    #   columns:
    #     - db.table.column
    #   except_columns:
    #     - db.table.column
    #   lower_case_tables:
    #     - db.table

    tables_list = list(set(mysql_config.get("tables") or []))
    except_tables = list(set(mysql_config.get("except_tables") or []))
    except_columns = list(set(mysql_config.get("except_columns") or []))
    lower_case_tables = list(set(mysql_config.get("lower_case_tables") or []))

    output_dir = mysql_config.get("output_dir", "dms_rules")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 포함/제외할 컬럼 처리
    # 구조: target_columns[db.table][column] = True (포함) / False (제외)
    target_columns = defaultdict(dict)

    for line in except_columns:
        if "." in line:
            db_table, column = line.rsplit(".", 1)
            target_columns[db_table][column] = False

    # 데이터베이스별로 테이블 그룹화
    # 구조: target[database][table_name] = columns_dict 또는 None
    target = defaultdict(dict)

    for line in tables_list:
        if "." in line:
            database, table = line.rsplit(".", 1)
            target[database][table] = target_columns.get(line)

    date_str = datetime.datetime.now().strftime("%Y%m%d")

    # MySQL은 데이터베이스별로 규칙 파일을 생성하지 않고, 전체를 하나의 파일로 생성

    for database, tables_dict in target.items():
        total_rule = []
        global_seq = 0

        sorted_tables = sorted(tables_dict.keys())

        for table_name in sorted_tables:
            full_name = f"{database}.{table_name}"

            global_seq += 1

            # 테이블 제외 목록 확인
            if full_name in except_tables:
                # 제외된 테이블은 추가 규칙 처리 불필요
                continue
            else:
                total_rule.append(
                    get_selection_include_table_rule(f"include_table_{global_seq}", global_seq, database, table_name)
                )

            # 테이블 이름 소문자 변환
            if full_name in lower_case_tables:
                global_seq += 1
                total_rule.append(
                    get_transformation_table_rename_lower_case_rule(
                        f"table_rename_lower_case_{global_seq}", global_seq, database, table_name
                    )
                )

            # 컬럼 규칙
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
                                database,
                                table_name,
                                column_name,
                            )
                        )

        final_form = {"rules": total_rule}

        output_file = os.path.join(output_dir, f"{date_str}-{database}-mysql.json")
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
