import json
import os
import shutil
from pathlib import Path

import requests
import yaml


def load_settings(settings_path: str) -> dict:
    with open(settings_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def prepare_directory(output_dir: str) -> None:
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)


def export_debezium_configs(settings):
    debezium_config = settings.get("debezium", {})
    target_url = debezium_config.get("url", "http://localhost:8083/connectors")
    target_dir = debezium_config.get("output_dir", "debezium-configs")
    proxies = debezium_config.get("proxies", None)
    verify = debezium_config.get("verify", False)

    # 커넥터 목록 조회
    try:
        result = requests.get(target_url, verify=verify, proxies=proxies)
        result.raise_for_status()
        connectors = result.json()
        print(connectors)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching connectors: {e}")
        return

    prepare_directory(target_dir)

    # 각 커넥터 설정 내보내기
    for connector in connectors:
        try:
            res = requests.get(f"{target_url}/{connector}", verify=verify, proxies=proxies)
            res.raise_for_status()

            output_file = Path(target_dir) / f"{connector}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(res.json(), f, ensure_ascii=False, indent=2)
            print(f"Exported {connector} to {output_file}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching config for {connector}: {e}")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(current_dir, "settings.yml")

    if os.path.exists(settings_path):
        settings = load_settings(settings_path)
        export_debezium_configs(settings)
    else:
        print(f"Settings file not found at {settings_path}")
