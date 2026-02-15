import json
import time
from pathlib import Path

import requests
import yaml


def load_settings(settings_path: str) -> dict:
    with open(settings_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_connectors_status(target_url: str, proxies: dict, verify: bool):
    try:
        result = requests.get(f"{target_url}?expand=status", verify=verify, proxies=proxies)
        result.raise_for_status()
        return result.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching connector status: {e}")
        return {}


def print_workers_status(connectors: dict):
    workers: dict = {}
    for name, config in connectors.items():
        worker_id = config["status"]["connector"]["worker_id"].split(".")[0]

        if worker_id not in workers:
            workers[worker_id] = []
        workers[worker_id].append(config)

    print(json.dumps(workers, ensure_ascii=False, indent=2))


def restart_connectors(target_url: str, connectors: dict, proxies: dict, verify: bool):
    for connector in connectors:
        try:
            print(f"Restarting connector: {connector}")
            requests.post(f"{target_url}/{connector}/restart", verify=verify, proxies=proxies)
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Error restarting connector {connector}: {e}")


if __name__ == "__main__":
    # 설정 파일 로드
    current_dir = Path(__file__).parent
    settings_path = current_dir / "settings.yml"

    try:
        settings = load_settings(str(settings_path))
    except FileNotFoundError:
        print(f"Settings file not found at {settings_path}")
        raise
    except yaml.YAMLError as e:
        print(f"Error parsing settings file: {e}")
        raise

    debezium_config = settings.get("debezium", {})
    target_url = debezium_config.get("url", "http://localhost:8083/connectors")
    proxies = debezium_config.get("proxies", None)
    verify = debezium_config.get("verify", False)

    # 커넥터 상태 조회 및 출력
    connectors_status = get_connectors_status(target_url, proxies, verify)
    if connectors_status:
        print_workers_status(connectors_status)

        # 커넥터 재시작 (필요한 경우 주석 해제 또는 조건 추가)
        # restart_connectors(target_url, connectors_status, proxies, verify)
