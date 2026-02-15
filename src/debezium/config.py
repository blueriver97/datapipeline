import yaml
from pydantic import BaseModel, HttpUrl, field_validator


class VaultConfig(BaseModel):
    url: HttpUrl
    username: str
    password: str
    mount_point: str
    path: dict[str, str]


class ConnectionConfig(BaseModel):
    name: str
    host: str
    port: str
    user: str
    password: str


class KafkaClusterConfig(BaseModel):
    bootstrap_servers: list[str]
    schema_registry: HttpUrl
    username: str | None = None
    password: str | None = None

    @field_validator("bootstrap_servers")
    @classmethod
    def check_servers_not_empty(cls, v):
        if not v:
            raise ValueError("Bootstrap servers list cannot be empty")
        return v


class AppConfig(BaseModel):
    prefix: str
    database: str
    tables: list
    vault: VaultConfig
    kafka: dict[str, KafkaClusterConfig]

    @classmethod
    def load(cls, file_path: str) -> "AppConfig":
        try:
            with open(file_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
                return cls(**data)
        except FileNotFoundError:
            print(f"ERROR: Configuration file not found at {file_path}")
            raise
        except Exception as e:
            print(f"ERROR: Failed to parse configuration - {str(e)}")
            raise


if __name__ == "__main__":
    config = AppConfig.load("settings.yaml")
    print(config)
