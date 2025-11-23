import os
from typing import Dict, Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # App
    API_TITLE: str = "Gnosis Cerebro Data API"
    API_VERSION: str = "v1"
    DEBUG: bool = False
    
    # Manifest Source (URL takes precedence over Path)
    DBT_MANIFEST_URL: Optional[str] = "https://gnosischain.github.io/dbt-cerebro/manifest.json"
    DBT_MANIFEST_PATH: str = "./manifest.json"
    API_CONFIG_PATH: str = "./api_config.yaml"

    # ClickHouse
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8443
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "default"
    CLICKHOUSE_SECURE: bool = True 

    # Security (Map Key -> Tier)
    API_KEYS: Dict[str, str] = {}

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()