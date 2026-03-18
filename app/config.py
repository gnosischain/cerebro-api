import json
import logging
import os
from typing import Any, Dict, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings

logger = logging.getLogger("cerebro_api.config")


def load_api_keys_from_file(filepath: str) -> Dict[str, Any]:
    """Load API keys from a JSON file if it exists."""
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Error loading API keys from %s: %s", filepath, e)
    return {}


class Settings(BaseSettings):
    # App
    API_TITLE: str = "Gnosis Cerebro Data API"
    API_VERSION: str = "v1"
    DEBUG: bool = False

    # Manifest Source (URL takes precedence over Path)
    DBT_MANIFEST_URL: Optional[str] = "https://gnosischain.github.io/dbt-cerebro/manifest.json"
    DBT_MANIFEST_PATH: str = "./manifest.json"
    API_CONFIG_PATH: str = "./api_config.yaml"
    DBT_MANIFEST_REFRESH_ENABLED: bool = True
    DBT_MANIFEST_REFRESH_INTERVAL_SECONDS: int = 300

    # API Keys file path (JSON file with user keys)
    API_KEYS_FILE: str = "./api_keys.json"

    # ClickHouse
    CLICKHOUSE_URL: Optional[str] = None
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8443
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "default"
    CLICKHOUSE_SECURE: bool = True

    # Security: API Keys mapped to user info
    API_KEYS: Dict[str, Any] = {}

    @field_validator("API_KEYS", mode="before")
    @classmethod
    def normalize_api_keys(cls, v):
        if not isinstance(v, dict):
            return {}

        normalized = {}
        for key, value in v.items():
            if isinstance(value, str):
                normalized[key] = {
                    "user": "anonymous",
                    "tier": value,
                    "org": None,
                }
            elif isinstance(value, dict):
                normalized[key] = {
                    "user": value.get("user", "anonymous"),
                    "tier": value.get("tier", "tier0"),
                    "org": value.get("org"),
                }
            else:
                continue

        return normalized

    DEFAULT_ENDPOINT_TIER: str = "tier0"

    TIER_RATE_LIMITS: Dict[str, int] = {
        "tier0": 20,
        "tier1": 100,
        "tier2": 500,
        "tier3": 10000,
    }

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.API_KEYS:
            logger.info("Loaded %d API keys from environment variable", len(self.API_KEYS))
        elif self.API_KEYS_FILE:
            file_keys = load_api_keys_from_file(self.API_KEYS_FILE)
            if file_keys:
                self.API_KEYS = self.normalize_api_keys(file_keys)
                logger.info("Loaded %d API keys from %s", len(self.API_KEYS), self.API_KEYS_FILE)
            else:
                logger.warning("No API keys found. Create %s or set API_KEYS env var.", self.API_KEYS_FILE)


settings = Settings()
