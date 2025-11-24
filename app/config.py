import os
import json
from typing import Dict, List, Optional, Any
from pydantic_settings import BaseSettings
from pydantic import field_validator


def load_api_keys_from_file(filepath: str) -> Dict[str, Any]:
    """Load API keys from a JSON file if it exists."""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Error loading API keys from {filepath}: {e}")
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
    
    # API Keys file path (JSON file with user keys)
    API_KEYS_FILE: str = "./api_keys.json"

    # ClickHouse
    # Option 1: Use URL (for ClickHouse Cloud)
    CLICKHOUSE_URL: Optional[str] = None  # e.g., "ujt1j3jrk0.eu-central-1.aws.clickhouse.cloud"
    
    # Option 2: Individual settings (URL takes precedence if provided)
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8443
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "default"
    CLICKHOUSE_SECURE: bool = True

    # Security: API Keys mapped to user info
    # Can be set via env var OR loaded from API_KEYS_FILE
    # Format: {
    #   "sk_live_abc123": {"user": "alice", "tier": "tier0", "org": "Acme Inc"},
    #   "sk_live_xyz789": {"user": "bob", "tier": "tier2", "org": "Partner Co"},
    # }
    API_KEYS: Dict[str, Any] = {}

    @field_validator('API_KEYS', mode='before')
    @classmethod
    def normalize_api_keys(cls, v):
        """
        Normalize API keys to full user format.
        Supports both simple format (key -> tier) and full format (key -> {user, tier, org}).
        """
        if not isinstance(v, dict):
            return {}
        
        normalized = {}
        for key, value in v.items():
            if isinstance(value, str):
                # Simple format: "sk_key": "tier0" -> convert to full format
                normalized[key] = {
                    "user": "anonymous",
                    "tier": value,
                    "org": None
                }
            elif isinstance(value, dict):
                # Full format: ensure required fields exist
                normalized[key] = {
                    "user": value.get("user", "anonymous"),
                    "tier": value.get("tier", "tier0"),
                    "org": value.get("org")
                }
            else:
                # Skip invalid entries
                continue
        
        return normalized

    # Default tier for endpoints without a tier tag (for testing, set to tier0)
    DEFAULT_ENDPOINT_TIER: str = "tier0"

    # Tier definitions with rate limits (requests per minute)
    TIER_RATE_LIMITS: Dict[str, int] = {
        "tier0": 20,      # Public/Free tier
        "tier1": 100,     # Partner tier
        "tier2": 500,     # Premium tier
        "tier3": 10000,   # Internal/Admin tier
    }

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Allow extra env vars without raising errors

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Debug: show if API_KEYS came from env
        if self.API_KEYS:
            print(f"✅ Loaded {len(self.API_KEYS)} API keys from environment variable")
        # Load API keys from file if env var is empty and file exists
        elif self.API_KEYS_FILE:
            file_keys = load_api_keys_from_file(self.API_KEYS_FILE)
            if file_keys:
                # Normalize the loaded keys
                self.API_KEYS = self.normalize_api_keys(file_keys)
                print(f"✅ Loaded {len(self.API_KEYS)} API keys from {self.API_KEYS_FILE}")
            else:
                print(f"⚠️ No API keys found. Create {self.API_KEYS_FILE} or set API_KEYS env var.")


settings = Settings()