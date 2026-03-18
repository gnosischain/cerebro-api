"""Shared fixtures for the cerebro-api test suite."""

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Test API keys injected into settings before the app module loads
# ---------------------------------------------------------------------------
TEST_API_KEYS = {
    "test-key-tier0": {"user": "tester0", "tier": "tier0", "org": "testorg"},
    "test-key-tier1": {"user": "tester1", "tier": "tier1", "org": "testorg"},
    "test-key-tier2": {"user": "tester2", "tier": "tier2", "org": "testorg"},
    "test-key-tier3": {"user": "tester3", "tier": "tier3", "org": "testorg"},
}


@pytest.fixture()
def api_keys():
    """Return the dictionary of test API keys."""
    return TEST_API_KEYS


@pytest.fixture()
def _mock_clickhouse():
    """Patch ClickHouseClient so no real database connection is attempted."""
    with patch("app.database.ClickHouseClient") as mock_cls:
        mock_client = MagicMock()
        mock_cls.get_client.return_value = mock_client
        mock_cls.query.return_value = []
        yield mock_cls


@pytest.fixture()
def _mock_manifest():
    """Patch ManifestLoader to avoid fetching a real manifest on import."""
    mock_loader = MagicMock()
    mock_loader.get_all_models.return_value = []
    mock_loader.model_count.return_value = 0
    mock_loader.reload_if_changed.return_value = (False, None)
    with patch("app.manifest.manifest", mock_loader), \
         patch("app.factory.manifest", mock_loader), \
         patch("app.router_manager.manifest", mock_loader):
        yield mock_loader


@pytest.fixture()
def _mock_settings():
    """Inject test API keys and disable manifest URL fetching."""
    with patch("app.config.settings") as mock_settings:
        mock_settings.API_TITLE = "Cerebro Test API"
        mock_settings.API_VERSION = "v1-test"
        mock_settings.DEBUG = False
        mock_settings.API_KEYS = TEST_API_KEYS.copy()
        mock_settings.DEFAULT_ENDPOINT_TIER = "tier0"
        mock_settings.TIER_RATE_LIMITS = {
            "tier0": 20,
            "tier1": 100,
            "tier2": 500,
            "tier3": 10000,
        }
        mock_settings.DBT_MANIFEST_URL = None
        mock_settings.DBT_MANIFEST_PATH = "./manifest.json"
        mock_settings.DBT_MANIFEST_REFRESH_ENABLED = False
        mock_settings.DBT_MANIFEST_REFRESH_INTERVAL_SECONDS = 300
        mock_settings.API_CONFIG_PATH = "./api_config.yaml"
        mock_settings.CLICKHOUSE_URL = None
        mock_settings.CLICKHOUSE_HOST = "localhost"
        mock_settings.CLICKHOUSE_PORT = 8443
        mock_settings.CLICKHOUSE_USER = "default"
        mock_settings.CLICKHOUSE_PASSWORD = ""
        mock_settings.CLICKHOUSE_DATABASE = "default"
        mock_settings.CLICKHOUSE_SECURE = True
        yield mock_settings


@pytest.fixture()
def test_app(_mock_clickhouse, _mock_manifest):
    """Create a fresh TestClient around the FastAPI app.

    External dependencies (ClickHouse, manifest) are mocked so the test
    suite runs without any network or database access.
    """
    # Patch settings.API_KEYS directly on the real settings object so the
    # app's security module picks them up without replacing the entire object.
    from app.config import settings
    original_keys = settings.API_KEYS
    settings.API_KEYS = TEST_API_KEYS.copy()
    try:
        from app.main import app
        with TestClient(app, raise_server_exceptions=False) as client:
            yield client
    finally:
        settings.API_KEYS = original_keys
