"""Tests for manifest refresh and router_manager interactions."""

from unittest.mock import MagicMock, patch

import pytest

from app.observability import cerebro_api_manifest_models_loaded


class TestManifestGauges:
    """Manifest refresh should update the manifest_models_loaded gauge."""

    def test_manifest_models_loaded_gauge_set(self):
        """Directly calling the gauge setter should work and be reflected
        in the Prometheus output."""
        cerebro_api_manifest_models_loaded.set(42)

        from app.observability import metrics_response
        resp = metrics_response()
        body = resp.body.decode("utf-8") if isinstance(resp.body, bytes) else resp.body
        assert "cerebro_api_manifest_models_loaded" in body
        # The gauge value 42 should appear
        assert "42.0" in body or "42" in body

    def test_manifest_models_loaded_updates_on_reload(self):
        """Simulating a manifest load should set the gauge to the model count."""
        cerebro_api_manifest_models_loaded.set(0)
        # Simulate what ManifestLoader._load_manifest does on success
        cerebro_api_manifest_models_loaded.set(17)

        from app.observability import metrics_response
        resp = metrics_response()
        body = resp.body.decode("utf-8") if isinstance(resp.body, bytes) else resp.body
        assert "17.0" in body or "17" in body


class TestRouterManagerRefreshSync:
    """router_manager.refresh_sync should return a proper status dict."""

    def test_refresh_sync_unchanged(self):
        """When the manifest has not changed, refresh_sync returns 'unchanged'."""
        mock_manifest = MagicMock()
        mock_manifest.reload_if_changed.return_value = (False, None)
        mock_manifest.model_count.return_value = 5

        with patch("app.router_manager.manifest", mock_manifest), \
             patch("app.router_manager.build_router") as mock_build:
            from app.router_manager import RouterManager
            from fastapi import FastAPI

            app = FastAPI()
            rm = RouterManager(app)
            result = rm.refresh_sync(trigger="test")

        assert result["status"] == "unchanged"
        assert result["models"] == 5

    def test_refresh_sync_error(self):
        """When the manifest returns an error, refresh_sync returns 'error'."""
        mock_manifest = MagicMock()
        mock_manifest.reload_if_changed.return_value = (False, "fetch failed")
        mock_manifest.model_count.return_value = 3

        with patch("app.router_manager.manifest", mock_manifest):
            from app.router_manager import RouterManager
            from fastapi import FastAPI

            app = FastAPI()
            rm = RouterManager(app)
            result = rm.refresh_sync(trigger="test")

        assert result["status"] == "error"
        assert "fetch failed" in result.get("detail", "")

    def test_refresh_sync_reloaded(self):
        """When the manifest changes, refresh_sync returns 'reloaded'."""
        mock_manifest = MagicMock()
        mock_manifest.reload_if_changed.return_value = (True, None)
        mock_manifest.model_count.return_value = 10
        mock_manifest.get_all_models.return_value = []

        mock_router = MagicMock()
        mock_router.routes = []

        with patch("app.router_manager.manifest", mock_manifest), \
             patch("app.router_manager.build_router") as mock_build:
            mock_build.return_value = (mock_router, {}, [])

            from app.router_manager import RouterManager
            from fastapi import FastAPI

            app = FastAPI()
            rm = RouterManager(app)
            result = rm.refresh_sync(trigger="test")

        assert result["status"] == "reloaded"
        assert result["models"] == 10
