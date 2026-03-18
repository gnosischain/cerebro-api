"""Tests for system endpoints: /, /health, /metrics."""

import pytest


class TestRootEndpoint:
    def test_root_returns_200(self, test_app):
        resp = test_app.get("/")
        assert resp.status_code == 200

    def test_root_contains_status(self, test_app):
        data = test_app.get("/").json()
        assert "status" in data
        assert data["status"] == "online"

    def test_root_contains_service(self, test_app):
        data = test_app.get("/").json()
        assert "service" in data

    def test_root_contains_docs(self, test_app):
        data = test_app.get("/").json()
        assert "docs" in data
        assert data["docs"] == "/docs"


class TestHealthEndpoint:
    def test_health_returns_200_when_db_ok(self, test_app):
        resp = test_app.get("/health")
        assert resp.status_code == 200

    def test_health_body_has_status(self, test_app):
        data = test_app.get("/health").json()
        assert "status" in data


class TestMetricsEndpoint:
    def test_metrics_returns_200(self, test_app):
        resp = test_app.get("/metrics")
        assert resp.status_code == 200

    def test_metrics_content_type(self, test_app):
        resp = test_app.get("/metrics")
        ct = resp.headers.get("content-type", "")
        # Prometheus exposition format uses text/plain or openmetrics
        assert "text/" in ct or "openmetrics" in ct

    def test_metrics_contains_cerebro_prefix(self, test_app):
        resp = test_app.get("/metrics")
        body = resp.text
        assert "cerebro_api_" in body

    def test_metrics_contains_http_requests_total(self, test_app):
        resp = test_app.get("/metrics")
        body = resp.text
        assert "cerebro_api_http_requests_total" in body

    def test_metrics_contains_manifest_models_loaded(self, test_app):
        resp = test_app.get("/metrics")
        body = resp.text
        assert "cerebro_api_manifest_models_loaded" in body
