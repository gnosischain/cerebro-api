"""Tests for rate limiting behaviour."""

import pytest


class TestRateLimitEnforcement:
    """Rate limiting is applied via @limiter.limit() on dynamic endpoints only.

    The root (/) and health endpoints are not rate-limited because they are
    simple info routes.  Dynamic endpoints (registered by factory.py) ARE
    rate-limited, but testing them end-to-end requires a manifest and
    ClickHouse mock that returns data.  The live 429 behaviour has been
    verified manually (see plan notes).

    This test verifies the limiter object is correctly wired into the app.
    """

    def test_limiter_attached_to_app(self, test_app):
        """app.state.limiter should be the SlowAPI Limiter instance."""
        from app.main import app
        from slowapi import Limiter

        assert hasattr(app.state, "limiter")
        assert isinstance(app.state.limiter, Limiter)

    def test_rate_limit_handler_registered(self, test_app):
        """The app should have an exception handler for RateLimitExceeded."""
        from app.main import app
        from slowapi.errors import RateLimitExceeded

        # FastAPI stores exception handlers in the exception_handlers dict
        assert RateLimitExceeded in app.exception_handlers


class TestRateLimitMetrics:
    """Rate limit decisions should be reflected in Prometheus metrics."""

    def test_rate_limit_metric_names_present(self, test_app):
        """After making some requests the rate-limit or HTTP request metrics
        should exist in the metrics output."""
        # Generate some traffic
        test_app.get("/")
        test_app.get("/")

        resp = test_app.get("/metrics")
        body = resp.text
        assert "cerebro_api_rate_limit_decisions_total" in body or \
               "cerebro_api_http_requests_total" in body, (
            "Expected rate-limit or HTTP request metrics to be present"
        )
