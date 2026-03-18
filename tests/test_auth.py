"""Tests for authentication and tier-based access control."""

import json
import logging
from unittest.mock import patch

import pytest

from tests.conftest import TEST_API_KEYS


class TestTier0AnonymousAccess:
    """Tier-0 endpoints allow anonymous (no API key) access."""

    def test_root_allows_anonymous(self, test_app):
        """The root endpoint is tier0 and needs no key."""
        resp = test_app.get("/")
        assert resp.status_code == 200


class TestInvalidKey:
    """Requests with an unrecognized API key should be rejected."""

    def test_invalid_key_returns_403(self, test_app):
        resp = test_app.post(
            "/v1/system/manifest/refresh",
            headers={"X-API-Key": "bogus-key-does-not-exist"},
        )
        assert resp.status_code == 403

    def test_invalid_key_message(self, test_app):
        resp = test_app.post(
            "/v1/system/manifest/refresh",
            headers={"X-API-Key": "bogus-key-does-not-exist"},
        )
        data = resp.json()
        assert "Invalid" in data.get("detail", "") or "invalid" in data.get("detail", "").lower()


class TestInsufficientTier:
    """A valid key with a tier below the endpoint requirement should be rejected."""

    def test_tier1_key_on_tier3_endpoint_returns_403(self, test_app):
        resp = test_app.post(
            "/v1/system/manifest/refresh",
            headers={"X-API-Key": "test-key-tier1"},
        )
        assert resp.status_code == 403

    def test_tier0_key_on_tier3_endpoint_returns_403(self, test_app):
        resp = test_app.post(
            "/v1/system/manifest/refresh",
            headers={"X-API-Key": "test-key-tier0"},
        )
        assert resp.status_code == 403


class TestSufficientTier:
    """A valid key with a tier matching or exceeding the requirement should succeed."""

    def test_tier3_key_on_tier3_endpoint(self, test_app, _mock_manifest):
        """Tier3 key should be allowed to call the manifest refresh endpoint."""
        # The refresh will return quickly because manifest is mocked
        resp = test_app.post(
            "/v1/system/manifest/refresh",
            headers={"X-API-Key": "test-key-tier3"},
        )
        # 200 means access was granted (manifest refresh itself may return any payload)
        assert resp.status_code == 200


class TestNoRawApiKeyInLogs:
    """Structured log output must never contain the raw API key value."""

    def test_raw_key_not_in_log_output(self, test_app, capfd):
        """Make a request with a known key and verify it does not leak into stderr."""
        secret_key = "test-key-tier3"
        # Hit an endpoint that triggers structured logging
        test_app.get("/", headers={"X-API-Key": secret_key})
        # Also trigger a 403 path which logs a warning
        test_app.post(
            "/v1/system/manifest/refresh",
            headers={"X-API-Key": "bogus-leak-test"},
        )

        captured = capfd.readouterr()
        stderr_output = captured.err

        # The raw secret key value should not appear in structured logs.
        # (The key "test-key-tier3" might appear in non-log output during
        # test setup, so we specifically check structured JSON lines.)
        for line in stderr_output.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                # If it parses as JSON it is a structured log line.
                # The raw API key value must not appear anywhere in it.
                serialized = json.dumps(payload)
                assert secret_key not in serialized, (
                    f"Raw API key leaked into structured log: {line}"
                )
            except json.JSONDecodeError:
                # Not a JSON log line; skip.
                pass
