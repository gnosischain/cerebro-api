"""Tests for the observability module: JSON logging, Prometheus metrics, middleware."""

import json
import logging
import sys
from io import StringIO
from unittest.mock import patch

import pytest

from app.observability import (
    JsonFormatter,
    PrometheusMiddleware,
    log_event,
    metrics_response,
    setup_logging,
)


class TestJsonFormatter:
    """JsonFormatter should produce valid JSON with required fields."""

    def _make_record(self, msg="hello", level=logging.INFO, name="test"):
        logger = logging.getLogger(name)
        record = logger.makeRecord(
            name=name,
            level=level,
            fn="test.py",
            lno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )
        return record

    def test_output_is_valid_json(self):
        fmt = JsonFormatter()
        record = self._make_record("test message")
        output = fmt.format(record)
        payload = json.loads(output)
        assert isinstance(payload, dict)

    def test_contains_timestamp(self):
        fmt = JsonFormatter()
        record = self._make_record()
        payload = json.loads(fmt.format(record))
        assert "timestamp" in payload

    def test_contains_level(self):
        fmt = JsonFormatter()
        record = self._make_record(level=logging.WARNING)
        payload = json.loads(fmt.format(record))
        assert payload["level"] == "WARNING"

    def test_contains_logger_name(self):
        fmt = JsonFormatter()
        record = self._make_record(name="cerebro_api.test")
        payload = json.loads(fmt.format(record))
        assert payload["logger"] == "cerebro_api.test"

    def test_contains_message(self):
        fmt = JsonFormatter()
        record = self._make_record("specific message")
        payload = json.loads(fmt.format(record))
        assert payload["message"] == "specific message"

    def test_extra_fields_are_included(self):
        fmt = JsonFormatter()
        record = self._make_record("msg")
        record.custom_field = "custom_value"
        payload = json.loads(fmt.format(record))
        assert payload.get("custom_field") == "custom_value"


class TestLogEvent:
    """log_event should inject an 'event' field and extra kwargs."""

    def test_event_field_in_output(self):
        logger = logging.getLogger("test_log_event")
        logger.handlers.clear()
        stream = StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        log_event(logger, "my_event", tier="tier1", success=True)

        output = stream.getvalue().strip()
        payload = json.loads(output)
        assert payload["event"] == "my_event"
        assert payload["tier"] == "tier1"
        assert payload["success"] is True

    def test_extra_kwargs_present(self):
        logger = logging.getLogger("test_log_event_extra")
        logger.handlers.clear()
        stream = StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        log_event(logger, "another_event", foo="bar", count=42)

        output = stream.getvalue().strip()
        payload = json.loads(output)
        assert payload.get("foo") == "bar"
        assert payload.get("count") == 42


class TestSetupLogging:
    """setup_logging should configure the root logger with a JsonFormatter on stderr."""

    def test_root_logger_has_handler(self):
        setup_logging()
        root = logging.getLogger()
        assert len(root.handlers) > 0

    def test_root_handler_uses_json_formatter(self):
        setup_logging()
        root = logging.getLogger()
        handler = root.handlers[0]
        assert isinstance(handler.formatter, JsonFormatter)

    def test_handler_writes_to_stderr(self):
        setup_logging()
        root = logging.getLogger()
        handler = root.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        assert handler.stream is sys.stderr


class TestPrometheusMiddleware:
    """PrometheusMiddleware should increment the HTTP request counter."""

    def test_request_counter_increments(self, test_app):
        """After making a request, cerebro_api_http_requests_total should appear
        in the metrics output with a count > 0."""
        # Make a request to generate metric data
        test_app.get("/")

        resp = test_app.get("/metrics")
        body = resp.text
        # The counter should have been incremented
        assert "cerebro_api_http_requests_total" in body
        # Look for a line with an actual count (not just HELP/TYPE)
        lines = body.splitlines()
        counter_lines = [
            l for l in lines
            if l.startswith("cerebro_api_http_requests_total{")
        ]
        assert len(counter_lines) > 0, "Expected at least one counter sample"


class TestMetricsResponse:
    """metrics_response() should return valid Prometheus exposition format."""

    def test_returns_response_object(self):
        resp = metrics_response()
        assert resp is not None
        assert hasattr(resp, "body")

    def test_media_type_is_prometheus(self):
        resp = metrics_response()
        ct = resp.media_type or ""
        assert "text/" in ct or "openmetrics" in ct

    def test_body_contains_cerebro_prefix(self):
        resp = metrics_response()
        body = resp.body.decode("utf-8") if isinstance(resp.body, bytes) else resp.body
        assert "cerebro_api_" in body
