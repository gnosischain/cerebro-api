import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# ---------------------------------------------------------------------------
# Histogram buckets
# ---------------------------------------------------------------------------
REQUEST_DURATION_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
    0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
)
ROWS_RETURNED_BUCKETS = (1, 10, 50, 100, 500, 1000, 5000, 10000)

# ---------------------------------------------------------------------------
# Reserved LogRecord keys (filtered from extra fields)
# ---------------------------------------------------------------------------
RESERVED_LOG_RECORD_KEYS = {
    "args", "asctime", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelname", "levelno", "lineno", "module", "msecs",
    "message", "msg", "name", "pathname", "process", "processName",
    "relativeCreated", "stack_info", "thread", "threadName", "taskName",
}

# ---------------------------------------------------------------------------
# HTTP metrics
# ---------------------------------------------------------------------------
cerebro_api_http_requests_total = Counter(
    "cerebro_api_http_requests_total",
    "Total HTTP requests",
    ("method", "route", "status"),
)
cerebro_api_http_request_duration_seconds = Histogram(
    "cerebro_api_http_request_duration_seconds",
    "HTTP request latency",
    ("method", "route"),
    buckets=REQUEST_DURATION_BUCKETS,
)
cerebro_api_http_requests_in_progress = Gauge(
    "cerebro_api_http_requests_in_progress",
    "HTTP requests currently in progress",
    ("method", "route"),
)

# ---------------------------------------------------------------------------
# Auth metrics
# ---------------------------------------------------------------------------
cerebro_api_auth_resolutions_total = Counter(
    "cerebro_api_auth_resolutions_total",
    "Auth identity resolutions",
    ("required_tier", "result"),
)
cerebro_api_access_denied_total = Counter(
    "cerebro_api_access_denied_total",
    "Access denied events",
    ("required_tier", "provided_tier", "reason"),
)

# ---------------------------------------------------------------------------
# Rate-limit metrics
# ---------------------------------------------------------------------------
cerebro_api_rate_limit_decisions_total = Counter(
    "cerebro_api_rate_limit_decisions_total",
    "Rate-limit decisions",
    ("tier", "result", "identity_kind"),
)

# ---------------------------------------------------------------------------
# Dynamic endpoint metrics
# ---------------------------------------------------------------------------
cerebro_api_dynamic_requests_total = Counter(
    "cerebro_api_dynamic_requests_total",
    "Dynamic API endpoint requests",
    ("category", "resource", "granularity", "tier", "method", "status"),
)
cerebro_api_dynamic_request_duration_seconds = Histogram(
    "cerebro_api_dynamic_request_duration_seconds",
    "Dynamic API endpoint latency",
    ("category", "resource", "granularity", "tier", "method"),
    buckets=REQUEST_DURATION_BUCKETS,
)

# ---------------------------------------------------------------------------
# ClickHouse metrics
# ---------------------------------------------------------------------------
cerebro_api_clickhouse_query_duration_seconds = Histogram(
    "cerebro_api_clickhouse_query_duration_seconds",
    "ClickHouse query latency",
    ("category", "resource", "granularity", "tier", "status"),
    buckets=REQUEST_DURATION_BUCKETS,
)
cerebro_api_clickhouse_query_errors_total = Counter(
    "cerebro_api_clickhouse_query_errors_total",
    "Failed ClickHouse queries",
    ("category", "resource", "granularity", "tier"),
)
cerebro_api_clickhouse_rows_returned = Histogram(
    "cerebro_api_clickhouse_rows_returned",
    "Rows returned by ClickHouse queries",
    ("category", "resource", "granularity", "tier"),
    buckets=ROWS_RETURNED_BUCKETS,
)

# ---------------------------------------------------------------------------
# Manifest / router metrics
# ---------------------------------------------------------------------------
cerebro_api_manifest_refresh_total = Counter(
    "cerebro_api_manifest_refresh_total",
    "Manifest refresh operations",
    ("trigger", "status"),
)
cerebro_api_manifest_refresh_duration_seconds = Histogram(
    "cerebro_api_manifest_refresh_duration_seconds",
    "Manifest refresh latency",
    ("trigger",),
    buckets=REQUEST_DURATION_BUCKETS,
)
cerebro_api_manifest_models_loaded = Gauge(
    "cerebro_api_manifest_models_loaded",
    "Number of dbt models loaded from manifest",
)
cerebro_api_dynamic_routes_registered = Gauge(
    "cerebro_api_dynamic_routes_registered",
    "Number of dynamic API routes registered",
)


# ===================================================================
# JSON logging
# ===================================================================

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = str(record.getMessage())
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        for key, value in record.__dict__.items():
            if key in RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def setup_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    root.addHandler(handler)

    for name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        uv_logger = logging.getLogger(name)
        uv_logger.handlers.clear()
        uv_logger.addHandler(handler)
        uv_logger.setLevel(logging.INFO)
        uv_logger.propagate = False


def log_event(
    logger: logging.Logger,
    event: str,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    safe_fields = {
        (
            key
            if key not in RESERVED_LOG_RECORD_KEYS and key != "event"
            else f"field_{key}"
        ): value
        for key, value in fields.items()
        if value is not None
    }
    logger.log(level, str(event), extra={"event": str(event), **safe_fields})


# ===================================================================
# HTTP path normalisation
# ===================================================================

def normalize_http_path(request: Request) -> str:
    route = request.scope.get("route")
    route_path = getattr(route, "path", None)
    if route_path:
        return str(route_path)
    return request.url.path


# ===================================================================
# Observer helpers
# ===================================================================

def observe_http_request(
    method: str, path: str, status: str, elapsed_seconds: float,
) -> None:
    cerebro_api_http_requests_total.labels(
        method=method, route=path, status=status,
    ).inc()
    cerebro_api_http_request_duration_seconds.labels(
        method=method, route=path,
    ).observe(elapsed_seconds)


def observe_auth_resolution(required_tier: str, result: str) -> None:
    cerebro_api_auth_resolutions_total.labels(
        required_tier=required_tier, result=result,
    ).inc()


def observe_access_denied(
    required_tier: str, provided_tier: str, reason: str,
) -> None:
    cerebro_api_access_denied_total.labels(
        required_tier=required_tier,
        provided_tier=provided_tier,
        reason=reason,
    ).inc()


def observe_rate_limit(tier: str, result: str, identity_kind: str) -> None:
    cerebro_api_rate_limit_decisions_total.labels(
        tier=tier, result=result, identity_kind=identity_kind,
    ).inc()


def observe_dynamic_request(
    *, category: str, resource: str, granularity: str,
    tier: str, method: str, status: str, elapsed_seconds: float,
) -> None:
    cerebro_api_dynamic_requests_total.labels(
        category=category, resource=resource, granularity=granularity,
        tier=tier, method=method, status=status,
    ).inc()
    cerebro_api_dynamic_request_duration_seconds.labels(
        category=category, resource=resource, granularity=granularity,
        tier=tier, method=method,
    ).observe(elapsed_seconds)


def observe_clickhouse_query(
    *, category: str, resource: str, granularity: str, tier: str,
    status: str, elapsed_seconds: float, row_count: int | None = None,
) -> None:
    cerebro_api_clickhouse_query_duration_seconds.labels(
        category=category, resource=resource, granularity=granularity,
        tier=tier, status=status,
    ).observe(elapsed_seconds)

    if status == "error":
        cerebro_api_clickhouse_query_errors_total.labels(
            category=category, resource=resource,
            granularity=granularity, tier=tier,
        ).inc()

    if row_count is not None:
        cerebro_api_clickhouse_rows_returned.labels(
            category=category, resource=resource,
            granularity=granularity, tier=tier,
        ).observe(row_count)


def observe_manifest_refresh(
    trigger: str, status: str, elapsed_seconds: float,
) -> None:
    cerebro_api_manifest_refresh_total.labels(
        trigger=trigger, status=status,
    ).inc()
    cerebro_api_manifest_refresh_duration_seconds.labels(
        trigger=trigger,
    ).observe(elapsed_seconds)


# ===================================================================
# Prometheus exposition
# ===================================================================

def metrics_response() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ===================================================================
# Middleware
# ===================================================================

class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = normalize_http_path(request)
        method = request.method
        started = time.perf_counter()
        status = "500"
        cerebro_api_http_requests_in_progress.labels(
            method=method, route=path,
        ).inc()
        try:
            response = await call_next(request)
            status = str(response.status_code)
            return response
        finally:
            elapsed = time.perf_counter() - started
            cerebro_api_http_requests_in_progress.labels(
                method=method, route=path,
            ).dec()
            observe_http_request(
                method=method, path=path,
                status=status, elapsed_seconds=elapsed,
            )
            # Structured request log
            log_event(
                logging.getLogger("cerebro_api.http"),
                "http_request",
                method=method,
                route=path,
                status=int(status),
                duration_seconds=round(elapsed, 4),
                success=int(status) < 400,
            )
