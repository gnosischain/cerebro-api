import logging

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.config import settings
from app.database import ClickHouseClient
from app.observability import PrometheusMiddleware, log_event, metrics_response
from app.router_manager import RouterManager
from app.security import (
    check_tier_access,
    get_api_key,
    limiter,
    observe_rate_limit_blocked,
)

logger = logging.getLogger("cerebro_api")

description = """
**Gnosis Cerebro API** - Data API dynamically generated from the `dbt-cerebro` manifest.
Serves data directly from ClickHouse with authentication and tier-based access control.

---

**Authentication:** Tier1+ endpoints require the header `X-API-Key: <your_key>`.
Tier0 endpoints are publicly accessible without authentication.

**Access Tiers:**

    - tier0 → Public   (100/min, no key required)
    - tier1 → Partner  (500/min)
    - tier2 → Premium  (1000/min)
    - tier3 → Internal (10k/min)
"""

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=description,
)

# SlowAPI state + 429 handler
app.state.limiter = limiter


def _custom_rate_limit_handler(request: Request, exc: RateLimitExceeded):
    observe_rate_limit_blocked(request)
    return _rate_limit_exceeded_handler(request, exc)


app.add_exception_handler(RateLimitExceeded, _custom_rate_limit_handler)

# Middleware (order matters — outermost first)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(PrometheusMiddleware)

router_manager = RouterManager(app)
router_manager.install_initial_routes()


@app.on_event("startup")
async def _startup():
    log_event(logger, "startup", service=settings.API_TITLE)
    router_manager.start_background_refresh()


@app.on_event("shutdown")
async def _shutdown():
    log_event(logger, "shutdown", service=settings.API_TITLE)
    await router_manager.stop_background_refresh()


# ---------------------------------------------------------------------------
# System endpoints
# ---------------------------------------------------------------------------

@app.get("/", tags=["System"])
def root():
    return {
        "status": "online",
        "service": settings.API_TITLE,
        "docs": "/docs",
    }


@app.get("/health", include_in_schema=False)
def health():
    try:
        ClickHouseClient.get_client()
        return JSONResponse({"status": "ok", "clickhouse_connected": True})
    except Exception as exc:
        return JSONResponse(
            {"status": "error", "clickhouse_connected": False, "error": str(exc)},
            status_code=503,
        )


@app.get("/metrics", include_in_schema=False)
def metrics():
    return metrics_response()


@app.post("/v1/system/manifest/refresh", tags=["System"])
@limiter.limit("10/minute")
async def refresh_manifest(request: Request, user_info=Depends(get_api_key)):
    check_tier_access(user_info, "tier3", "/v1/system/manifest/refresh")
    return await router_manager.refresh_async(trigger="manual")
