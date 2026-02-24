from fastapi import FastAPI, Depends

from app.config import settings
from app.router_manager import RouterManager
from app.security import get_api_key, check_tier_access

description = """
**Gnosis Cerebro API** - Data API dynamically generated from the `dbt-cerebro` manifest.
Serves data directly from ClickHouse with authentication and tier-based access control.

---

**Authentication:** Tier1+ endpoints require the header `X-API-Key: <your_key>`.
Tier0 endpoints are publicly accessible without authentication.

**Access Tiers:**

    - tier0 → Public   (20/min, no key required)
    - tier1 → Partner  (100/min)
    - tier2 → Premium  (500/min)
    - tier3 → Internal (10k/min)
"""

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=description
)

router_manager = RouterManager(app)
router_manager.install_initial_routes()


@app.on_event("startup")
async def _startup():
    router_manager.start_background_refresh()


@app.on_event("shutdown")
async def _shutdown():
    await router_manager.stop_background_refresh()

@app.get("/", tags=["System"])
def root():
    return {
        "status": "online",
        "service": settings.API_TITLE,
        "docs": "/docs"
    }


@app.post("/v1/system/manifest/refresh", tags=["System"])
async def refresh_manifest(user_info=Depends(get_api_key)):
    check_tier_access(user_info, "tier3", "/v1/system/manifest/refresh")
    return await router_manager.refresh_async()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=settings.DEBUG)
