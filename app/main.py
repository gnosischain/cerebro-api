from fastapi import FastAPI

from app.config import settings
from app.factory import router as dynamic_router

description = """
**Gnosis Cerebro API** - Data API dynamically generated from the `dbt-cerebro` manifest.
Serves data directly from ClickHouse with authentication and tier-based access control.

---

**Authentication:** All endpoints require the header `X-API-Key: <your_key>`

**Access Tiers:** 

    - tier0 → Public   (20/min) 
    - tier1 → Partner  (100/min)
    - tier2 → Premium  (500/min)
    - tier3 → Internal (10k/min)
"""

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=description
)

# Register the Dynamic Router
app.include_router(dynamic_router, prefix="/v1")

@app.get("/", tags=["System"])
def root():
    return {
        "status": "online",
        "service": settings.API_TITLE,
        "docs": "/docs"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=settings.DEBUG)