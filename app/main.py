from fastapi import FastAPI
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.config import settings
from app.security import limiter
from app.factory import router as dynamic_router

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description="""
    ## Gnosis Cerebro API
    
    This API is dynamically generated from the `dbt-cerebro` manifest.
    It serves data directly from ClickHouse with rate limiting and authentication.
    
    ### Authentication
    All endpoints require the header:
    `X-API-Key: <your_key>`
    """
)

# Register Rate Limiter Exception Handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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