from fastapi import Security, HTTPException, Request
from fastapi.security.api_key import APIKeyHeader
from slowapi import Limiter
from slowapi.util import get_remote_address
from app.config import settings

# Header key definition
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# Rate Limiter (uses in-memory storage by default)
limiter = Limiter(key_func=get_remote_address)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    """
    Verifies the API Key exists in the configuration.
    Returns the 'tier' associated with the key.
    """
    if not api_key_header:
        raise HTTPException(
            status_code=403, 
            detail="Missing authentication header: X-API-Key"
        )
    
    if api_key_header in settings.API_KEYS:
        return settings.API_KEYS[api_key_header]
    
    raise HTTPException(
        status_code=403, 
        detail="Invalid API Key"
    )

def get_limit_string(request: Request):
    """
    Determines the rate limit based on the API Key tier.
    This function is called by the @limiter decorator.
    """
    key = request.headers.get("X-API-Key")
    tier = settings.API_KEYS.get(key, "public")

    if tier == "unlimited":
        return "10000/minute"
    elif tier == "pro":
        return "500/minute"
    elif tier == "free":
        return "20/minute"
    
    # Fallback/Unauthorized limit (before 403 is thrown)
    return "5/minute"