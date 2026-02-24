import re
from typing import Dict, Any
from fastapi import Security, HTTPException, Request
from fastapi.security.api_key import APIKeyHeader
from slowapi import Limiter
from slowapi.util import get_remote_address
from app.config import settings

# Header key definition
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_rate_limit_key(request: Request) -> str:
    """
    Returns the API key as the rate limit key, falling back to IP address.
    This allows per-key rate limiting for authenticated users.
    """
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return api_key
    return get_remote_address(request)


# Rate Limiter keyed by API key (or IP if no key)
# Using in-memory storage - for production use Redis: storage_uri="redis://localhost:6379"
limiter = Limiter(
    key_func=get_rate_limit_key,
    default_limits=["200/minute"],  # Default limit for all endpoints
    storage_uri="memory://",  # Explicit in-memory storage
    strategy="fixed-window"  # Simple fixed window strategy
)

# Tier hierarchy: higher number = more access
TIER_LEVELS = {
    "tier0": 0,  # Public/Free
    "tier1": 1,  # Partner
    "tier2": 2,  # Premium
    "tier3": 3,  # Internal/Admin
}


def get_tier_level(tier: str) -> int:
    """
    Convert tier string to numeric level.
    Supports 'tier0', 'tier1', etc. format.
    Returns -1 for invalid tiers.
    """
    if tier in TIER_LEVELS:
        return TIER_LEVELS[tier]

    # Try to parse 'tierN' format
    match = re.match(r'^tier(\d+)$', tier.lower())
    if match:
        return int(match.group(1))

    return -1


def can_access_tier(user_tier: str, required_tier: str) -> bool:
    """
    Check if a user's tier grants access to an endpoint's required tier.
    Higher tier users can access lower tier endpoints.

    Examples:
        can_access_tier("tier2", "tier0") -> True  (premium can access public)
        can_access_tier("tier0", "tier2") -> False (public cannot access premium)
        can_access_tier("tier1", "tier1") -> True  (same tier)
    """
    user_level = get_tier_level(user_tier)
    required_level = get_tier_level(required_tier)

    if user_level < 0 or required_level < 0:
        return False

    return user_level >= required_level


ANONYMOUS_USER_INFO = {
    "user": "anonymous",
    "tier": "tier0",
    "org": None,
    "api_key": None,
}


async def get_optional_api_key(
    api_key_header: str = Security(api_key_header),
) -> Dict[str, Any]:
    """
    Optional API key validation for tier0 endpoints.
    - No key provided: returns anonymous user_info (tier0 access).
    - Valid key provided: validates and returns real user_info.
    - Invalid key provided: raises 403.
    """
    if not api_key_header:
        return ANONYMOUS_USER_INFO.copy()

    if api_key_header in settings.API_KEYS:
        user_info = settings.API_KEYS[api_key_header].copy()
        user_info["api_key"] = api_key_header
        return user_info

    raise HTTPException(
        status_code=403,
        detail="Invalid API Key",
    )


async def get_api_key(api_key_header: str = Security(api_key_header)) -> Dict[str, Any]:
    """
    Verifies the API Key exists in the configuration.
    Returns user info dict: {"user": str, "tier": str, "org": str|None, "api_key": str}
    """
    if not api_key_header:
        raise HTTPException(
            status_code=403,
            detail="Missing authentication header: X-API-Key"
        )

    if api_key_header in settings.API_KEYS:
        user_info = settings.API_KEYS[api_key_header].copy()
        user_info["api_key"] = api_key_header  # Include key reference for logging
        return user_info

    raise HTTPException(
        status_code=403,
        detail="Invalid API Key"
    )


def check_tier_access(user_info: Dict[str, Any], required_tier: str, endpoint_path: str):
    """
    Verify user has sufficient tier access for the endpoint.
    Raises HTTPException if access is denied.
    """
    user_tier = user_info.get("tier", "tier0")
    user_name = user_info.get("user", "anonymous")

    if not can_access_tier(user_tier, required_tier):
        raise HTTPException(
            status_code=403,
            detail=f"Access denied. This endpoint requires {required_tier} access. "
                   f"User '{user_name}' has {user_tier} access."
        )