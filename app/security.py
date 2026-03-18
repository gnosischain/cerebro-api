import logging
import re
from typing import Any, Dict

from fastapi import HTTPException, Request, Security
from fastapi.security.api_key import APIKeyHeader
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.config import settings
from app.observability import (
    log_event,
    observe_access_denied,
    observe_auth_resolution,
    observe_rate_limit,
)

logger = logging.getLogger("cerebro_api.auth")

# ---------------------------------------------------------------------------
# Header key definition
# ---------------------------------------------------------------------------
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


# ---------------------------------------------------------------------------
# Rate limiter (per-pod in-memory)
# ---------------------------------------------------------------------------
def get_rate_limit_key(request: Request) -> str:
    """Return the API key as rate-limit key, falling back to client IP."""
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return api_key
    return get_remote_address(request)


limiter = Limiter(
    key_func=get_rate_limit_key,
    default_limits=["200/minute"],
    storage_uri="memory://",
    strategy="fixed-window",
)


# ---------------------------------------------------------------------------
# Tier helpers
# ---------------------------------------------------------------------------
TIER_LEVELS = {
    "tier0": 0,
    "tier1": 1,
    "tier2": 2,
    "tier3": 3,
}

ANONYMOUS_USER_INFO: Dict[str, Any] = {
    "user": "anonymous",
    "tier": "tier0",
    "org": None,
    "api_key": None,
}


def get_tier_level(tier: str) -> int:
    if tier in TIER_LEVELS:
        return TIER_LEVELS[tier]
    match = re.match(r"^tier(\d+)$", tier.lower())
    if match:
        return int(match.group(1))
    return -1


def can_access_tier(user_tier: str, required_tier: str) -> bool:
    user_level = get_tier_level(user_tier)
    required_level = get_tier_level(required_tier)
    if user_level < 0 or required_level < 0:
        return False
    return user_level >= required_level


# ---------------------------------------------------------------------------
# Stage 1: resolve request identity (non-raising)
# ---------------------------------------------------------------------------
async def resolve_request_identity(
    request: Request,
    api_key_value: str = Security(api_key_header),
) -> Dict[str, Any]:
    """Classify the caller. Always sets request.state fields; never raises."""
    presented = bool(api_key_value)
    valid = False
    auth_result = "anonymous"
    user_info = ANONYMOUS_USER_INFO.copy()
    identity_kind = "ip"

    if presented:
        if api_key_value in settings.API_KEYS:
            valid = True
            auth_result = "valid"
            user_info = settings.API_KEYS[api_key_value].copy()
            user_info["api_key"] = api_key_value
            identity_kind = "api_key"
        else:
            auth_result = "invalid"
            identity_kind = "api_key"

    rate_limit_key = api_key_value if presented else get_remote_address(request)

    request.state.presented = presented
    request.state.valid = valid
    request.state.auth_result = auth_result
    request.state.user_tier = user_info.get("tier", "tier0")
    request.state.identity_kind = identity_kind
    request.state.rate_limit_key = rate_limit_key
    request.state.user_info = user_info

    return user_info


# ---------------------------------------------------------------------------
# Stage 2: enforce access (raises 403 when needed)
# ---------------------------------------------------------------------------
def enforce_access(required_tier: str):
    """Return a dependency that enforces tier access based on resolved identity."""

    async def _enforce(request: Request) -> Dict[str, Any]:
        user_info = getattr(request.state, "user_info", ANONYMOUS_USER_INFO)
        auth_result = getattr(request.state, "auth_result", "anonymous")
        user_tier = getattr(request.state, "user_tier", "tier0")
        identity_kind = getattr(request.state, "identity_kind", "ip")

        # Observe the resolution
        observe_auth_resolution(required_tier, auth_result)

        # For tier0 endpoints, anonymous access is allowed
        if required_tier == "tier0" and auth_result == "anonymous":
            return user_info

        # Invalid key: always reject
        if auth_result == "invalid":
            observe_access_denied(required_tier, "unknown", "invalid")
            log_event(
                logger, "api_access_denied",
                level=logging.WARNING,
                required_tier=required_tier,
                reason="invalid",
                identity_kind=identity_kind,
                success=False,
            )
            raise HTTPException(status_code=403, detail="Invalid API Key")

        # Missing key on protected endpoint
        if auth_result == "anonymous" and required_tier != "tier0":
            observe_access_denied(required_tier, "none", "missing")
            log_event(
                logger, "api_access_denied",
                level=logging.WARNING,
                required_tier=required_tier,
                reason="missing",
                identity_kind=identity_kind,
                success=False,
            )
            raise HTTPException(
                status_code=403,
                detail="Missing authentication header: X-API-Key",
            )

        # Valid key but insufficient tier
        if not can_access_tier(user_tier, required_tier):
            observe_access_denied(required_tier, user_tier, "insufficient")
            log_event(
                logger, "api_access_denied",
                level=logging.WARNING,
                required_tier=required_tier,
                provided_tier=user_tier,
                reason="insufficient",
                identity_kind=identity_kind,
                success=False,
            )
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Access denied. This endpoint requires {required_tier} access. "
                    f"User '{user_info.get('user', 'anonymous')}' has {user_tier} access."
                ),
            )

        return user_info

    return _enforce


# ---------------------------------------------------------------------------
# Rate-limit helpers
# ---------------------------------------------------------------------------
def get_tier_rate_limit(key: str) -> str:
    """Dynamic limit provider for SlowAPI.

    SlowAPI calls this with the result of the key_func (API key string or IP).
    We look up the tier from the API key to determine the rate limit.
    """
    # If the key is a known API key, use its tier; otherwise default to tier0
    user_info = settings.API_KEYS.get(key)
    if user_info:
        tier = user_info.get("tier", "tier0")
    else:
        tier = "tier0"
    rpm = settings.TIER_RATE_LIMITS.get(tier, settings.TIER_RATE_LIMITS.get("tier0", 20))
    return f"{rpm}/minute"


def observe_rate_limit_allowed(request: Request) -> None:
    """Call after a successful (non-429) request to record an allowed decision."""
    tier = getattr(request.state, "user_tier", "tier0")
    identity_kind = getattr(request.state, "identity_kind", "ip")
    observe_rate_limit(tier, "allowed", identity_kind)


def observe_rate_limit_blocked(request: Request) -> None:
    """Call from the 429 handler to record a blocked decision."""
    tier = getattr(request.state, "user_tier", "tier0")
    identity_kind = getattr(request.state, "identity_kind", "ip")
    observe_rate_limit(tier, "blocked", identity_kind)
    log_event(
        logger, "api_rate_limit",
        level=logging.WARNING,
        tier=tier,
        identity_kind=identity_kind,
        success=False,
    )


# ---------------------------------------------------------------------------
# Legacy compatibility shims (used by main.py for manifest refresh)
# ---------------------------------------------------------------------------
async def get_api_key(api_key_value: str = Security(api_key_header)) -> Dict[str, Any]:
    """Kept for /v1/system/manifest/refresh which uses the old pattern."""
    if not api_key_value:
        raise HTTPException(
            status_code=403,
            detail="Missing authentication header: X-API-Key",
        )
    if api_key_value in settings.API_KEYS:
        user_info = settings.API_KEYS[api_key_value].copy()
        user_info["api_key"] = api_key_value
        return user_info
    raise HTTPException(status_code=403, detail="Invalid API Key")


def check_tier_access(
    user_info: Dict[str, Any], required_tier: str, endpoint_path: str,
) -> None:
    """Kept for /v1/system/manifest/refresh which uses the old pattern."""
    user_tier = user_info.get("tier", "tier0")
    if not can_access_tier(user_tier, required_tier):
        raise HTTPException(
            status_code=403,
            detail=(
                f"Access denied. This endpoint requires {required_tier} access. "
                f"User '{user_info.get('user', 'anonymous')}' has {user_tier} access."
            ),
        )
