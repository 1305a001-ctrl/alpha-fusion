"""Redis-backed dedup on signal.id.

SET <key> 1 NX EX <ttl> — atomic; returns OK if new, nil if existing.
Survives alpha-fusion restarts (TTL keeps memory bounded).
"""
from uuid import UUID

import structlog

from alpha_fusion.redis_client import r
from alpha_fusion.settings import settings

log = structlog.get_logger(__name__)

_KEY_PREFIX = "alpha-fusion:seen:"


async def is_new(signal_id: UUID) -> bool:
    """Returns True if this signal_id is new (and registers it),
    False if it was already seen within the TTL window."""
    key = f"{_KEY_PREFIX}{signal_id}"
    try:
        result = await r().set(key, "1", nx=True, ex=settings.dedup_ttl_seconds)
    except Exception:
        # On redis hiccup, fall back to "treat as new" — at-least-once is
        # better than dropping legitimate signals. The downstream
        # idempotency_key keeps oms-gateway honest anyway.
        log.exception("dedup.redis_failed_falling_back", signal_id=str(signal_id))
        return True
    return result is True or result == "OK" or result == b"OK" or result == 1
