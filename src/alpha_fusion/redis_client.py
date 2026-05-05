"""Async Redis client."""
import redis.asyncio as aioredis

from alpha_fusion.settings import settings

_redis: aioredis.Redis | None = None


def r() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    return _redis


async def close() -> None:
    global _redis
    if _redis is not None:
        await _redis.close()
        _redis = None
