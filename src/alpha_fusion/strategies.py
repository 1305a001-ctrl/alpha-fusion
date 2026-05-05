"""In-memory cache of strategies table — refreshed every N seconds.

We need (id → slug, asset_class, type, tags) to hydrate Alpha metadata
without a DB hit per signal. New strategies get picked up at the next
refresh tick.
"""
import asyncio
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import structlog

from alpha_fusion.db import db
from alpha_fusion.normalize import AssetClass, infer_asset_class
from alpha_fusion.settings import settings

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class StrategyMeta:
    id: UUID
    slug: str
    type: str  # 'news' | 'trading' | 'poly'
    status: str  # 'active' | 'inactive' | 'draft'
    asset_class: AssetClass
    tags: list[str]
    bucket: str | None  # fast-intraday | swing | conviction | poly-bet | hedge | None


_cache: dict[UUID, StrategyMeta] = {}
_cache_lock = asyncio.Lock()


def _build_meta(row: dict[str, Any], signal_asset_hint: str = "") -> StrategyMeta:
    fm = row.get("frontmatter") or {}
    tags = fm.get("tags") or []
    bucket = fm.get("bucket")
    asset_class = infer_asset_class(
        strategy_tags=tags, signal_asset=signal_asset_hint
    )
    return StrategyMeta(
        id=row["id"],
        slug=row["slug"],
        type=row["type"],
        status=row["status"],
        asset_class=asset_class,
        tags=list(tags),
        bucket=bucket,
    )


async def refresh_cache() -> int:
    """Pull all strategies into the cache. Returns count."""
    rows = await db.pool.fetch(
        "SELECT id, slug, type, status, frontmatter FROM strategies"
    )
    new_cache: dict[UUID, StrategyMeta] = {}
    for row in rows:
        new_cache[row["id"]] = _build_meta(dict(row))
    async with _cache_lock:
        _cache.clear()
        _cache.update(new_cache)
    log.info("strategies.refreshed", count=len(new_cache))
    return len(new_cache)


async def get(strategy_id: UUID) -> StrategyMeta | None:
    """Look up a strategy. Falls through a single-shot DB query if cache miss."""
    if strategy_id in _cache:
        return _cache[strategy_id]

    row = await db.pool.fetchrow(
        "SELECT id, slug, type, status, frontmatter FROM strategies WHERE id = $1",
        strategy_id,
    )
    if row is None:
        return None
    meta = _build_meta(dict(row))
    async with _cache_lock:
        _cache[strategy_id] = meta
    return meta


async def refresh_loop() -> None:
    """Periodic background task — keeps the cache fresh."""
    while True:
        try:
            await refresh_cache()
        except Exception:
            log.exception("strategies.refresh_failed")
        await asyncio.sleep(settings.strategy_refresh_seconds)
