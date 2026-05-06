"""In-memory cache of strategies table — refreshed every N seconds.

We need (id → slug, asset_class, type, tags) to hydrate Alpha metadata
without a DB hit per signal. New strategies get picked up at the next
refresh tick.
"""
import asyncio
import json
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
    # v0.3: per-strategy alpha lifetime in seconds. Pulled from
    # frontmatter.trading.time_stop_seconds OR frontmatter.trading.time_stop_hours*3600.
    # None → fall through to settings.alpha_lifetime_seconds default.
    alpha_lifetime_seconds: int | None = None


_cache: dict[UUID, StrategyMeta] = {}
_cache_lock = asyncio.Lock()


def _extract_lifetime_seconds(trading_block: dict[str, Any] | None) -> int | None:
    """Pure: pull alpha lifetime from a strategy's `trading` frontmatter block.

    Accepts either:
      - trading.time_stop_seconds (preferred, used by Phase A1/A3 momentum)
      - trading.time_stop_hours   (legacy, multiply by 3600)
    Returns None if neither set / malformed.
    """
    if not isinstance(trading_block, dict):
        return None
    secs = trading_block.get("time_stop_seconds")
    if isinstance(secs, int) and secs > 0:
        return secs
    if isinstance(secs, float) and secs > 0:
        return int(secs)
    hours = trading_block.get("time_stop_hours")
    if isinstance(hours, int | float) and hours > 0:
        return int(hours * 3600)
    return None


def _build_meta(row: dict[str, Any], signal_asset_hint: str = "") -> StrategyMeta:
    fm_raw = row.get("frontmatter") or {}
    # asyncpg returns jsonb as a string by default — decode if so.
    fm = json.loads(fm_raw) if isinstance(fm_raw, str) else fm_raw
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
        alpha_lifetime_seconds=_extract_lifetime_seconds(fm.get("trading")),
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
