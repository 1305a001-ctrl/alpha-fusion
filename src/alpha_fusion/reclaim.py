"""Pending-entries reclaim — Phase 2.6 v0.4.

Closes a real correctness gap: when alpha-fusion crashes mid-process,
entries claimed by the dying consumer end up in the consumer-group PEL
(Pending Entries List) indefinitely. The next-restart consumer (with a
different name OR the same name but new connection) reads only `>` (new
entries), never these orphans.

This module periodically XAUTOCLAIMs entries that have sat in PEL longer
than `min_idle_ms` and processes them through the same ingest path.
Poison-message guard: if delivery count exceeds `poison_threshold`, the
entry is XACKed + logged loudly so it doesn't churn forever.

Pure separation: I/O lives in `runtime.py`; this module just owns the
reclaim loop logic.
"""
import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from alpha_fusion.redis_client import r
from alpha_fusion.settings import settings

log = structlog.get_logger(__name__)


async def _delivery_count(stream: str, group: str, entry_id: str) -> int:
    """XPENDING for a single entry. Returns the delivery count or 0 on miss."""
    try:
        details = await r().xpending_range(
            stream, group, idle=0, min=entry_id, max=entry_id, count=1,
        )
    except Exception:
        log.exception("reclaim.xpending_failed", entry_id=entry_id)
        return 0
    if not details:
        return 0
    # redis-py returns dicts with keys: message_id, consumer,
    # time_since_delivered, times_delivered.
    first = details[0]
    return int(first.get("times_delivered") or 0)


async def reclaim_once(
    *,
    stream: str,
    process: Callable[[str, dict[str, Any]], Awaitable[str]],
) -> tuple[int, int, int]:
    """One pass over pending-entry orphans on a stream.

    Returns: (reclaimed, processed_ok, dropped_poison).
    """
    cursor = "0-0"
    reclaimed = 0
    processed_ok = 0
    dropped = 0
    while True:
        try:
            cursor, entries, _deleted = await r().xautoclaim(
                name=stream,
                groupname=settings.consumer_group,
                consumername=settings.consumer_name,
                min_idle_time=settings.pending_claim_min_idle_ms,
                start_id=cursor,
                count=settings.pending_claim_batch_size,
            )
        except Exception:
            log.exception("reclaim.xautoclaim_failed", stream=stream)
            break
        if not entries:
            break
        for entry_id, fields in entries:
            reclaimed += 1
            deliveries = await _delivery_count(stream, settings.consumer_group, entry_id)
            if deliveries > settings.poison_threshold_deliveries:
                # poison — ack and drop, don't re-process forever
                try:
                    await r().xack(stream, settings.consumer_group, entry_id)
                    dropped += 1
                    log.warning(
                        "reclaim.poison_dropped",
                        stream=stream,
                        entry_id=entry_id,
                        deliveries=deliveries,
                        threshold=settings.poison_threshold_deliveries,
                    )
                except Exception:
                    log.exception(
                        "reclaim.poison_ack_failed", stream=stream, entry_id=entry_id,
                    )
                continue

            try:
                outcome = await process(stream, fields)
                # Even on logical drops (low_confidence, bad_payload, duplicate)
                # we want to ack — the consumer-group has the entry; we've
                # decided what to do.
                await r().xack(stream, settings.consumer_group, entry_id)
                processed_ok += 1
                log.info(
                    "reclaim.processed",
                    stream=stream,
                    entry_id=entry_id,
                    deliveries=deliveries,
                    outcome=outcome,
                )
            except Exception:
                # Leave un-acked → caught next reclaim loop with bumped delivery count
                log.exception(
                    "reclaim.process_failed", stream=stream, entry_id=entry_id,
                )
        # cursor "0-0" indicates the stream has been fully scanned
        if cursor in ("0-0", "0", b"0-0", b"0"):
            break
    return reclaimed, processed_ok, dropped


async def loop(
    *,
    process: Callable[[str, dict[str, Any]], Awaitable[str]],
) -> None:
    """Periodic reclaim across all configured signal streams."""
    streams = [s.strip() for s in settings.signal_streams.split(",") if s.strip()]
    log.info(
        "reclaim.starting",
        streams=streams,
        interval_sec=settings.pending_claim_interval_sec,
        min_idle_ms=settings.pending_claim_min_idle_ms,
        poison_threshold=settings.poison_threshold_deliveries,
    )
    while True:
        try:
            for stream in streams:
                reclaimed, ok, dropped = await reclaim_once(stream=stream, process=process)
                if reclaimed > 0 or dropped > 0:
                    log.info(
                        "reclaim.cycle",
                        stream=stream,
                        reclaimed=reclaimed,
                        processed_ok=ok,
                        dropped_poison=dropped,
                    )
        except Exception:
            log.exception("reclaim.loop_iteration_failed")
        await asyncio.sleep(settings.pending_claim_interval_sec)
