"""Main fusion loops — XREAD signals → bucketer → emit-loop → XADD alphas.

v0.2 splits the v0.1 single-pass loop into two cooperating tasks:

  ingest_loop()
      XREADGROUP signals:* → dedup on signal.id → bucketer.add()
      Per-signal drops (low_confidence / direction / strategy) happen
      here so dropped signals don't sit in buckets.

  emit_loop()
      Every bucket_emit_poll_seconds, scan bucketer for ripe buckets
      (quiet for N sec OR aged max_age sec). Aggregate to one Alpha,
      XADD to alphas:active. Pure-function aggregator in fusion.py.
"""
import asyncio
import json
from datetime import UTC, datetime

import structlog
from signals_contract.signal import Signal

from alpha_fusion import dedup, strategies
from alpha_fusion.bucketer import Bucketer
from alpha_fusion.fusion import aggregate_to_alpha
from alpha_fusion.redis_client import r
from alpha_fusion.settings import settings

log = structlog.get_logger(__name__)

# Single in-process bucketer — both loops share it.
bucketer = Bucketer()


async def _ensure_groups() -> None:
    streams = [s.strip() for s in settings.signal_streams.split(",") if s.strip()]
    for stream in streams:
        try:
            await r().xgroup_create(
                stream, settings.consumer_group, id="0", mkstream=True
            )
            log.info("xgroup.created", stream=stream, group=settings.consumer_group)
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                continue
            log.exception("xgroup.create_failed", stream=stream)


async def _publish_alpha(alpha) -> None:
    payload = alpha.model_dump_json()
    await r().xadd(
        settings.alphas_stream,
        {"data": payload},
        maxlen=settings.alphas_stream_maxlen,
        approximate=True,
    )


def _signal_passes_filters(signal: Signal) -> tuple[bool, str]:
    """Per-signal drop checks — done in ingest before bucketing."""
    if signal.confidence < settings.min_confidence:
        return False, "low_confidence"
    if signal.direction == "neutral":
        return False, "direction_neutral"
    if signal.direction == "watch":
        return False, "direction_watch"
    return True, "ok"


async def _ingest_one(stream_name: str, fields: dict) -> str:
    raw = fields.get("data") or fields.get(b"data")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if not raw:
        return "empty_payload"
    try:
        payload = json.loads(raw)
        signal = Signal.model_validate(payload)
    except Exception:
        log.exception("ingest.bad_payload", stream=stream_name, raw=raw[:200])
        return "bad_payload"

    if not await dedup.is_new(signal.id):
        log.debug(
            "ingest.duplicate", stream=stream_name, signal_id=str(signal.id)
        )
        return "duplicate_signal_id"

    ok, reason = _signal_passes_filters(signal)
    if not ok:
        log.debug(
            "ingest.dropped",
            stream=stream_name,
            signal_id=str(signal.id),
            reason=reason,
            confidence=signal.confidence,
            direction=signal.direction,
        )
        return reason

    strategy = await strategies.get(signal.strategy_id)
    if strategy is None:
        log.warning(
            "ingest.unknown_strategy",
            stream=stream_name,
            signal_id=str(signal.id),
            strategy_id=str(signal.strategy_id),
        )
        return "unknown_strategy"
    if strategy.status != "active":
        log.debug(
            "ingest.inactive_strategy",
            stream=stream_name,
            signal_id=str(signal.id),
            strategy_slug=strategy.slug,
        )
        return "inactive_strategy"

    key = bucketer.add(signal, strategy, datetime.now(UTC))
    log.info(
        "ingest.bucketed",
        signal_id=str(signal.id),
        strategy_slug=strategy.slug,
        bucket_key=f"{key[0]}/{key[1]}/{key[2]}",
    )
    return "bucketed"


async def ingest_loop() -> None:
    await _ensure_groups()
    streams = [s.strip() for s in settings.signal_streams.split(",") if s.strip()]
    log.info(
        "ingest.starting",
        streams=streams,
        group=settings.consumer_group,
        min_confidence=settings.min_confidence,
        dedup_ttl=settings.dedup_ttl_seconds,
    )

    while True:
        try:
            result = await r().xreadgroup(
                settings.consumer_group,
                settings.consumer_name,
                dict.fromkeys(streams, ">"),
                count=settings.batch_size,
                block=settings.block_ms,
            )
        except Exception:
            log.exception("ingest.xread_failed")
            await asyncio.sleep(5)
            continue

        if not result:
            continue

        for stream_name, entries in result:
            ack_ids: list[str] = []
            for entry_id, fields in entries:
                try:
                    await _ingest_one(stream_name, fields)
                    ack_ids.append(entry_id)
                except Exception:
                    log.exception(
                        "ingest.process_failed",
                        stream=stream_name,
                        entry_id=entry_id,
                    )
            if ack_ids:
                try:
                    await r().xack(stream_name, settings.consumer_group, *ack_ids)
                except Exception:
                    log.exception(
                        "ingest.ack_failed", stream=stream_name, ack_ids=ack_ids
                    )


async def emit_loop() -> None:
    log.info(
        "emit.starting",
        quiet_seconds=settings.bucket_quiet_seconds,
        max_age_seconds=settings.bucket_max_age_seconds,
        poll_seconds=settings.bucket_emit_poll_seconds,
    )
    while True:
        try:
            ripe = bucketer.ready_to_emit(
                datetime.now(UTC),
                quiet_seconds=settings.bucket_quiet_seconds,
                max_age_seconds=settings.bucket_max_age_seconds,
            )
            for bucket in ripe:
                entries = [(e.signal, e.strategy) for e in bucket.entries]
                alpha, reason = aggregate_to_alpha(entries)
                if alpha is None:
                    log.warning(
                        "emit.aggregate_returned_none",
                        bucket_key=bucket.key,
                        reason=reason,
                    )
                    continue
                await _publish_alpha(alpha)
                log.info(
                    "alpha.fused",
                    alpha_id=str(alpha.id),
                    asset=alpha.asset,
                    direction=alpha.direction,
                    confidence=alpha.confidence,
                    fused_signal_count=len(entries),
                    bucket_key=f"{bucket.key[0]}/{bucket.key[1]}/{bucket.key[2]}",
                )
        except Exception:
            log.exception("emit.tick_failed")
        await asyncio.sleep(settings.bucket_emit_poll_seconds)
