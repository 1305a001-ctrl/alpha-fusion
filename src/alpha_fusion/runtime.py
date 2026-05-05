"""Main fusion loop — XREAD signals:* → fuse → XADD alphas:active.

Uses XREADGROUP for at-least-once delivery + ack-on-success. If we crash
between fuse and XADD, the message is redelivered next round and re-fused
(producing a different Alpha.id, different created_at — small dup risk
that's acceptable; downstream oms-gateway dedups via idempotency_key).
"""
import asyncio
import json

import structlog
from signals_contract.signal import Signal

from alpha_fusion import strategies
from alpha_fusion.fusion import signal_to_alpha
from alpha_fusion.redis_client import r
from alpha_fusion.settings import settings

log = structlog.get_logger(__name__)


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


async def _process_one(stream_name: str, fields: dict) -> str:
    """Returns reason ('fused' on success, drop-reason otherwise)."""
    raw = fields.get("data") or fields.get(b"data")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if not raw:
        return "empty_payload"
    try:
        payload = json.loads(raw)
        signal = Signal.model_validate(payload)
    except Exception:
        log.exception("fusion.bad_payload", stream=stream_name, raw=raw[:200])
        return "bad_payload"

    strategy = await strategies.get(signal.strategy_id)
    alpha, reason = signal_to_alpha(signal, strategy)
    if alpha is None:
        log.debug(
            "fusion.dropped",
            stream=stream_name,
            signal_id=str(signal.id),
            strategy_id=str(signal.strategy_id),
            reason=reason,
            confidence=signal.confidence,
            direction=signal.direction,
        )
        return reason

    await _publish_alpha(alpha)
    log.info(
        "alpha.fused",
        stream=stream_name,
        alpha_id=str(alpha.id),
        signal_id=str(signal.id),
        strategy_slug=strategy.slug,
        asset=alpha.asset,
        direction=alpha.direction,
        confidence=alpha.confidence,
    )
    return "fused"


async def loop() -> None:
    await _ensure_groups()
    streams = [s.strip() for s in settings.signal_streams.split(",") if s.strip()]
    log.info(
        "fusion.starting",
        streams=streams,
        group=settings.consumer_group,
        alphas_stream=settings.alphas_stream,
        min_confidence=settings.min_confidence,
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
            log.exception("fusion.xread_failed")
            await asyncio.sleep(5)
            continue

        if not result:
            continue

        for stream_name, entries in result:
            ack_ids: list[str] = []
            for entry_id, fields in entries:
                try:
                    await _process_one(stream_name, fields)
                    ack_ids.append(entry_id)
                except Exception:
                    log.exception(
                        "fusion.process_failed",
                        stream=stream_name,
                        entry_id=entry_id,
                    )
                    # leave un-acked → redelivered next round
            if ack_ids:
                try:
                    await r().xack(stream_name, settings.consumer_group, *ack_ids)
                except Exception:
                    log.exception(
                        "fusion.ack_failed", stream=stream_name, ack_ids=ack_ids
                    )
