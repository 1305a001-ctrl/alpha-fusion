"""Pure-function Signal → Alpha transformers.

Two paths:
  - signal_to_alpha(): v0.1 1:1 — kept for tests + edge cases
  - aggregate_to_alpha(): v0.2 N:M — combines a list of (Signal, StrategyMeta)
                                      from a single bucket into one Alpha

No I/O here — easy to unit-test with synthesised inputs.
"""
from datetime import timedelta
from uuid import uuid4

from signals_contract.alpha import Alpha, ContributingSource
from signals_contract.signal import Signal

from alpha_fusion.normalize import canonicalize_asset
from alpha_fusion.settings import settings
from alpha_fusion.strategies import StrategyMeta


def _edge_bps_from_confidence(conf: float) -> int:
    """Linear scale conf [0,1] → edge [floor, ceiling]."""
    floor = settings.edge_bps_floor
    ceiling = settings.edge_bps_ceiling
    return int(floor + (ceiling - floor) * max(0.0, min(1.0, conf)))


def signal_to_alpha(
    signal: Signal, strategy: StrategyMeta | None
) -> tuple[Alpha | None, str]:
    """Transform a single Signal into an Alpha, OR return (None, reason).

    Drop reasons mirrored back so the runtime can log and metric them:
      - 'low_confidence': signal.confidence < settings.min_confidence
      - 'direction_neutral': not actionable
      - 'direction_watch': v0.1 doesn't fuse watch alphas
      - 'unknown_strategy': lookup failed
      - 'inactive_strategy': strategy.status != 'active'
    """
    if signal.confidence < settings.min_confidence:
        return None, "low_confidence"
    if signal.direction == "neutral":
        return None, "direction_neutral"
    if signal.direction == "watch":
        return None, "direction_watch"

    if strategy is None:
        return None, "unknown_strategy"
    if strategy.status != "active":
        return None, "inactive_strategy"

    asset = canonicalize_asset(signal.asset, strategy.asset_class)
    expires_at = signal.published_at + timedelta(
        seconds=settings.alpha_lifetime_seconds
    )

    alpha = Alpha(
        id=uuid4(),
        created_at=signal.published_at,
        expires_at=expires_at,
        asset_class=strategy.asset_class,
        asset=asset,
        direction=signal.direction,
        confidence=signal.confidence,
        edge_bps=_edge_bps_from_confidence(signal.confidence),
        reasoning=(
            f"Fused from news-consolidator signal via strategy "
            f"{strategy.slug} (conf={signal.confidence:.2f})"
        ),
        contributing_sources=[
            ContributingSource(
                source_id=f"news-consolidator:{strategy.slug}",
                source_kind="internal-strategy",
                weight=1.0,
                raw_confidence=signal.confidence,
                raw_signal_id=signal.id,
                metadata={
                    "research_config_id": str(signal.research_config_id),
                    "research_config_version": signal.research_config_version,
                    "source_article_count": len(signal.source_article_ids),
                },
            )
        ],
        metadata={
            "strategy_id": str(strategy.id),
            "strategy_slug": strategy.slug,
            "strategy_type": strategy.type,
            "strategy_bucket": strategy.bucket,
            "source_signal_id": str(signal.id),
            "source_signal_published_at": signal.published_at.isoformat(),
        },
    )
    return alpha, "fused"


def aggregate_to_alpha(
    entries: list[tuple[Signal, StrategyMeta]],
) -> tuple[Alpha | None, str]:
    """v0.2 — combine a bucket's worth of signals into one Alpha.

    All entries are expected to share the same (asset_class, canonical_asset,
    direction) — that's how the bucketer keys them.

    Aggregation rules:
        confidence  = mean of signals' confidence (simple average — each
                      signal carries its own weight already in source_kind)
        edge_bps    = max across signals (most aggressive thesis)
        reasoning   = "N signals across <slugs>" + brief
        contributing_sources = one ContributingSource per signal, weight
                               = signal.confidence / sum(confidences)
                               (so weights sum to 1.0)
        created_at  = earliest signal's published_at
        expires_at  = latest signal's published_at + lifetime
        metadata.source_signal_ids = list of all signal ids

    Returns (None, "empty_bucket") if entries is empty.
    """
    if not entries:
        return None, "empty_bucket"

    # All entries should share key (caller's responsibility); pick from first.
    first_signal, first_strategy = entries[0]
    asset = canonicalize_asset(first_signal.asset, first_strategy.asset_class)

    # confidence + edge
    confidences = [s.confidence for s, _ in entries]
    avg_conf = sum(confidences) / len(confidences)
    max_edge = _edge_bps_from_confidence(max(confidences))

    # contributing source weights — proportional to per-signal confidence
    total_conf = sum(confidences) or 1.0
    sources: list[ContributingSource] = []
    for s, st in entries:
        sources.append(
            ContributingSource(
                source_id=f"news-consolidator:{st.slug}",
                source_kind="internal-strategy",
                weight=round(s.confidence / total_conf, 4),
                raw_confidence=s.confidence,
                raw_signal_id=s.id,
                metadata={
                    "research_config_id": str(s.research_config_id),
                    "research_config_version": s.research_config_version,
                    "source_article_count": len(s.source_article_ids),
                },
            )
        )

    earliest = min(s.published_at for s, _ in entries)
    latest = max(s.published_at for s, _ in entries)

    slugs = sorted({st.slug for _, st in entries})
    reasoning = (
        f"Aggregate of {len(entries)} signal(s) across "
        f"strategy slug(s) [{', '.join(slugs)}] "
        f"on ({first_strategy.asset_class}, {asset}, {first_signal.direction}); "
        f"avg conf={avg_conf:.2f}"
    )

    alpha = Alpha(
        id=uuid4(),
        created_at=earliest,
        expires_at=latest + timedelta(seconds=settings.alpha_lifetime_seconds),
        asset_class=first_strategy.asset_class,
        asset=asset,
        direction=first_signal.direction,
        confidence=round(avg_conf, 4),
        edge_bps=max_edge,
        reasoning=reasoning,
        contributing_sources=sources,
        metadata={
            "strategy_ids": sorted({str(st.id) for _, st in entries}),
            "strategy_slugs": slugs,
            "strategy_buckets": sorted({st.bucket or "" for _, st in entries}),
            "source_signal_ids": sorted([str(s.id) for s, _ in entries]),
            "fused_signal_count": len(entries),
            "fused_window_first_at": earliest.isoformat(),
            "fused_window_last_at": latest.isoformat(),
            # Pick a canonical strategy_id for downstream sizing — first by
            # slug. Phase 2.7 might do bucket-weighted sizing; for now the
            # first-strategy bucket controls notional size in oms-gateway.
            "strategy_id": str(first_strategy.id),
            "strategy_slug": first_strategy.slug,
        },
    )
    return alpha, "fused"
