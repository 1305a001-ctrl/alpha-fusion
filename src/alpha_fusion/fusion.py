"""Pure-function Signal → Alpha transformer.

The runtime in runtime.py reads a Signal off Redis, looks up the strategy
from cache, calls signal_to_alpha, and (if not None) publishes the Alpha.
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
    """Transform a Signal into an Alpha, OR return (None, reason) for drops.

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
