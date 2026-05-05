"""Pure-function tests for v0.2 aggregate_to_alpha."""
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from signals_contract.signal import Signal

from alpha_fusion.fusion import aggregate_to_alpha
from alpha_fusion.strategies import StrategyMeta


def _strategy(slug: str = "btc-momentum") -> StrategyMeta:
    return StrategyMeta(
        id=uuid4(),
        slug=slug,
        type="trading",
        status="active",
        asset_class="crypto",
        tags=["crypto", "btc"],
        bucket="swing",
    )


def _signal(
    confidence: float = 0.8,
    direction: str = "long",
    asset: str = "BTC",
    published_at: datetime | None = None,
) -> Signal:
    return Signal(
        id=uuid4(),
        strategy_id=uuid4(),
        research_config_id=uuid4(),
        strategy_git_sha="abc",
        research_config_version=1,
        asset=asset,
        direction=direction,
        confidence=confidence,
        published_at=published_at or datetime.now(UTC),
    )


def test_empty_returns_none():
    alpha, reason = aggregate_to_alpha([])
    assert alpha is None
    assert reason == "empty_bucket"


def test_single_signal_aggregates_correctly():
    s = _signal(confidence=0.8)
    st = _strategy()
    alpha, _ = aggregate_to_alpha([(s, st)])
    assert alpha is not None
    assert alpha.confidence == 0.8
    assert alpha.metadata["fused_signal_count"] == 1


def test_two_signals_average_confidence():
    s1 = _signal(confidence=0.6)
    s2 = _signal(confidence=0.9)
    st = _strategy()
    alpha, _ = aggregate_to_alpha([(s1, st), (s2, st)])
    assert alpha is not None
    assert alpha.confidence == 0.75  # avg


def test_contributing_source_weights_sum_to_one():
    s1 = _signal(confidence=0.6)
    s2 = _signal(confidence=0.9)
    s3 = _signal(confidence=0.7)
    st = _strategy()
    alpha, _ = aggregate_to_alpha([(s1, st), (s2, st), (s3, st)])
    assert alpha is not None
    total_weight = sum(cs.weight for cs in alpha.contributing_sources)
    assert abs(total_weight - 1.0) < 1e-3


def test_higher_confidence_gets_higher_weight():
    s_low = _signal(confidence=0.6)
    s_high = _signal(confidence=0.9)
    st = _strategy()
    alpha, _ = aggregate_to_alpha([(s_low, st), (s_high, st)])
    assert alpha is not None
    weights = {cs.raw_signal_id: cs.weight for cs in alpha.contributing_sources}
    assert weights[s_high.id] > weights[s_low.id]


def test_metadata_carries_all_signal_ids():
    s1, s2, s3 = _signal(), _signal(), _signal()
    st = _strategy()
    alpha, _ = aggregate_to_alpha([(s1, st), (s2, st), (s3, st)])
    assert alpha is not None
    ids = alpha.metadata["source_signal_ids"]
    assert len(ids) == 3
    assert set(ids) == {str(s1.id), str(s2.id), str(s3.id)}


def test_metadata_collects_distinct_strategy_slugs():
    s1, s2 = _signal(), _signal()
    st_a = _strategy(slug="btc-momentum")
    st_b = _strategy(slug="btc-breakout")
    alpha, _ = aggregate_to_alpha([(s1, st_a), (s2, st_b)])
    assert alpha is not None
    assert set(alpha.metadata["strategy_slugs"]) == {"btc-momentum", "btc-breakout"}


def test_created_at_is_earliest_published():
    base = datetime.now(UTC)
    s_early = _signal(published_at=base)
    s_late = _signal(published_at=base + timedelta(seconds=10))
    st = _strategy()
    alpha, _ = aggregate_to_alpha([(s_late, st), (s_early, st)])
    assert alpha is not None
    assert alpha.created_at == base


def test_canonicalises_asset_to_btc_usdt():
    s = _signal(asset="BTC")
    st = _strategy()  # asset_class=crypto
    alpha, _ = aggregate_to_alpha([(s, st)])
    assert alpha is not None
    assert alpha.asset == "BTC-USDT"


def test_edge_bps_uses_max_confidence():
    """Most aggressive signal sets edge_bps."""
    s_low = _signal(confidence=0.6)
    s_high = _signal(confidence=0.95)
    st = _strategy()
    alpha_only_low, _ = aggregate_to_alpha([(s_low, st)])
    alpha_with_high, _ = aggregate_to_alpha([(s_low, st), (s_high, st)])
    assert alpha_only_low and alpha_with_high
    assert alpha_with_high.edge_bps > alpha_only_low.edge_bps


def test_reasoning_mentions_signal_count():
    s1, s2, s3 = _signal(), _signal(), _signal()
    st = _strategy()
    alpha, _ = aggregate_to_alpha([(s1, st), (s2, st), (s3, st)])
    assert alpha is not None
    assert "3 signal" in alpha.reasoning.lower()
