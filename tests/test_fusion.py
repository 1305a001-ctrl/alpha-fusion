"""Pure tests for signal_to_alpha — no DB, no redis."""
from datetime import UTC, datetime
from uuid import uuid4

from signals_contract.signal import Signal

from alpha_fusion.fusion import signal_to_alpha
from alpha_fusion.strategies import StrategyMeta


def _signal(**overrides) -> Signal:
    base = {
        "id": uuid4(),
        "strategy_id": uuid4(),
        "research_config_id": uuid4(),
        "strategy_git_sha": "abc1234",
        "research_config_version": 1,
        "asset": "BTC",
        "direction": "long",
        "confidence": 0.8,
        "composite_risk_score": None,
        "risk_score": None,
        "source_article_ids": [],
        "payload": {},
        "published_at": datetime.now(UTC),
    }
    base.update(overrides)
    return Signal(**base)


def _strategy(**overrides) -> StrategyMeta:
    base = {
        "id": uuid4(),
        "slug": "btc-momentum",
        "type": "trading",
        "status": "active",
        "asset_class": "crypto",
        "tags": ["crypto", "btc", "momentum"],
        "bucket": "swing",
    }
    base.update(overrides)
    return StrategyMeta(**base)


def test_high_conf_long_signal_fuses():
    alpha, reason = signal_to_alpha(_signal(), _strategy())
    assert alpha is not None
    assert reason == "fused"
    assert alpha.direction == "long"
    assert alpha.asset == "BTC-USDT"
    assert alpha.confidence == 0.8
    assert alpha.asset_class == "crypto"


def test_low_confidence_dropped():
    alpha, reason = signal_to_alpha(_signal(confidence=0.3), _strategy())
    assert alpha is None
    assert reason == "low_confidence"


def test_neutral_direction_dropped():
    alpha, reason = signal_to_alpha(_signal(direction="neutral"), _strategy())
    assert alpha is None
    assert reason == "direction_neutral"


def test_watch_direction_dropped():
    alpha, reason = signal_to_alpha(_signal(direction="watch"), _strategy())
    assert alpha is None
    assert reason == "direction_watch"


def test_unknown_strategy_dropped():
    alpha, reason = signal_to_alpha(_signal(), None)
    assert alpha is None
    assert reason == "unknown_strategy"


def test_inactive_strategy_dropped():
    alpha, reason = signal_to_alpha(_signal(), _strategy(status="draft"))
    assert alpha is None
    assert reason == "inactive_strategy"


def test_alpha_metadata_includes_signal_provenance():
    sig = _signal()
    strat = _strategy()
    alpha, _ = signal_to_alpha(sig, strat)
    assert alpha is not None
    assert alpha.metadata["strategy_slug"] == "btc-momentum"
    assert alpha.metadata["strategy_id"] == str(strat.id)
    assert alpha.metadata["source_signal_id"] == str(sig.id)
    assert alpha.metadata["strategy_bucket"] == "swing"


def test_contributing_source_carries_raw_signal_id():
    sig = _signal()
    alpha, _ = signal_to_alpha(sig, _strategy())
    assert alpha is not None
    assert len(alpha.contributing_sources) == 1
    cs = alpha.contributing_sources[0]
    assert cs.raw_signal_id == sig.id
    assert cs.source_kind == "internal-strategy"
    assert cs.weight == 1.0


def test_short_direction_passthrough():
    alpha, _ = signal_to_alpha(_signal(direction="short"), _strategy())
    assert alpha is not None
    assert alpha.direction == "short"


def test_stocks_strategy_keeps_ticker_unchanged():
    alpha, _ = signal_to_alpha(
        _signal(asset="NVDA"),
        _strategy(slug="nvda-sentiment", asset_class="stocks", tags=["stocks"]),
    )
    assert alpha is not None
    assert alpha.asset == "NVDA"
    assert alpha.asset_class == "stocks"


def test_edge_bps_scales_with_confidence():
    """0.6 conf and 0.95 conf should produce different edge_bps."""
    a_low, _ = signal_to_alpha(_signal(confidence=0.6), _strategy())
    a_high, _ = signal_to_alpha(_signal(confidence=0.95), _strategy())
    assert a_low and a_high
    assert a_high.edge_bps > a_low.edge_bps


def test_alpha_expires_after_lifetime():
    """expires_at must be after published_at."""
    sig = _signal()
    alpha, _ = signal_to_alpha(sig, _strategy())
    assert alpha is not None
    assert alpha.expires_at > sig.published_at
