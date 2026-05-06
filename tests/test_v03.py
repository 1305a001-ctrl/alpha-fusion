"""v0.3-specific tests: per-strategy alpha lifetime + uuid5 determinism."""
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from signals_contract.signal import Signal

from alpha_fusion.fusion import (
    _alpha_id_for_aggregate,
    _alpha_id_for_signal,
    _lifetime_seconds_for,
    aggregate_to_alpha,
    signal_to_alpha,
)
from alpha_fusion.settings import settings
from alpha_fusion.strategies import StrategyMeta, _extract_lifetime_seconds


def _signal(**overrides):
    base = {
        "id": uuid4(),
        "strategy_id": uuid4(),
        "research_config_id": uuid4(),
        "strategy_git_sha": "abc1234",
        "research_config_version": 1,
        "asset": "BTC",
        "direction": "long",
        "confidence": 0.85,
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
        "tags": ["crypto", "btc"],
        "bucket": "swing",
        "alpha_lifetime_seconds": None,
    }
    base.update(overrides)
    return StrategyMeta(**base)


# --- _extract_lifetime_seconds (frontmatter parser) -------------------------

def test_extract_lifetime_seconds_explicit():
    fm = {"time_stop_seconds": 600}
    assert _extract_lifetime_seconds(fm) == 600


def test_extract_lifetime_hours_legacy():
    """Old-style 'time_stop_hours: 48' should convert to 172800s."""
    fm = {"time_stop_hours": 48}
    assert _extract_lifetime_seconds(fm) == 48 * 3600


def test_extract_lifetime_seconds_priority_over_hours():
    """If both present, seconds wins (more precise)."""
    fm = {"time_stop_seconds": 600, "time_stop_hours": 24}
    assert _extract_lifetime_seconds(fm) == 600


def test_extract_lifetime_zero_returns_none():
    assert _extract_lifetime_seconds({"time_stop_seconds": 0}) is None
    assert _extract_lifetime_seconds({"time_stop_hours": 0}) is None


def test_extract_lifetime_negative_returns_none():
    assert _extract_lifetime_seconds({"time_stop_seconds": -10}) is None


def test_extract_lifetime_garbage_returns_none():
    assert _extract_lifetime_seconds({"time_stop_seconds": "ten"}) is None
    assert _extract_lifetime_seconds(None) is None
    assert _extract_lifetime_seconds({}) is None


# --- _lifetime_seconds_for (per-strategy override) --------------------------

def test_lifetime_for_strategy_with_override():
    s = _strategy(alpha_lifetime_seconds=900)
    assert _lifetime_seconds_for(s) == 900


def test_lifetime_for_strategy_falls_back_to_settings_default():
    s = _strategy()  # alpha_lifetime_seconds=None
    assert _lifetime_seconds_for(s) == settings.alpha_lifetime_seconds


# --- alpha id determinism ---------------------------------------------------

def test_signal_alpha_id_deterministic():
    """Same signal id → same alpha id, every call."""
    sid = uuid4()
    a = _alpha_id_for_signal(sid)
    b = _alpha_id_for_signal(sid)
    assert a == b
    assert isinstance(a, UUID)
    assert a.version == 5


def test_signal_alpha_id_distinct_per_signal():
    a = _alpha_id_for_signal(uuid4())
    b = _alpha_id_for_signal(uuid4())
    assert a != b


def test_aggregate_alpha_id_order_independent():
    """Sorting before hashing means [s1, s2] and [s2, s1] produce the same id."""
    s1, s2, s3 = uuid4(), uuid4(), uuid4()
    a = _alpha_id_for_aggregate([s1, s2, s3])
    b = _alpha_id_for_aggregate([s3, s1, s2])
    assert a == b


def test_aggregate_alpha_id_distinct_per_set():
    s1, s2 = uuid4(), uuid4()
    a = _alpha_id_for_aggregate([s1, s2])
    b = _alpha_id_for_aggregate([s1])
    assert a != b


# --- end-to-end: re-fusion of same signal collapses to same alpha id --------

def test_signal_to_alpha_id_stable_across_calls():
    sig = _signal()
    s = _strategy()
    a, _ = signal_to_alpha(sig, s)
    b, _ = signal_to_alpha(sig, s)
    assert a is not None and b is not None
    assert a.id == b.id  # core dedup property


def test_aggregate_id_stable_across_re_fusions():
    sig1, sig2 = _signal(), _signal()
    s = _strategy()
    a, _ = aggregate_to_alpha([(sig1, s), (sig2, s)])
    b, _ = aggregate_to_alpha([(sig2, s), (sig1, s)])  # reordered
    assert a is not None and b is not None
    assert a.id == b.id


def test_signal_to_alpha_uses_strategy_lifetime():
    """An override of 600s should produce expires_at = published_at + 10min."""
    sig = _signal()
    s = _strategy(alpha_lifetime_seconds=600)
    a, _ = signal_to_alpha(sig, s)
    assert a is not None
    delta = a.expires_at - sig.published_at
    assert delta == timedelta(seconds=600)


def test_aggregate_uses_shortest_lifetime():
    """Two strategies with different lifetimes — aggregate uses the shorter."""
    sig1, sig2 = _signal(), _signal()
    swing = _strategy(slug="swing-strat", alpha_lifetime_seconds=3600)
    fast = _strategy(slug="fast-strat", alpha_lifetime_seconds=120)
    a, _ = aggregate_to_alpha([(sig1, swing), (sig2, fast)])
    assert a is not None
    delta = a.expires_at - max(sig1.published_at, sig2.published_at)
    assert delta == timedelta(seconds=120)


def test_aggregate_falls_back_to_default_when_no_overrides():
    sig = _signal()
    s = _strategy()  # no override
    a, _ = aggregate_to_alpha([(sig, s)])
    assert a is not None
    delta = a.expires_at - sig.published_at
    assert delta == timedelta(seconds=settings.alpha_lifetime_seconds)
