"""Pure-function tests for the v0.2 Bucketer (debounce + max-age)."""
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from signals_contract.signal import Signal

from alpha_fusion.bucketer import Bucketer, key_for
from alpha_fusion.strategies import StrategyMeta


def _strategy(slug: str = "btc-momentum", asset_class: str = "crypto") -> StrategyMeta:
    return StrategyMeta(
        id=uuid4(),
        slug=slug,
        type="trading",
        status="active",
        asset_class=asset_class,
        tags=[asset_class],
        bucket="swing",
    )


def _signal(asset: str = "BTC", direction: str = "long", confidence: float = 0.8) -> Signal:
    return Signal(
        id=uuid4(),
        strategy_id=uuid4(),
        research_config_id=uuid4(),
        strategy_git_sha="abc",
        research_config_version=1,
        asset=asset,
        direction=direction,
        confidence=confidence,
        published_at=datetime.now(UTC),
    )


def test_key_groups_same_asset_direction():
    s1 = _signal("BTC", "long")
    s2 = _signal("BTC", "long")  # different signal id, same asset/direction
    st = _strategy()
    assert key_for(s1, st) == key_for(s2, st)


def test_key_separates_by_direction():
    s_long = _signal("BTC", "long")
    s_short = _signal("BTC", "short")
    st = _strategy()
    assert key_for(s_long, st) != key_for(s_short, st)


def test_key_separates_by_asset():
    s_btc = _signal("BTC", "long")
    s_eth = _signal("ETH", "long")
    st = _strategy()
    assert key_for(s_btc, st) != key_for(s_eth, st)


def test_add_creates_bucket():
    b = Bucketer()
    assert b.size == 0
    b.add(_signal(), _strategy(), datetime.now(UTC))
    assert b.size == 1


def test_add_same_key_appends_not_duplicates_bucket():
    b = Bucketer()
    st = _strategy()
    b.add(_signal("BTC"), st, datetime.now(UTC))
    b.add(_signal("BTC"), st, datetime.now(UTC))
    assert b.size == 1
    assert len(b.all_buckets()[0].entries) == 2


def test_add_idempotent_on_signal_id():
    b = Bucketer()
    s = _signal()
    st = _strategy()
    now = datetime.now(UTC)
    b.add(s, st, now)
    b.add(s, st, now)  # same signal.id — no-op
    assert len(b.all_buckets()[0].entries) == 1


def test_ready_emits_after_quiet_window():
    b = Bucketer()
    t0 = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
    b.add(_signal(), _strategy(), t0)
    # Not ripe yet (quiet for 0s)
    assert b.ready_to_emit(t0, quiet_seconds=5, max_age_seconds=30) == []
    # Ripe after 5s of quiet
    ripe = b.ready_to_emit(
        t0 + timedelta(seconds=5), quiet_seconds=5, max_age_seconds=30
    )
    assert len(ripe) == 1


def test_ready_force_emits_at_max_age_even_with_recent_activity():
    b = Bucketer()
    t0 = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
    st = _strategy()
    # Add a signal at t0
    b.add(_signal(), st, t0)
    # Add another every 2s for 30s — bucket never goes "quiet"
    for offset in [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28]:
        b.add(_signal(), st, t0 + timedelta(seconds=offset))
    # At t=30s, bucket has been alive 30s — should force-emit
    ripe = b.ready_to_emit(
        t0 + timedelta(seconds=30), quiet_seconds=5, max_age_seconds=30
    )
    assert len(ripe) == 1


def test_emit_pops_bucket_from_state():
    b = Bucketer()
    t0 = datetime.now(UTC)
    b.add(_signal(), _strategy(), t0)
    b.ready_to_emit(
        t0 + timedelta(seconds=10), quiet_seconds=5, max_age_seconds=30
    )
    assert b.size == 0


def test_multiple_buckets_emit_independently():
    b = Bucketer()
    t0 = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
    # BTC long bucket — quiet
    b.add(_signal("BTC", "long"), _strategy(), t0)
    # ETH long bucket — fresh activity at t=4s
    b.add(_signal("ETH", "long"), _strategy("eth-momentum"), t0 + timedelta(seconds=4))

    # At t=6s: BTC is quiet for 6s (ripe), ETH only quiet for 2s (not ripe)
    ripe = b.ready_to_emit(
        t0 + timedelta(seconds=6), quiet_seconds=5, max_age_seconds=30
    )
    assert len(ripe) == 1
    assert ripe[0].entries[0].signal.asset == "BTC"
    # ETH still in bucketer
    assert b.size == 1


def test_normalises_asset_in_key():
    """BTC and BTC-USDT should land in the same bucket."""
    b = Bucketer()
    st = _strategy()
    b.add(_signal("BTC", "long"), st, datetime.now(UTC))
    b.add(_signal("BTC-USDT", "long"), st, datetime.now(UTC))
    # Both normalise to BTC-USDT for crypto, same bucket
    assert b.size == 1
    assert len(b.all_buckets()[0].entries) == 2
