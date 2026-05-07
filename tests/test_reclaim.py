"""Tests for reclaim.reclaim_once via fakes.

The redis client and process callback are both mocked. We test the
control-flow: poison detection, ack-on-success, ack-on-logical-drop,
and don't-ack-on-exception.
"""
from unittest.mock import AsyncMock, patch

import pytest

from alpha_fusion import reclaim


@pytest.fixture
def fake_redis():
    rc = AsyncMock()
    # xautoclaim returns (next_cursor, entries, deleted_ids)
    rc.xautoclaim = AsyncMock(return_value=("0-0", [], []))
    rc.xack = AsyncMock(return_value=1)
    rc.xpending_range = AsyncMock(return_value=[])
    return rc


@pytest.fixture(autouse=True)
def patch_redis(fake_redis):
    with patch("alpha_fusion.reclaim.r", return_value=fake_redis):
        yield


async def test_reclaim_no_pending_returns_zero(fake_redis):
    process = AsyncMock(return_value="ok")
    reclaimed, ok, dropped = await reclaim.reclaim_once(stream="signals:trading", process=process)
    assert reclaimed == 0
    assert ok == 0
    assert dropped == 0
    process.assert_not_called()


async def test_reclaim_processes_orphan(fake_redis):
    """One orphaned entry; delivery_count below threshold; processed + acked."""
    fake_redis.xautoclaim.side_effect = [
        ("1234-0", [("1233-0", {"data": '{"id":"x"}'})], []),
        ("0-0", [], []),
    ]
    fake_redis.xpending_range.return_value = [{"times_delivered": 1}]
    process = AsyncMock(return_value="fused")

    reclaimed, ok, dropped = await reclaim.reclaim_once(stream="signals:trading", process=process)
    assert reclaimed == 1
    assert ok == 1
    assert dropped == 0
    process.assert_awaited_once()
    fake_redis.xack.assert_awaited()


async def test_reclaim_drops_poison(fake_redis):
    """Delivery count > threshold → ack and drop without processing."""
    fake_redis.xautoclaim.side_effect = [
        ("1234-0", [("1233-0", {"data": '{"id":"x"}'})], []),
        ("0-0", [], []),
    ]
    # threshold default = 5; simulate 6 deliveries
    fake_redis.xpending_range.return_value = [{"times_delivered": 6}]
    process = AsyncMock(return_value="ok")

    reclaimed, ok, dropped = await reclaim.reclaim_once(stream="signals:trading", process=process)
    assert reclaimed == 1
    assert ok == 0
    assert dropped == 1
    process.assert_not_called()
    fake_redis.xack.assert_awaited()


async def test_reclaim_leaves_unacked_on_exception(fake_redis):
    """If process raises, the entry is not acked (will retry next loop)."""
    fake_redis.xautoclaim.side_effect = [
        ("1234-0", [("1233-0", {"data": '{"id":"x"}'})], []),
        ("0-0", [], []),
    ]
    fake_redis.xpending_range.return_value = [{"times_delivered": 1}]
    process = AsyncMock(side_effect=RuntimeError("boom"))

    reclaimed, ok, dropped = await reclaim.reclaim_once(stream="signals:trading", process=process)
    assert reclaimed == 1
    assert ok == 0
    assert dropped == 0
    process.assert_awaited_once()
    fake_redis.xack.assert_not_awaited()


async def test_reclaim_acks_on_logical_drop(fake_redis):
    """Process returning 'low_confidence' / 'duplicate' / 'bad_payload' is
    a deliberate decision — still ack so the entry doesn't churn."""
    fake_redis.xautoclaim.side_effect = [
        ("1234-0", [("1233-0", {"data": '{"id":"x"}'})], []),
        ("0-0", [], []),
    ]
    fake_redis.xpending_range.return_value = [{"times_delivered": 2}]
    process = AsyncMock(return_value="duplicate")

    reclaimed, ok, dropped = await reclaim.reclaim_once(stream="signals:trading", process=process)
    assert reclaimed == 1
    assert ok == 1  # logical-drop counts as processed (decision made)
    fake_redis.xack.assert_awaited()


async def test_reclaim_paginates_via_cursor(fake_redis):
    """Multi-batch claim — cursor advances until we hit '0-0'."""
    fake_redis.xautoclaim.side_effect = [
        ("100-0", [("50-0", {"data": "a"})], []),
        ("200-0", [("150-0", {"data": "b"})], []),
        ("0-0", [], []),
    ]
    fake_redis.xpending_range.return_value = [{"times_delivered": 1}]
    process = AsyncMock(return_value="ok")

    reclaimed, ok, dropped = await reclaim.reclaim_once(stream="signals:trading", process=process)
    assert reclaimed == 2
    assert ok == 2
    assert process.await_count == 2


async def test_reclaim_xautoclaim_failure_breaks_cleanly(fake_redis):
    """Network error → log + return current counts; don't infinite-loop."""
    fake_redis.xautoclaim.side_effect = RuntimeError("redis down")
    process = AsyncMock(return_value="ok")
    reclaimed, ok, dropped = await reclaim.reclaim_once(stream="signals:trading", process=process)
    assert reclaimed == 0
    assert ok == 0
    process.assert_not_called()
