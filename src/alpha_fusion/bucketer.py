"""Pure-function bucket/debouncer for v0.2 N:M aggregation.

Signals get added to in-process buckets keyed by (asset_class,
canonical_asset, direction). Periodic emit-loop calls
`ready_to_emit(now)` and emits aggregate Alphas for buckets that have:
    - been quiet for `bucket_quiet_seconds` (no new signal added), OR
    - reached `bucket_max_age_seconds` since first signal arrived

The bucketer is in-process — single instance of alpha-fusion. If we
ever scale to multiple instances we'll need to redis-back the bucket
state OR shard by key. v0.2 single-instance is fine.
"""
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from signals_contract.signal import Signal

from alpha_fusion.normalize import canonicalize_asset
from alpha_fusion.strategies import StrategyMeta

# (asset_class, canonical_asset, direction)
BucketKey = tuple[str, str, str]


@dataclass
class BucketEntry:
    signal: Signal
    strategy: StrategyMeta


@dataclass
class Bucket:
    key: BucketKey
    entries: list[BucketEntry] = field(default_factory=list)
    first_added_at: datetime | None = None
    last_added_at: datetime | None = None

    def signal_ids(self) -> set[UUID]:
        return {e.signal.id for e in self.entries}


def key_for(signal: Signal, strategy: StrategyMeta) -> BucketKey:
    return (
        strategy.asset_class,
        canonicalize_asset(signal.asset, strategy.asset_class),
        signal.direction,
    )


class Bucketer:
    def __init__(self) -> None:
        self._buckets: dict[BucketKey, Bucket] = {}

    @property
    def size(self) -> int:
        return len(self._buckets)

    def add(self, signal: Signal, strategy: StrategyMeta, now: datetime) -> BucketKey:
        """Add a signal to its bucket. Idempotent on (signal.id) within a
        bucket — duplicate adds for the same signal are no-ops (caller
        should pre-dedupe via redis but this is defence-in-depth).
        """
        key = key_for(signal, strategy)
        b = self._buckets.get(key)
        if b is None:
            b = Bucket(key=key, entries=[], first_added_at=now, last_added_at=now)
            self._buckets[key] = b
        if signal.id in b.signal_ids():
            return key  # already there
        b.entries.append(BucketEntry(signal=signal, strategy=strategy))
        b.last_added_at = now
        return key

    def ready_to_emit(
        self,
        now: datetime,
        *,
        quiet_seconds: int,
        max_age_seconds: int,
    ) -> list[Bucket]:
        """Pop and return buckets ready for aggregate emit.

        Ready conditions (any one):
            - last_added_at older than quiet_seconds (debounce)
            - first_added_at older than max_age_seconds (force-emit cap)
        """
        ripe: list[Bucket] = []
        for key in list(self._buckets.keys()):
            b = self._buckets[key]
            if b.last_added_at is None or b.first_added_at is None:
                continue
            quiet_for = (now - b.last_added_at).total_seconds()
            age = (now - b.first_added_at).total_seconds()
            if quiet_for >= quiet_seconds or age >= max_age_seconds:
                ripe.append(b)
                del self._buckets[key]
        return ripe

    def all_buckets(self) -> list[Bucket]:
        """For debugging / tests."""
        return list(self._buckets.values())
