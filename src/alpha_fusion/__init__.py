"""alpha-fusion — Phase 2.6 of the trading stack.

Subscribes to news-consolidator's signals:* Redis Streams, transforms
Signals into Alphas, publishes to alphas:active for oms-gateway pickup.

v0.2 — N:M aggregation + redis-backed dedup:
    - dedup on signal.id via Redis SET NX EX (cross-restart safe)
    - bucket signals by (asset_class, canonical_asset, direction)
    - debounce: emit a bucket when no new signal for N sec OR bucket
      reached max_age — collapses M signals into 1 Alpha
    - aggregate confidence = weighted mean of signal confidences (by
      contributing-source weight); edge_bps = max; reasoning + sources
      union'd

Drop conditions still apply per signal:
    - confidence < threshold → drop
    - direction in {neutral, watch} → drop
    - strategy unknown / inactive
"""

__version__ = "0.2.0"
