"""alpha-fusion — Phase 2.6 of the trading stack.

Subscribes to news-consolidator's signals:* Redis Streams, transforms each
Signal into an Alpha, publishes to alphas:active for oms-gateway pickup.

v0.1 — 1:1 Signal→Alpha passthrough:
    - signal.confidence below threshold → drop
    - signal.direction in {neutral, watch} → drop
    - asset canonicalization via strategy.frontmatter.tags
    - strategy_id → (slug, asset_class) cached + refreshed every 10min
    - new ContributingSource per signal, source_kind=internal-strategy

v0.2 will add N:M aggregation (combine multiple signals on the same
asset/direction inside a window into one weighted Alpha).
"""

__version__ = "0.1.0"
