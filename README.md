# alpha-fusion

**Phase 2.6.** Bridges `signals:*` Redis Streams from news-consolidator to `alphas:active` for oms-gateway pickup.

```
news-consolidator (hourly pipeline)
        │  XADD
        ▼
   signals:trading
   signals:critical          ◄── Redis Streams
   signals:poly
        │  XREADGROUP (alpha-fusion)
        ▼
    alpha-fusion
   ┌─────────────────┐
   │ confidence ≥ θ? │
   │ direction ok?   │
   │ strategy active?│
   │ canonicalize    │
   └─────────────────┘
        │  XADD (1:1 in v0.1)
        ▼
    alphas:active  ──► oms-gateway → oms_intents → ...
```

## What v0.1 does

- Subscribes to `signals:trading`, `signals:critical`, `signals:poly` (configurable)
- For each Signal:
  - Drop if `confidence < 0.6` (configurable)
  - Drop if `direction in {neutral, watch}`
  - Drop if strategy unknown / inactive
  - Otherwise: build Alpha with mapped asset, configured lifetime, all signal provenance threaded through `metadata` + `contributing_sources`
- Publishes Alpha JSON to `alphas:active` via XADD with maxlen cap

## What v0.1 does NOT do (deferred to v0.2)

- N:M aggregation: combining multiple signals on same (asset, direction) inside a window into one weighted Alpha
- Per-strategy `alpha_lifetime_seconds` (currently a single global default)
- Risk-score-based confidence dampening (`signal.composite_risk_score` is read but not yet used)

## Asset canonicalization

Lives in `normalize.py` (pure). Signal.asset isn't canonical — strategies use bare tickers like "BTC" or "NVDA" but Alpha consumers (oms-gateway → broker adapters) need the full pair format:

| Signal.asset | strategy.asset_class | Alpha.asset |
|---|---|---|
| BTC | crypto | BTC-USDT |
| btc | crypto | BTC-USDT |
| BTC-USDT | crypto | BTC-USDT (unchanged) |
| BTCUSDT | crypto | BTC-USDT |
| ETH-USDC | crypto | ETH-USDC (preserved) |
| NVDA | stocks | NVDA |
| poly:btc-100k | predictions | poly:btc-100k |
| EURUSD | forex | EUR/USD |

`asset_class` is inferred from `strategy.frontmatter.tags` first, then ticker shape as fallback.

## Strategy lookup

Cached in-process. Refreshed every `STRATEGY_REFRESH_SECONDS` (default 600) plus on cold-start. Cache miss → single-shot DB query.

## Idempotency

The XREADGROUP pattern (with ack-on-success) gives at-least-once delivery. If we crash between fuse and XADD, the message is redelivered → re-fused → produces a fresh Alpha.id, fresh created_at. Downstream oms-gateway dedups intents via `idempotency_key` constructed from `<strategy>:<alpha.id>:entry`, so a re-fused alpha → new alpha_id → new idempotency_key → new (independent) intent. Acceptable v0.1; v0.2 may add fusion-side dedup using `idempotency_key = <signal.id>` so a redelivered Signal collapses to one Alpha.

## Run

```bash
pip install -e '.[dev]'
ruff check src/ tests/
pytest -q   # 22 tests, all pure (no redis, no DB)
alpha-fusion
```

Required env (see `src/alpha_fusion/settings.py`):

- `REDIS_URL`
- `AICORE_DB_URL`

## Health

`GET http://localhost:8007/health`
