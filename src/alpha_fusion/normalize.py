"""Pure-function asset canonicalization + asset-class inference.

Kept side-effect-free so unit tests need no fixtures.
"""
from typing import Literal

# Bare crypto tickers we know about. Anything matching gets `-USDT` appended.
_CRYPTO_BARE_TICKERS = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "DOGE", "DOT",
    "LINK", "MATIC", "TRX", "LTC", "BCH", "ATOM", "ETC", "FIL", "NEAR",
    "APT", "ARB", "OP", "SUI", "INJ", "TIA", "SEI",
}


AssetClass = Literal["crypto", "stocks", "predictions", "forex", "unknown"]


def infer_asset_class(
    *, strategy_tags: list[str], signal_asset: str
) -> AssetClass:
    """Best-effort: prefer explicit tag, fall back to ticker shape."""
    tags_lower = {t.lower() for t in (strategy_tags or [])}
    if "crypto" in tags_lower:
        return "crypto"
    if "stocks" in tags_lower or "stock" in tags_lower or "equity" in tags_lower:
        return "stocks"
    if "poly" in tags_lower or "predictions" in tags_lower:
        return "predictions"
    if "forex" in tags_lower or "fx" in tags_lower:
        return "forex"

    # Fall back: ticker shape — check the more specific prefixes first.
    if signal_asset.startswith("poly:"):
        return "predictions"
    if "/" in signal_asset:
        # e.g. EUR/USD
        return "forex"
    bare = signal_asset.upper().split("-")[0]
    if bare in _CRYPTO_BARE_TICKERS:
        return "crypto"
    if "-" in signal_asset:
        # e.g. BTC-USDT → crypto (ticker we don't have in the bare set)
        return "crypto"
    # Default: stocks (covers NVDA / AAPL / TSLA / KLSE tickers)
    return "stocks"


def canonicalize_asset(raw: str, asset_class: AssetClass) -> str:
    """Convert a Signal.asset to the canonical format Alpha expects.

    Crypto: 'BTC' → 'BTC-USDT', 'BTC-USDT' → 'BTC-USDT' (unchanged).
    Stocks: 'NVDA' → 'NVDA' (uppercased only).
    Predictions: passthrough — market slug.
    Forex: 'EUR/USD' → 'EUR/USD' (passthrough); 'EURUSD' → 'EUR/USD' (split).
    """
    if asset_class == "crypto":
        s = raw.strip().upper()
        if "-" in s or s.endswith("USDT") or s.endswith("USDC"):
            # Already canonical or ends in a quote currency; leave it.
            if s.endswith("USDT") and "-" not in s:
                # 'BTCUSDT' → 'BTC-USDT'
                return f"{s[:-4]}-USDT"
            if s.endswith("USDC") and "-" not in s:
                return f"{s[:-4]}-USDC"
            return s
        return f"{s}-USDT"
    if asset_class == "stocks":
        return raw.strip().upper()
    if asset_class == "predictions":
        return raw.strip()
    if asset_class == "forex":
        s = raw.strip().upper()
        if "/" in s:
            return s
        if len(s) == 6:
            # 'EURUSD' → 'EUR/USD'
            return f"{s[:3]}/{s[3:]}"
        return s
    return raw
