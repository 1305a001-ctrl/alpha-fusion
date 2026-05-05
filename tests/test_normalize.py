"""Pure tests for asset canonicalization + asset-class inference."""
from alpha_fusion.normalize import canonicalize_asset, infer_asset_class

# --- infer_asset_class ------------------------------------------------------


def test_infer_crypto_from_tag():
    assert infer_asset_class(strategy_tags=["crypto", "btc"], signal_asset="BTC") == "crypto"


def test_infer_stocks_from_tag():
    assert infer_asset_class(strategy_tags=["stocks"], signal_asset="NVDA") == "stocks"


def test_infer_predictions_from_tag():
    assert infer_asset_class(
        strategy_tags=["poly", "btc"], signal_asset="poly:fed-rate"
    ) == "predictions"


def test_infer_forex_from_tag():
    assert infer_asset_class(strategy_tags=["fx"], signal_asset="EUR/USD") == "forex"


def test_infer_crypto_from_bare_ticker_no_tags():
    """No useful tags? Bare 'BTC' is recognized as crypto."""
    assert infer_asset_class(strategy_tags=[], signal_asset="BTC") == "crypto"


def test_infer_crypto_from_dashed_ticker():
    assert infer_asset_class(strategy_tags=[], signal_asset="ETH-USDT") == "crypto"


def test_infer_default_to_stocks():
    """Unknown ticker, no tags → default to stocks."""
    assert infer_asset_class(strategy_tags=[], signal_asset="NVDA") == "stocks"


def test_infer_predictions_from_poly_prefix():
    assert infer_asset_class(strategy_tags=[], signal_asset="poly:will-btc-100k") == "predictions"


# --- canonicalize_asset -----------------------------------------------------


def test_canonicalize_bare_btc_to_btc_usdt():
    assert canonicalize_asset("BTC", "crypto") == "BTC-USDT"


def test_canonicalize_lowercase_eth():
    assert canonicalize_asset("eth", "crypto") == "ETH-USDT"


def test_canonicalize_already_canonical_unchanged():
    assert canonicalize_asset("BTC-USDT", "crypto") == "BTC-USDT"


def test_canonicalize_btcusdt_with_dash():
    assert canonicalize_asset("BTCUSDT", "crypto") == "BTC-USDT"


def test_canonicalize_usdc_quote_preserved():
    assert canonicalize_asset("ETH-USDC", "crypto") == "ETH-USDC"


def test_canonicalize_btcusdc_split():
    assert canonicalize_asset("BTCUSDC", "crypto") == "BTC-USDC"


def test_canonicalize_stocks_uppercase():
    assert canonicalize_asset("nvda", "stocks") == "NVDA"


def test_canonicalize_predictions_passthrough():
    assert canonicalize_asset("poly:btc-100k-2026", "predictions") == "poly:btc-100k-2026"


def test_canonicalize_forex_with_slash():
    assert canonicalize_asset("EUR/USD", "forex") == "EUR/USD"


def test_canonicalize_forex_without_slash():
    assert canonicalize_asset("EURUSD", "forex") == "EUR/USD"


def test_canonicalize_unknown_passthrough():
    assert canonicalize_asset("WHATEVER", "unknown") == "WHATEVER"
