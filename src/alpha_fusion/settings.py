"""Env-driven settings."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str
    aicore_db_url: str

    # Subscribe — comma-separated Redis Streams.
    signal_streams: str = "signals:trading,signals:critical,signals:poly"
    consumer_group: str = "alpha-fusion"
    consumer_name: str = "alpha-fusion-1"
    block_ms: int = 5_000
    batch_size: int = 50

    # Output
    alphas_stream: str = "alphas:active"
    alphas_stream_maxlen: int = 100_000

    # Fusion thresholds
    min_confidence: float = 0.6
    alpha_lifetime_seconds: int = 3_600  # 1h default; per-strategy override TBD
    edge_bps_floor: int = 10
    edge_bps_ceiling: int = 100  # confidence 1.0 maps here; 0.0 to floor

    # v0.2 — bucket / debounce parameters for N:M aggregation
    bucket_quiet_seconds: int = 5     # emit when no new signal for this long
    bucket_max_age_seconds: int = 30  # OR force-emit at this age (cap on debounce)
    bucket_emit_poll_seconds: float = 1.0  # how often the emit-loop scans buckets

    # v0.2 — dedup: redis SET NX EX on signal.id
    dedup_ttl_seconds: int = 3_600

    # Strategy cache refresh
    strategy_refresh_seconds: int = 600  # 10 min

    # Health endpoint
    http_host: str = "0.0.0.0"  # noqa: S104  — bound to 127.0.0.1 in compose
    http_port: int = 8007

    log_level: str = "INFO"


settings = Settings()  # type: ignore[call-arg]
