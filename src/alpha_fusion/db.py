"""Postgres pool — only reads strategies for the in-memory cache."""
import asyncpg
import structlog

from alpha_fusion.settings import settings

log = structlog.get_logger(__name__)


class DB:
    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("DB not connected — call connect() first")
        return self._pool

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            settings.aicore_db_url, min_size=1, max_size=2
        )
        log.info("db.connected", url=settings.aicore_db_url.split("@")[-1])

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()


db = DB()
