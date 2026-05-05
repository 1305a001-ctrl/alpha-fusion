"""Tiny aiohttp /health endpoint."""
import asyncio

import structlog
from aiohttp import web

from alpha_fusion import __version__
from alpha_fusion.db import db
from alpha_fusion.redis_client import r
from alpha_fusion.settings import settings

log = structlog.get_logger(__name__)


async def _health(_req: web.Request) -> web.Response:
    checks: dict[str, str] = {}
    try:
        async with db.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"down: {exc}"
    try:
        await r().ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"down: {exc}"
    overall = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    return web.json_response(
        {"status": overall, "version": __version__, "checks": checks},
        status=200 if overall == "ok" else 503,
    )


async def serve() -> None:
    app = web.Application()
    app.router.add_get("/health", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, settings.http_host, settings.http_port)
    await site.start()
    log.info("health.listening", port=settings.http_port)
    await asyncio.Event().wait()
