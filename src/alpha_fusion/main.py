"""Entrypoint — connects DB, primes strategy cache, runs fusion + health + refresh tasks."""
import asyncio
import logging
import signal as os_signal

import structlog

from alpha_fusion import health, runtime, strategies
from alpha_fusion.db import db
from alpha_fusion.redis_client import close as close_redis
from alpha_fusion.settings import settings


def _configure_logging() -> None:
    logging.basicConfig(level=getattr(logging, settings.log_level), format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
    )


async def _run() -> None:
    log = structlog.get_logger("alpha_fusion.main")
    log.info("starting", version="0.1.0")

    await db.connect()
    # Prime cache before the fusion loop so we don't miss signals on cold start.
    await strategies.refresh_cache()

    tasks = [
        asyncio.create_task(runtime.ingest_loop(), name="ingest"),
        asyncio.create_task(runtime.emit_loop(), name="emit"),
        asyncio.create_task(strategies.refresh_loop(), name="strategy-refresh"),
        asyncio.create_task(health.serve(), name="health"),
    ]

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (os_signal.SIGTERM, os_signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    log.info("started", tasks=[t.get_name() for t in tasks])

    done, _pending = await asyncio.wait(
        [*tasks, asyncio.create_task(stop.wait(), name="stop")],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for t in done:
        if t.get_name() != "stop":
            log.error("task.exited", name=t.get_name(), result=t.exception())

    log.info("shutting_down")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await close_redis()
    await db.close()
    log.info("stopped")


def main() -> None:
    _configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
