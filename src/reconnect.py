"""
Reconnector utility.

Usage:
    reconn = Reconnector(logger=LOG, base=1.0, cap=60.0, max_attempts=None)
    await reconn.run(lambda: connect_once(...))
Where connect_once is a coroutine factory that performs a single connection session
and returns when connection ends (normally or due to error). Reconnector will attempt
reconnects with exponential backoff + jitter.
"""
import asyncio
import logging
import random
from typing import Callable, Optional, Awaitable

class Reconnector:
    def __init__(self, logger: Optional[logging.Logger] = None, base: float = 1.0, cap: float = 60.0, max_attempts: Optional[int] = None):
        self.log = logger or logging.getLogger("reconnector")
        self.base = float(base)
        self.cap = float(cap)
        self.max_attempts = max_attempts  # None => infinite
        self._stop = False

    def stop(self):
        self._stop = True

    async def _sleep_with_jitter(self, attempt: int):
        delay = min(self.cap, self.base * (2 ** attempt))
        jitter = delay * 0.2
        delay = max(0.0, delay + (jitter * (2 * (random.random() - 0.5))))
        self.log.info("Backoff: sleeping %.2f s (attempt=%d)", delay, attempt)
        await asyncio.sleep(delay)

    async def run(self, conn_coro_factory: Callable[[], Awaitable[None]]):
        """
        conn_coro_factory should be a callable that returns a coroutine which runs
        one connection/session (it should return or raise when the connection ends).
        Reconnector will call it, await its completion, and if it failed or ended,
        attempt reconnect with backoff until stopped or max_attempts reached.
        """
        attempt = 0
        while not self._stop:
            try:
                self.log.info("Reconnector: starting connection attempt %d", attempt + 1)
                await conn_coro_factory()
                # if connection task exits normally, reset attempt (we can continue or break)
                self.log.info("Connection ended normally; resetting attempt counter.")
                attempt = 0
                # brief pause before reconnecting, avoids spin if server closes immediately
                await asyncio.sleep(0.2)
                continue
            except asyncio.CancelledError:
                self.log.info("Reconnector: cancelled, exiting")
                break
            except Exception as exc:
                self.log.exception("Connection attempt failed: %s", exc)
            # check max attempts
            attempt += 1
            if self.max_attempts is not None and attempt > self.max_attempts:
                self.log.error("Reconnector: max attempts reached (%d). Giving up.", self.max_attempts)
                break
            await self._sleep_with_jitter(attempt - 1)
        self.log.info("Reconnector stopped.")
