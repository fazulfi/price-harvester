#!/usr/bin/env python3
"""
WS client using Reconnector for graceful reconnect + backoff.

- single_session: one websocket connection lifecycle (connect, subscribe, receive until closed)
- Reconnector.run will call single_session repeatedly with backoff on failures
"""
import asyncio
import json
import logging
import signal
from typing import List

import aiohttp

from config.config import config
from src.logging_setup import setup_logging
from src.metrics import increment as metrics_increment
from src.reconnect import Reconnector
from src.state import set_start_time, update_last_tick
from src.utils import pretty

LOG = setup_logging("ws_client")
DEFAULT_WS = "wss://stream.bybit.com/v5/public/linear"
INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"

# global flag for clean shutdown
_shutdown = False


def _on_signal(signum, frame):
    global _shutdown
    LOG.info("Signal %s received, shutting down...", signum)
    _shutdown = True


async def fetch_all_usdt_symbols(session: aiohttp.ClientSession) -> List[str]:
    """Fetch all active USDT linear symbols from Bybit REST API."""
    cursor = None
    symbols: set[str] = set()

    while True:
        params = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor

        async with session.get(INSTRUMENTS_URL, params=params) as resp:
            resp.raise_for_status()
            payload = await resp.json()

        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        for item in result.get("list", []) or []:
            symbol = str(item.get("symbol", "")).upper()
            status = str(item.get("status", "")).upper()
            if symbol.endswith("USDT") and status in {"TRADING", "SETTLING"}:
                symbols.add(symbol)

        cursor = result.get("nextPageCursor")
        if not cursor:
            break

    return sorted(symbols)


async def resolve_symbols() -> List[str]:
    configured = [str(s).upper() for s in getattr(config, "SYMBOLS", ["BTCUSDT"]) if s]
    use_all_usdt = any(s in {"ALLUSDT", "ALL_USDT", "*"} for s in configured)
    if not use_all_usdt:
        return configured

    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        symbols = await fetch_all_usdt_symbols(session)

    if symbols:
        LOG.info("Resolved ALL_USDT to %d symbols", len(symbols))
        return symbols

    LOG.warning("Failed to resolve ALL_USDT symbols; fallback to BTCUSDT")
    return ["BTCUSDT"]


async def subscribe(ws, topics: List[str], chunk_size: int = 10):
    if not topics:
        return

    for i in range(0, len(topics), chunk_size):
        batch = topics[i : i + chunk_size]
        msg = {"op": "subscribe", "args": batch}
        await ws.send_str(json.dumps(msg))
        LOG.info("Sent subscribe batch %d-%d (%d topics)", i + 1, i + len(batch), len(batch))
        await asyncio.sleep(0.05)


async def single_session(endpoint: str, topics: List[str]):
    """Run one websocket session: connect, subscribe, consume messages."""
    LOG.info("single_session: connecting to %s", endpoint)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(endpoint) as ws:
            LOG.info("single_session: connected, subscribing to %d topics...", len(topics))
            await subscribe(ws, topics)

            async for msg in ws:
                if _shutdown:
                    LOG.info("single_session: shutdown flag set, exiting receive loop")
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        metrics_increment("messages_received")
                    except Exception:
                        LOG.debug("metrics_increment(messages_received) failed", exc_info=True)

                    try:
                        payload = json.loads(msg.data)
                    except Exception:
                        try:
                            metrics_increment("errors")
                        except Exception:
                            LOG.debug("metrics_increment(errors) failed", exc_info=True)
                        payload = msg.data

                    print(pretty(payload))

                    try:
                        if isinstance(payload, dict) and str(payload.get("topic", "")).startswith("publicTrade"):
                            for it in payload.get("data", []):
                                sym = it.get("s")
                                ts = it.get("T") or it.get("ts")
                                if sym and ts:
                                    update_last_tick(sym, int(ts))
                    except Exception:
                        LOG.debug("update_last_tick failed", exc_info=True)

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    LOG.debug("Binary message received (%d bytes)", len(msg.data))
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    LOG.warning("Websocket closed by server: %s", msg)
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    try:
                        metrics_increment("errors")
                    except Exception:
                        LOG.debug("metrics_increment(errors) failed", exc_info=True)
                    LOG.error("Websocket error: %s", msg)
                    break

    LOG.info("single_session: connection context ended (clean exit)")


async def run_with_reconnect(endpoint: str = DEFAULT_WS):
    symbols = await resolve_symbols()
    topics = [f"publicTrade.{s}" for s in symbols]
    reconn = Reconnector(logger=LOG, base=1.0, cap=60.0, max_attempts=None)

    async def factory():
        await single_session(endpoint, topics)

    await reconn.run(factory)


async def main():
    import time

    set_start_time(int(time.time() * 1000))

    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    loop = asyncio.get_running_loop()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: _on_signal(s, None))
    except NotImplementedError:
        LOG.warning("Signal handlers not supported on this platform")

    try:
        await run_with_reconnect()
    except asyncio.CancelledError:
        LOG.info("main: cancelled")
    except Exception:
        try:
            metrics_increment("errors")
        except Exception:
            LOG.debug("metrics_increment(errors) failed", exc_info=True)
        LOG.exception("main: unhandled exception")
    finally:
        LOG.info("main: exiting")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Keyboard interrupt - exiting")
    except Exception:
        try:
            metrics_increment("errors")
        except Exception:
            LOG.debug("metrics_increment(errors) failed", exc_info=True)
        LOG.exception("Unhandled exception in ws_client")
