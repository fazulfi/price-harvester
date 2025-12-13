#!/usr/bin/env python3
"""
src.ws_client
Async WebSocket client skeleton for Bybit V5 public streams.

- Uses aiohttp
- Connects to V5 public linear endpoint and subscribes to publicTrade.<SYMBOL>
- Prints incoming messages (pretty) for now
- Reconnect with backoff, graceful shutdown
"""
import asyncio
import json
import logging
import signal
from typing import List

import aiohttp

from config.config import config
from src.utils import pretty, backoff_sleep

LOG = logging.getLogger("ws_client")

# Use Bybit V5 public linear endpoint (suitable for BTCUSDT, USDT pairs)
DEFAULT_WS = "wss://stream.bybit.com/v5/public/linear"

running = True

def _signal_handler(signum, frame):
    global running
    LOG.info("Signal %s received, shutting down...", signum)
    running = False

async def subscribe(ws, topics: List[str]):
    if not topics:
        return
    msg = {"op": "subscribe", "args": topics}
    await ws.send_str(json.dumps(msg))
    LOG.info("Sent subscribe: %s", topics)

async def run_ws(endpoint: str = DEFAULT_WS):
    global running
    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
    attempt = 0
    max_attempts = 30

    # Build V5 topics (publicTrade.<SYMBOL>)
    topics = [f"publicTrade.{s.upper()}" for s in getattr(config, "SYMBOLS", ["BTCUSDT"])]
    LOG.info("Topics to subscribe (public V5): %s", topics)

    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while running:
            try:
                LOG.info("Connecting to %s ...", endpoint)
                async with session.ws_connect(endpoint) as ws:
                    LOG.info("Connected to WS (public)")
                    # subscribe
                    await subscribe(ws, topics)
                    attempt = 0  # reset backoff

                    async for msg in ws:
                        if not running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            # try parse JSON
                            try:
                                payload = json.loads(msg.data)
                            except Exception:
                                payload = msg.data
                            # print public messages; ignore internal control if wanted
                            print(pretty(payload))
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            LOG.debug("Binary message received (%d bytes)", len(msg.data))
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            LOG.warning("WS closed: %s", msg)
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            LOG.error("WS error: %s", msg)
                            break
                    LOG.info("Websocket connection ended, will attempt reconnect if running.")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                LOG.exception("WebSocket connection failed: %s", exc)
            # backoff before reconnecting
            await backoff_sleep(attempt)
            attempt = min(attempt + 1, max_attempts)
    LOG.info("Exiting run_ws")

async def main():
    # configure logging
    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    loop = asyncio.get_running_loop()
    # signal handlers (Windows may warn; safe to ignore)
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s, None))
    except NotImplementedError:
        LOG.warning("Signal handlers not supported on this platform; use Ctrl+C to stop")

    await run_ws()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Keyboard interrupt - exiting")
    except Exception:
        LOG.exception("Unhandled exception in ws_client")
