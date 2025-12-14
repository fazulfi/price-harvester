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
from src.utils import pretty
from src.reconnect import Reconnector

LOG = setup_logging('ws_client')
DEFAULT_WS = "wss://stream.bybit.com/v5/public/linear"

# global flag for clean shutdown
_shutdown = False

def _on_signal(signum, frame):
    global _shutdown
    LOG.info("Signal %s received, shutting down...", signum)
    _shutdown = True

async def subscribe(ws, topics: List[str]):
    if not topics:
        return
    msg = {"op": "subscribe", "args": topics}
    await ws.send_str(json.dumps(msg))
    LOG.info("Sent subscribe: %s", topics)

async def single_session(endpoint: str, topics: List[str]):
    """
    Run a single websocket session: open connection, subscribe, and consume messages.
    Returns when connection closes (or raises on error).
    """
    LOG.info("single_session: connecting to %s", endpoint)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(endpoint) as ws:
            LOG.info("single_session: connected, subscribing...")
            await subscribe(ws, topics)
            # consume until ws closes or shutdown
            async for msg in ws:
                if _shutdown:
                    LOG.info("single_session: shutdown flag set, exiting receive loop")
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    # metrics: one message received
                    try:
                        metrics_increment('messages_received')
                    except Exception:
                        # metrics error shouldn't stop the client
                        LOG.debug("metrics_increment(messages_received) failed", exc_info=True)

                    try:
                        payload = json.loads(msg.data)
                    except Exception:
                        # count parsing errors
                        try:
                            metrics_increment('errors')
                        except Exception:
                            LOG.debug("metrics_increment(errors) failed", exc_info=True)
                        payload = msg.data

                    # default behavior: print payload (STEP 7 requirement)
                    print(pretty(payload))

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    LOG.debug("Binary message received (%d bytes)", len(msg.data))

                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    LOG.warning("Websocket closed by server: %s", msg)
                    break

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    # log and increment error counter
                    try:
                        metrics_increment('errors')
                    except Exception:
                        LOG.debug("metrics_increment(errors) failed", exc_info=True)
                    LOG.error("Websocket error: %s", msg)
                    break

    LOG.info("single_session: connection context ended (clean exit)")

async def run_with_reconnect(endpoint: str = DEFAULT_WS):
    topics = [f"publicTrade.{s.upper()}" for s in getattr(config, "SYMBOLS", ["BTCUSDT"])]
    reconn = Reconnector(logger=LOG, base=1.0, cap=60.0, max_attempts=None)
    # define factory that returns the coroutine for a single session
    async def factory():
        await single_session(endpoint, topics)
    await reconn.run(factory)

async def main():
    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    loop = asyncio.get_running_loop()
    # register signals for graceful shutdown
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
            metrics_increment('errors')
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
            metrics_increment('errors')
        except Exception:
            LOG.debug("metrics_increment(errors) failed", exc_info=True)
        LOG.exception("Unhandled exception in ws_client")
