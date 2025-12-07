#!/usr/bin/env python3
"""
WebSocket client for Bybit with graceful reconnect (Reconnector),
metrics, pretty printing, and last_tick updates.
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
from src.state import update_last_tick, set_start_time
from src.utils import pretty
from src.reconnect import Reconnector

LOG = setup_logging("ws_client")

DEFAULT_WS = "wss://stream.bybit.com/v5/public/linear"

_shutdown = False


def _on_signal(signum, frame):
    """Graceful exit for systemd / Ctrl+C."""
    global _shutdown
    LOG.info("Signal %s received — shutting down...", signum)
    _shutdown = True


async def subscribe(ws, topics: List[str]):
    if not topics:
        return
    msg = {"op": "subscribe", "args": topics}
    await ws.send_str(json.dumps(msg))
    LOG.info("Subscribed: %s", topics)


async def single_session(endpoint: str, topics: List[str]):
    """One websocket connection session."""
    LOG.info("Connecting to: %s", endpoint)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(endpoint) as ws:
            LOG.info("Connected — sending subscribe...")
            await subscribe(ws, topics)

            async for msg in ws:
                if _shutdown:
                    LOG.info("Shutdown flag detected — closing session.")
                    break

                # TEXT MESSAGE
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        metrics_increment("messages_received")
                    except Exception:
                        LOG.debug("metrics_increment failed", exc_info=True)

                    try:
                        payload = json.loads(msg.data)
                    except Exception:
                        metrics_increment("errors")
                        payload = msg.data

                    # pretty print message (Step 7 spec)
                    print(pretty(payload))

                    # update last_tick
                    try:
                        if isinstance(payload, dict) and payload.get("topic", "").startswith("publicTrade"):
                            for t in payload.get("data", []):
                                sym = t.get("s")
                                ts = t.get("T") or t.get("ts")
                                if sym and ts:
                                    update_last_tick(sym, int(ts))
                    except Exception:
                        LOG.debug("Error updating last_tick", exc_info=True)

                # BINARY
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    LOG.debug("Binary %d bytes", len(msg.data))

                # CLOSED
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    LOG.warning("Websocket closed by server")
                    break

                # ERROR
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    metrics_increment("errors")
                    LOG.error("Websocket error: %s", msg)
                    break

    LOG.info("Session ended (clean exit)")


async def run_with_reconnect(endpoint: str = DEFAULT_WS):
    topics = [f"publicTrade.{s.upper()}" for s in getattr(config, "SYMBOLS", ["BTCUSDT"])]
    reconn = Reconnector(logger=LOG, base=1.0, cap=60.0, max_attempts=None)

    def factory():
        return single_session(endpoint, topics)

    await reconn.run(factory)


async def main():
    import time

    set_start_time(int(time.time() * 1000))
    loop = asyncio.get_running_loop()

    # signal handler (systemd-compatible)
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
        metrics_increment("errors")
        LOG.exception("Unhandled exception in ws_client main()")
    finally:
        LOG.info("Shutting down ws_client")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Keyboard interrupt — exiting.")
    except Exception:
        metrics_increment("errors")
        LOG.exception("Fatal error in ws_client")
