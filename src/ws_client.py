"""
Simple Bybit public WS client (publicTrade example).

- Exposes run_ws() coroutine which connects to Bybit public realtime feed
  and listens for messages.
- Provides set_tick_consumer(callable) to allow wiring to an external queue
  (e.g. queue.put_nowait).
- Also defines enqueue_tick fallback to be replaced by main.
- Uses simple reconnect/backoff logic.
"""

import asyncio
import json
import logging
import math
import time
from typing import Any, Awaitable, Callable, Optional

import aiohttp

log = logging.getLogger("price_harvester.ws_client")
logging.getLogger("aiohttp").setLevel(logging.WARNING)

# Bybit public (linear) v5 endpoint & example topic format
DEFAULT_ENDPOINT = "wss://stream.bybit.com/v5/public/linear"
# Example topic: "publicTrade.BTCUSDT"
# We'll subscribe to topics passed via config or default to BTCUSDT
DEFAULT_SYMBOLS = ["BTCUSDT"]

# tick consumer will be set by main via set_tick_consumer(queue.put_nowait)
_tick_consumer: Optional[Callable[[dict], None]] = None

# default enqueue: no-op until replaced by main
def enqueue_tick(tick: dict) -> None:
    """Fallback enqueue — replaced by main via assignment or set_tick_consumer."""
    log.debug("enqueue_tick called but no consumer installed; tick: %r", tick)


def set_tick_consumer(fn: Callable[[dict], None]) -> None:
    """Set callable used to deliver parsed ticks (should be non-blocking)."""
    global _tick_consumer, enqueue_tick
    _tick_consumer = fn
    enqueue_tick = fn  # convenience: main can set this to queue.put_nowait
    log.info("ws_client: tick consumer set")


# optional: allow the main program to supply an asyncio loop or parser,
# but we will import parser lazily to avoid import-time errors.
def _deliver_tick(tick: dict) -> None:
    """Deliver a tick to the configured consumer (best-effort)."""
    if _tick_consumer:
        try:
            _tick_consumer(tick)
        except Exception:
            # consumer is expected to be non-blocking and robust; log at debug
            log.exception("tick consumer raised")
    else:
        # if still no consumer, default fallback
        try:
            enqueue_tick(tick)
        except Exception:
            log.debug("fallback enqueue_tick raised", exc_info=True)


async def _subscribe(ws, symbols: list[str]):
    """Send subscribe message for each symbol (Bybit v5 public format)."""
    topics = [f"publicTrade.{s}" for s in symbols]
    payload = {"op": "subscribe", "args": topics}
    await ws.send_json(payload)
    log.info("Sent subscribe: %s", topics)


async def _handle_message_raw(msg: dict):
    """
    Convert raw bybit message into ticks and deliver them.
    Uses src.parse.parse_public_trade_items or parse_public_trade_item if present.
    If parser not available, will try to extract msg['data'] list and deliver each.
    """
    # lazy import parser to avoid circular import / import errors
    try:
        from src.parse import parse_public_trade_items, parse_public_trade_item  # type: ignore
    except Exception:
        parse_public_trade_items = None
        parse_public_trade_item = None

    ticks = None
    try:
        if parse_public_trade_items:
            try:
                ticks = parse_public_trade_items(msg)
            except Exception:
                ticks = None

        if ticks is None and parse_public_trade_item:
            # msg may contain a list under 'data' or be a single item
            if isinstance(msg, dict) and "data" in msg and isinstance(msg["data"], (list, tuple)):
                tks = []
                for it in msg["data"]:
                    try:
                        parsed = parse_public_trade_item(it)
                        if isinstance(parsed, list):
                            tks.extend(parsed)
                        elif parsed is not None:
                            tks.append(parsed)
                    except Exception:
                        tks.append(it)
                ticks = tks
            else:
                try:
                    parsed = parse_public_trade_item(msg)
                    if isinstance(parsed, list):
                        ticks = parsed
                    elif parsed is not None:
                        ticks = [parsed]
                except Exception:
                    ticks = None

        # fallback: if message is dict with 'data' list, deliver items as-is
        if ticks is None:
            if isinstance(msg, dict) and "data" in msg and isinstance(msg["data"], (list, tuple)):
                ticks = list(msg["data"])
            else:
                # otherwise deliver the whole msg as single item
                ticks = [msg]
    except Exception:
        log.exception("Error while parsing message; delivering raw")
        ticks = [msg]

    # deliver ticks
    for t in ticks:
        try:
            # ensure dict
            if not isinstance(t, dict):
                # try wrap into a dict at least
                t = {"raw": t}
            _deliver_tick(t)
        except Exception:
            log.exception("Failed to deliver tick")


async def run_ws(
    endpoint: str = DEFAULT_ENDPOINT,
    symbols: Optional[list[str]] = None,
    reconnect_backoff: float = 1.0,
    max_backoff: float = 60.0,
):
    """
    Connect to Bybit WS and stream messages. This coroutine runs until cancelled.
    It will attempt reconnects with exponential backoff on errors.
    """
    if symbols is None:
        symbols = DEFAULT_SYMBOLS

    session = aiohttp.ClientSession()
    backoff = reconnect_backoff

    try:
        while True:
            ws = None
            try:
                log.info("Connecting to %s ...", endpoint)
                async with session.ws_connect(endpoint, heartbeat=25) as ws:
                    log.info("Connected to WS %s", endpoint)
                    # reset backoff after successful connect
                    backoff = reconnect_backoff

                    # subscribe
                    try:
                        await _subscribe(ws, symbols)
                    except Exception:
                        log.exception("Subscribe failed")

                    async for raw in ws:
                        if raw.type == aiohttp.WSMsgType.TEXT:
                            try:
                                msg = json.loads(raw.data)
                            except Exception:
                                log.debug("Received non-json text: %r", raw.data)
                                continue

                            # handle different 'op' / 'topic' structures
                            # deliver to handler
                            await _handle_message_raw(msg)

                        elif raw.type == aiohttp.WSMsgType.BINARY:
                            log.debug("binary message (ignored)")
                        elif raw.type == aiohttp.WSMsgType.PING:
                            log.debug("ws ping")
                        elif raw.type == aiohttp.WSMsgType.PONG:
                            log.debug("ws pong")
                        elif raw.type == aiohttp.WSMsgType.CLOSED:
                            log.warning("ws closed by server")
                            break
                        elif raw.type == aiohttp.WSMsgType.ERROR:
                            log.error("ws error frame: %r", raw)
                            break

            except asyncio.CancelledError:
                log.info("run_ws cancelled, exiting")
                raise
            except Exception:
                log.exception("WebSocket connection error, will reconnect")
            # reconnect with backoff
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2 or 1)
            # small jitter
            backoff = backoff * (0.9 + 0.2 * (time.time() % 1))
            log.info("Reconnecting in %.2f seconds", backoff)
    finally:
        try:
            await session.close()
        except Exception:
            pass
