# src/ws_client.py
import asyncio
import json
import logging
from typing import Any, Callable, Optional

import aiohttp

logger = logging.getLogger("ws_client")
logger.setLevel(logging.INFO)

# default endpoint and symbols (override in your config if needed)
DEFAULT_WS_ENDPOINT = "wss://stream.bybit.com/v5/public/linear"
DEFAULT_SYMBOLS = ["BTCUSDT"]

# consumer: function to call with normalized tick dict
# main.py will try to set this by either assigning ws_client.enqueue_tick
# or calling set_tick_consumer(...)
_tick_consumer: Optional[Callable[[dict], None]] = None


def set_tick_consumer(fn: Callable[[dict], None]) -> None:
    """Set a sync function used to enqueue ticks (expected to be fast / non-blocking)."""
    global _tick_consumer
    _tick_consumer = fn
    logger.info("ws_client: tick consumer set")


def enqueue_tick(tick: dict) -> None:
    """Fallback enqueue function that main may override. Default: use consumer if set, else no-op."""
    if _tick_consumer:
        try:
            _tick_consumer(tick)
        except Exception:
            logger.exception("ws_client: tick consumer raised")
    else:
        # no consumer set yet; drop silently or log at debug level
        logger.debug("ws_client: no tick consumer; dropping tick")


def _normalize_trade_item(item: dict) -> dict:
    """
    Normalize a single trade item from Bybit publicTrade response into:
    {
      "symbol": "BTCUSDT",
      "ts": 1670000000000,   # milliseconds
      "price": 12345.6,
      "qty": 0.001,
      "side": "Buy" or "Sell",
      "raw": { ... }        # original item for debugging
    }
    """
    # Bybit fields vary; attempt common names: T/t/timestamp, p price, v/v quantity, s symbol, S/side
    symbol = item.get("s") or item.get("symbol") or item.get("S")
    # timestamp may already be ms (T) or seconds; we try to coerce
    ts = item.get("T") or item.get("t") or item.get("ts") or item.get("time")
    if ts is None:
        ts_ms = int(asyncio.get_event_loop().time() * 1000)
    else:
        ts = int(ts)
        # heuristic: if ts looks like seconds (<= 1e10), convert to ms
        if ts < 1_000_000_000_000:
            ts_ms = ts * 1000
        else:
            ts_ms = ts

    # price and qty
    p = item.get("p") or item.get("price")
    v = item.get("v") or item.get("q") or item.get("qty") or item.get("size")
    try:
        price = float(p) if p is not None else 0.0
    except Exception:
        price = 0.0
    try:
        qty = float(v) if v is not None else 0.0
    except Exception:
        qty = 0.0

    side = item.get("S") or item.get("side") or item.get("s")  # sometimes S holds side
    # normalize side to "Buy"/"Sell"
    if isinstance(side, str):
        side_norm = "Buy" if side.lower().startswith("b") else ("Sell" if side.lower().startswith("s") else side)
    else:
        side_norm = "Buy" if item.get("isBuyerMaker") is False else "Sell"

    return {
        "symbol": symbol,
        "ts": ts_ms,
        "price": price,
        "qty": qty,
        "side": side_norm,
        "raw": item,
    }


async def _subscribe(ws, symbols):
    """
    Send subscription message. For Bybit v5 the subscribe format is like:
    {"op":"subscribe","args":["publicTrade.BTCUSDT"]}
    Adjust if using a different exchange.
    """
    try:
        args = [f"publicTrade.{s}" for s in symbols]
        msg = {"op": "subscribe", "args": args}
        await ws.send_json(msg)
        logger.info("ws_client: Sent subscribe: %s", args)
    except Exception:
        logger.exception("ws_client: subscribe failed")


async def run(endpoint: str = DEFAULT_WS_ENDPOINT, symbols: Optional[list] = None):
    """
    Long-running coroutine: connect, subscribe, receive messages, parse and enqueue ticks.
    Reconnects automatically with backoff on failures.
    """
    if symbols is None:
        symbols = DEFAULT_SYMBOLS

    backoff = 1
    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=30)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            try:
                logger.info("ws_client: Connecting to WS: %s", endpoint)
                async with session.ws_connect(endpoint) as ws:
                    logger.info("ws_client: Connected. Subscribing...")
                    await _subscribe(ws, symbols)
                    backoff = 1
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                logger.debug("ws_client: non-json message: %s", msg.data)
                                continue

                            # typical Bybit message: {"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":..., "data":[...]}
                            # handle if 'data' is list of items
                            if isinstance(data, dict) and "data" in data and isinstance(data["data"], (list, tuple)):
                                for it in data["data"]:
                                    try:
                                        t = _normalize_trade_item(it)
                                        enqueue_tick(t)
                                    except Exception:
                                        logger.exception("ws_client: failed normalize/enqueue item")
                            else:
                                # if it's an event or ping/pong, ignore or log
                                logger.debug("ws_client: received event/other: %s", data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error("ws_client: websocket error: %s", msg)
                            break
                        else:
                            # ignore other message types (binary, ping, pong)
                            pass

            except asyncio.CancelledError:
                logger.info("ws_client: cancelled, exiting")
                raise
            except Exception:
                logger.exception("ws_client: connection failed; reconnecting in %s sec", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
