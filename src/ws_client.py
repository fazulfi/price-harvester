# src/ws_client.py
import asyncio
import json
import logging
import math
from typing import List, Optional

import aiohttp

from src import storage
from src import metrics
import config  # expecting config.DATABASE_PATH, config.WS_ENDPOINT, config.SYMBOLS

logger = logging.getLogger("ws_client")
logger.setLevel(logging.INFO)


DEFAULT_ENDPOINT = getattr(config, "WS_ENDPOINT", "wss://stream.bybit.com/v5/public/linear")
DEFAULT_SYMBOLS = getattr(config, "SYMBOLS", ["BTCUSDT"])


class WSClient:
    def __init__(
        self,
        endpoint: str = DEFAULT_ENDPOINT,
        symbols: Optional[List[str]] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.endpoint = endpoint
        self.symbols = symbols or DEFAULT_SYMBOLS
        self._session = session
        self._running = False
        self._backoff_base = 1.0
        self._backoff_max = 60.0

    async def _ensure_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()

    async def connect_and_run(self):
        """Top-level connector with reconnect/backoff."""
        attempt = 0
        self._running = True
        await self._ensure_session()

        while self._running:
            try:
                attempt += 1
                logger.info("Connecting to %s (attempt %d)", self.endpoint, attempt)
                await self._run_once()
                # If _run_once returns normally (clean stop), break.
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # on error, backoff and retry
                wait = min(self._backoff_max, self._backoff_base * (2 ** (attempt - 1)))
                jitter = wait * 0.2 * (2 * (math.random() if hasattr(math, "random") else 0.5) - 1)
                wait = max(1.0, wait + jitter)
                logger.exception("WS connection error: %s. reconnecting in %.1fs", exc, wait)
                await asyncio.sleep(wait)

    async def _run_once(self):
        """Single live connection lifetime: connect, subscribe, receive."""
        await self._ensure_session()
        async with self._session.ws_connect(self.endpoint, heartbeat=20) as ws:
            logger.info("Connected to WS %s", self.endpoint)
            await self._send_subscribe(ws)
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_text(msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    # unlikely for Bybit; ignore or parse if needed
                    logger.debug("binary message received (len=%d)", len(msg.data))
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error("ws error: %s", msg)
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.info("ws closed by server")
                    break

    async def _send_subscribe(self, ws):
        # Bybit v5 subscribe message expects topics like "publicTrade.BTCUSDT"
        topics = [f"publicTrade.{s}" for s in self.symbols]
        subscribe = {"op": "subscribe", "args": topics}
        await ws.send_json(subscribe)
        logger.info("Sent subscribe: %s", topics)

    async def _handle_text(self, raw: str):
        try:
            obj = json.loads(raw)
        except Exception:
            logger.exception("failed to parse ws json")
            metrics.ERRORS.inc()
            return

        # quickly ignore pings or control messages
        if "topic" not in obj and obj.get("op") != "subscribe":
            # Not a trade payload; still count as message
            metrics.MESSAGES_RECEIVED.inc()
            return

        # Bybit sends snapshots with data[] or single events
        if obj.get("op") == "subscribe":
            # subscription ack
            logger.debug("subscribe response: %s", obj)
            return

        topic = obj.get("topic", "")
        # expecting "publicTrade.SYMBOL"
        if topic.startswith("publicTrade."):
            data = obj.get("data") or []
            # data is typically a list of trade items
            for item in data:
                tick = self._normalize_bybit_trade(item)
                if tick:
                    # update metrics + enqueue
                    metrics.MESSAGES_RECEIVED.inc()
                    metrics.LAST_TICK_TS.labels(symbol=tick["symbol"]).set(tick["ts"])
                    await storage.enqueue_tick(tick)
        else:
            # other public topics - ignore but count
            metrics.MESSAGES_RECEIVED.inc()

    def _get_str(self, d, keys, default=None):
        for k in keys:
            if k in d:
                return d[k]
        return default

    def _normalize_bybit_trade(self, item: dict):
        """
        Convert Bybit publicTrade item into normalized tick dict:
        { symbol, ts (ms), price (float), qty (float), side (str), raw }
        """
        try:
            # Bybit fields seen: 'T' or 't' as ts (ms), 's' symbol, 'p' price, 'v' qty, 'S' side ('Buy'/'Sell')
            symbol = self._get_str(item, ("s", "symbol"))
            if symbol is None:
                return None
            ts = self._get_str(item, ("T", "t", "ts", "time"))
            # some messages have ts as int (ms) or as string; normalize to int
            if isinstance(ts, str) and ts.isdigit():
                ts = int(ts)
            elif isinstance(ts, (int, float)):
                ts = int(ts)
            else:
                # fallback to current time in ms
                ts = int(asyncio.get_event_loop().time() * 1000)

            price = self._get_str(item, ("p", "price"))
            qty = self._get_str(item, ("v", "size", "qty"))
            side = self._get_str(item, ("S", "side"))

            price_f = float(price) if price is not None else 0.0
            qty_f = float(qty) if qty is not None else 0.0
            side = side or "Unknown"

            return {
                "symbol": symbol,
                "ts": ts,
                "price": price_f,
                "qty": qty_f,
                "side": side,
                "raw": item,
            }
        except Exception:
            logger.exception("failed to normalize trade item")
            metrics.ERRORS.inc()
            return None

    async def stop(self):
        self._running = False
        if self._session:
            await self._session.close()


# convenience top-level helper used by src.main
async def run_ws_loop(*, endpoint=None, symbols=None):
    client = WSClient(endpoint=endpoint or DEFAULT_ENDPOINT, symbols=symbols or DEFAULT_SYMBOLS)
    await client.connect_and_run()


if __name__ == "__main__":
    # quick manual test runner
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_ws_loop())
