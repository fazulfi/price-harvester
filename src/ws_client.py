#!/usr/bin/env python3
import asyncio
import json
import logging
import sys
import aiohttp

from config.config import config
from .utils import pretty, backoff_sleep

LOG = logging.getLogger("ws_client")

WS_PUBLIC = "wss://stream.bybit.com/v5/public/spot"

async def run_ws():
    symbols = getattr(config, "SYMBOLS", ["BTCUSDT"])
    topics = [f"publicTrade.{s}" for s in symbols]

    LOG.info("Starting WS client")
    LOG.info("Symbols: %s", symbols)
    LOG.info("Topics: %s", topics)

    timeout = aiohttp.ClientTimeout(total=None)
    attempt = 0

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                LOG.info("Connecting to %s", WS_PUBLIC)
                async with session.ws_connect(WS_PUBLIC) as ws:
                    LOG.info("CONNECTED")

                    sub_msg = {
                        "op": "subscribe",
                        "args": topics
                    }
                    await ws.send_json(sub_msg)
                    LOG.info("SUBSCRIBED")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            print(pretty(data), flush=True)

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError("WS error")

            except Exception as e:
                LOG.error("WS error: %s", e)

            attempt += 1
            await backoff_sleep(attempt)

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout
    )
    await run_ws()

if __name__ == "__main__":
    asyncio.run(main())
