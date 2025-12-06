#!/usr/bin/env python3
"""
Minimal entrypoint for price-harvester.
Nanti akan diisi: websocket client -> parser -> storage.
"""
import asyncio
import signal
import sys
from datetime import datetime

running = True

def _sigterm_handler(signum, frame):
    global running
    print("Signal received, shutting down...")
    running = False

async def main():
    print(f"price-harvester starter @ {datetime.utcnow().isoformat()}Z")
    # placeholder loop: nanti diganti logic websocket
    while running:
        await asyncio.sleep(1)
    print("clean exit")

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    try:
        asyncio.run(main())
    except Exception as e:
        print("Unhandled exception:", e, file=sys.stderr)
        raise
