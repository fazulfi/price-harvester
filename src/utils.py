import asyncio
import json
import logging
import math
import random
from typing import Any

log = logging.getLogger(__name__)

async def backoff_sleep(attempt: int, base: float = 1.0, cap: float = 60.0) -> None:
    """
    Exponential backoff sleep with jitter.
    attempt: 0-based attempt count
    base: base seconds
    cap: maximum seconds
    """
    # exponential with cap
    delay = min(cap, base * (2 ** attempt))
    # add jitter up to +/-20%
    jitter = delay * 0.2
    delay = delay + (jitter * (2 * (random.random() - 0.5)))
    log.debug("backoff sleep: %.2f s (attempt=%d)", delay, attempt)
    await asyncio.sleep(delay)

def pretty(msg: Any) -> str:
    """Return a pretty-printed JSON string for logging/printing."""
    try:
        return json.dumps(msg, ensure_ascii=False, indent=2)
    except Exception:
        return str(msg)
