"""
Shared in-memory state for healthcheck and monitoring.
"""

import threading
from typing import Dict, Optional

_lock = threading.Lock()

_last_tick_ts: Dict[str, int] = {}
_start_time_ms: Optional[int] = None

def set_start_time(ts: int):
    global _start_time_ms
    with _lock:
        _start_time_ms = ts

def get_start_time() -> Optional[int]:
    with _lock:
        return _start_time_ms

def update_last_tick(symbol: str, ts: int):
    with _lock:
        _last_tick_ts[symbol] = ts

def get_last_ticks() -> Dict[str, int]:
    with _lock:
        return dict(_last_tick_ts)
