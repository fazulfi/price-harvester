"""
Simple async/thread-safe in-memory counters for basic metrics.
Counters:
- messages_received
- messages_stored
- errors
Provides increment(counter, n=1) and snapshot() for reporting.
"""

import threading
from typing import Dict

_lock = threading.Lock()
_counters: Dict[str, int] = {
    "messages_received": 0,
    "messages_stored": 0,
    "errors": 0,
}

def increment(name: str, n: int = 1) -> None:
    with _lock:
        if name not in _counters:
            _counters[name] = 0
        _counters[name] += int(n)

def snapshot() -> Dict[str, int]:
    with _lock:
        return dict(_counters)
