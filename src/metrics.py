"""
Simple async/thread-safe in-memory counters for basic metrics.
Counters:
- messages_received
- messages_stored
- errors

Provides increment(counter, n=1), snapshot(), and set_dry_run(flag).
"""
import threading
from typing import Dict

_lock = threading.Lock()
_counters: Dict[str, int] = {
    "messages_received": 0,
    "messages_stored": 0,
    "errors": 0,
}
_dry_run = False

def set_dry_run(flag: bool = True) -> None:
    global _dry_run
    with _lock:
        _dry_run = bool(flag)

def is_dry_run() -> bool:
    with _lock:
        return _dry_run

def increment(name: str, n: int = 1) -> None:
    if is_dry_run() and name == "messages_stored":
        # in dry-run, don't count stored messages
        return
    with _lock:
        if name not in _counters:
            _counters[name] = 0
        _counters[name] += int(n)

def snapshot() -> Dict[str, int]:
    with _lock:
        return dict(_counters)
