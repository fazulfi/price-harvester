# src/api.py
import time
import logging
from typing import Dict, Any
from fastapi import FastAPI, Response, status

logger = logging.getLogger("price_harvester.api")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="price-harvester", version="0.1")

# shared state (safe defaults) — main akan meng-overwrite ini jika ada
_shared_state: Dict[str, Any] = {
    "started_at": int(time.time() * 1000),
    "metrics": {"messages_received": 0, "messages_stored": 0, "errors": 0},
    "last_ticks": {},
    # optional: other fields that main may set
}

def get_state() -> Dict[str, Any]:
    """Return a copy of state for API responses (avoid mutation)."""
    # shallow copy is fine for our small schema
    st = {
        "status": "ok",
        "uptime_ms": None,
        "metrics": {"messages_received": 0, "messages_stored": 0, "errors": 0},
        "last_ticks": {},
    }
    try:
        started_at = _shared_state.get("started_at")
        if started_at:
            st["uptime_ms"] = int(time.time() * 1000) - int(started_at)
        st["metrics"].update(_shared_state.get("metrics", {}))
        st["last_ticks"] = dict(_shared_state.get("last_ticks", {}))
    except Exception as e:
        # Defensive: log and return minimal state
        logger.exception("error while building health state: %s", e)
    return st

@app.get("/health")
async def health() -> Dict[str, Any]:
    """
    Health endpoint. Defensive: will not raise even if internal state is broken.
    main() can call `update_shared_state` to push real metrics into API.
    """
    try:
        return get_state()
    except Exception as exc:
        # This should rarely run because get_state() is defensive, but keep extra safeguard
        logger.exception("unhandled exception in /health: %s", exc)
        return Response(
            content='{"status":"error","message":"internal error"}',
            media_type="application/json",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

# helper for other modules to update the health state
def update_shared_state(metrics: Dict[str, Any] = None, last_ticks: Dict[str, int] = None, started_at: int = None):
    global _shared_state
    try:
        if started_at is not None:
            _shared_state["started_at"] = started_at
        if metrics:
            _shared_state.setdefault("metrics", {}).update(metrics)
        if last_ticks:
            _shared_state.setdefault("last_ticks", {}).update(last_ticks)
    except Exception:
        logger.exception("failed to update shared state")

# optional: expose a debug endpoint to peek at raw internal state (useful during dev)
@app.get("/_debug/state")
async def debug_state():
    # only for local dev — remove or protect in production
    return _shared_state
