# src/api.py
import time
import logging
from typing import Dict, Any, List
from fastapi import FastAPI, Response, status
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("price_harvester.api")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="price-harvester", version="1.0")

# -----------------------------
# SHARED STATE (diisi oleh main)
# -----------------------------
_shared_state: Dict[str, Any] = {
    "started_at": int(time.time() * 1000),

    # metrics diisi main: ws_client + storage writer
    "metrics": {
        "messages_received": 0,
        "messages_stored": 0,
        "errors": 0,
    },

    # tick terakhir per simbol
    "last_ticks": {},      # {"BTCUSDT": 1765102723123}

    # agregat OHLCV terbaru
    "latest_ohlcv": {},    # {"BTCUSDT": {"1s": {...}, "1m": {...}, ...}}

    # daftar WebSocket clients untuk dashboard realtime
    "ws_clients": [],      # list[WebSocket]
}


# --------------------------
# INTERNAL: ambil health state
# --------------------------
def get_state() -> Dict[str, Any]:
    """Return health/report state for /health endpoint."""
    try:
        started_at = _shared_state.get("started_at")
        uptime = None
        if started_at:
            uptime = int(time.time() * 1000) - int(started_at)

        return {
            "status": "ok",
            "uptime_ms": uptime,
            "metrics": dict(_shared_state.get("metrics", {})),
            "last_ticks": dict(_shared_state.get("last_ticks", {})),
            "latest_ohlcv": _shared_state.get("latest_ohlcv", {}),
        }

    except Exception as e:
        logger.exception("error building /health state: %s", e)
        return {
            "status": "error",
            "uptime_ms": None,
            "metrics": {"messages_received": 0, "messages_stored": 0, "errors": 1},
            "last_ticks": {},
            "latest_ohlcv": {},
        }


# --------------------------
# HEALTH CHECK
# --------------------------
@app.get("/health")
async def health() -> Dict[str, Any]:
    try:
        return get_state()
    except Exception as exc:
        logger.exception("unhandled /health failure: %s", exc)
        return Response(
            content='{"status":"error"}',
            media_type="application/json",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


# --------------------------
# ENDPOINT: ticks terbaru
# --------------------------
@app.get("/latest_ticks")
async def latest_ticks():
    """Return dict of symbol → last timestamp."""
    return dict(_shared_state.get("last_ticks", {}))


# --------------------------
# ENDPOINT: OHLCV chart data
# --------------------------
@app.get("/chart/{symbol}/{interval}")
async def chart(symbol: str, interval: str):
    """
    Dashboard frontend memanggil ini untuk dapatkan data OHLCV terakhir.
    interval: '1s', '1m', '5m', '1h'
    """
    latest = _shared_state.get("latest_ohlcv", {})
    sym = latest.get(symbol.upper(), {})

    if interval not in sym:
        return {"symbol": symbol, "interval": interval, "data": []}

    return {
        "symbol": symbol,
        "interval": interval,
        "data": sym[interval],  # list of bars
    }


# ---------------------------------------------------
# WEBSOCKET: kirim data live ke dashboard (opsional)
# ---------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _shared_state["ws_clients"].append(ws)
    logger.info("Dashboard WebSocket connected")

    try:
        while True:
            await ws.receive_text()  # ignore incoming messages
    except WebSocketDisconnect:
        logger.info("Dashboard WebSocket disconnected")
    finally:
        if ws in _shared_state["ws_clients"]:
            _shared_state["ws_clients"].remove(ws)


# ---------------------------------------------------
# DIPANGGIL DARI main.py
# ---------------------------------------------------
def update_shared_state(
    metrics: Dict[str, Any] = None,
    last_ticks: Dict[str, int] = None,
    started_at: int = None,
    latest_ohlcv: Dict[str, Any] = None,
):
    """
    Digunakan main.py untuk update metrik / last tick / OHLCV aggregator.
    """
    try:
        if started_at is not None:
            _shared_state["started_at"] = started_at

        if metrics:
            _shared_state.setdefault("metrics", {}).update(metrics)

        if last_ticks:
            _shared_state.setdefault("last_ticks", {}).update(last_ticks)

        if latest_ohlcv:
            _shared_state.setdefault("latest_ohlcv", {}).update(latest_ohlcv)

    except Exception:
        logger.exception("failed updating shared state")


def broadcast_to_dashboards(payload: Dict[str, Any]):
    """Main.py memanggil ini untuk broadcast realtime update ke dashboard."""
    clients: List[WebSocket] = _shared_state.get("ws_clients", [])

    for ws in clients[:]:
        try:
            import json
            ws.send_text(json.dumps(payload))
        except Exception:
            logger.warning("Dropping dead WebSocket client")
            clients.remove(ws)
