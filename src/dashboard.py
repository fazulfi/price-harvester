# src/dashboard.py
import asyncio
import json
from typing import Set
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()

class Broadcaster:
    def __init__(self):
        self._clients: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._clients.discard(ws)

    async def publish(self, message: dict):
        """
        Send JSON message to all connected clients.
        Non-blocking best-effort: remove clients that error.
        """
        text = json.dumps(message, default=str)
        to_remove = []
        async with self._lock:
            clients = list(self._clients)
        for ws in clients:
            try:
                await ws.send_text(text)
            except Exception:
                # client dead / closed
                to_remove.append(ws)
        if to_remove:
            async with self._lock:
                for ws in to_remove:
                    self._clients.discard(ws)

broadcaster = Broadcaster()

@router.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket):
    """
    WebSocket endpoint frontend should connect to.
    Receives no messages; only server->client pushes.
    """
    await broadcaster.connect(ws)
    try:
        while True:
            # keep connection alive; ignore incoming messages
            await ws.receive_text()
    except WebSocketDisconnect:
        await broadcaster.disconnect(ws)
    except Exception:
        await broadcaster.disconnect(ws)
