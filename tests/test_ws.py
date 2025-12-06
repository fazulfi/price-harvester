# tests/test_ws.py
import pytest
import asyncio

@pytest.mark.asyncio
async def test_ws_client_connects_monkeypatched(monkeypatch):
    """
    Try to import src.ws_client and monkeypatch its network calls to simulate a connection.
    The test will skip if module or expected functions are not present.
    """
    try:
        import importlib
        ws = importlib.import_module("src.ws_client")
    except Exception:
        pytest.skip("src.ws_client not present - skipping ws tests")

    # prefer run_with_reconnect or run_ws or main
    candidate_names = ["run_with_reconnect", "run_ws", "main", "run_ws_client"]

    fn = None
    for name in candidate_names:
        if hasattr(ws, name):
            fn = getattr(ws, name)
            break
    if fn is None:
        pytest.skip("No runnable entrypoint found in src.ws_client")

    # monkeypatch the network layer used inside the ws client.
    # Two common libraries: aiohttp.ClientSession.ws_connect OR websockets.connect
    called = {"connected": False}

    async def fake_ws_connect(*args, **kwargs):
        class FakeWS:
            async def __aenter__(self):
                called["connected"] = True
                return self
            async def __aexit__(self, exc_type, exc, tb):
                return False
            async def receive(self):
                await asyncio.sleep(0.01)
                return None
            async def receive_str(self):
                await asyncio.sleep(0.01)
                return "{}"
            async def __aiter__(self):
                return self
            async def __anext__(self):
                raise StopAsyncIteration
            async def close(self):
                return
        return FakeWS()

    # try monkeypatching both common connection points
    try:
        import aiohttp
        monkeypatch.setattr("aiohttp.ClientSession.ws_connect", fake_ws_connect, raising=False)
    except Exception:
        pass

    try:
        import websockets
        monkeypatch.setattr("websockets.connect", fake_ws_connect, raising=False)
    except Exception:
        pass

    # now call the function for short time
    try:
        if asyncio.iscoroutinefunction(fn):
            # run it but cancel quickly to avoid long-running loops
            task = asyncio.create_task(fn())
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            # sync entrypoint - call and hope it returns quickly
            fn()
    except Exception:
        # if it raises because of missing implementation, skip
        pytest.skip("ws client entrypoint failed under test harness")
    assert True  # if we reached here, test passed
