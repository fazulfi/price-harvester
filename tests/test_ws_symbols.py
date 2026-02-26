import pytest

from src import ws_client


@pytest.mark.asyncio
async def test_resolve_symbols_uses_all_usdt(monkeypatch):
    monkeypatch.setattr(ws_client.config, "SYMBOLS", ["ALL_USDT"])

    async def fake_fetch(_session):
        return ["BTCUSDT", "ETHUSDT"]

    monkeypatch.setattr(ws_client, "fetch_all_usdt_symbols", fake_fetch)
    symbols = await ws_client.resolve_symbols()
    assert symbols == ["BTCUSDT", "ETHUSDT"]


@pytest.mark.asyncio
async def test_resolve_symbols_uses_configured(monkeypatch):
    monkeypatch.setattr(ws_client.config, "SYMBOLS", ["btcusdt", "ethusdt"])
    symbols = await ws_client.resolve_symbols()
    assert symbols == ["BTCUSDT", "ETHUSDT"]
