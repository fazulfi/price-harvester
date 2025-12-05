import pytest
from src.parse import parse_public_trade_item

# sample trade item (V5 publicTrade snapshot element)
SAMPLE_ITEM = {
    "T": 1764946789306,
    "s": "BTCUSDT",
    "S": "Buy",
    "v": "0.001",
    "p": "90388.20",
    "i": "some-id",
    "seq": 493589733415
}

def test_parse_basic_fields():
    tick = parse_public_trade_item(SAMPLE_ITEM)
    assert tick["symbol"] == "BTCUSDT"
    assert isinstance(tick["ts"], int) and tick["ts"] == 1764946789306
    assert isinstance(tick["price"], float) and abs(tick["price"] - 90388.20) < 1e-8
    assert isinstance(tick["qty"], float) and abs(tick["qty"] - 0.001) < 1e-12
    assert tick["side"] == "Buy"
    assert "raw" in tick

def test_parse_missing_fields():
    item = {"s": "ETHUSDT"}  # minimal
    tick = parse_public_trade_item(item)
    assert tick["symbol"] == "ETHUSDT"
    assert tick["ts"] is None
    assert tick["price"] is None
    assert tick["qty"] is None

def test_parse_non_dict_returns_raw():
    tick = parse_public_trade_item("not-a-dict")
    assert tick["symbol"] is None
    assert tick["raw"] == "not-a-dict"
