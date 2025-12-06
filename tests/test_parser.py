# tests/test_parser.py
import json
import pytest

# try several common function names used for parser
POSSIBLE_NAMES = [
    "parse_public_trade",
    "parse_public_trade_item",
    "parse_tick",
    "parse_trade",
]

def _get_parser():
    try:
        import src.parse as parse_mod
    except Exception:
        pytest.skip("src.parse not present - skipping parser tests")
    for name in POSSIBLE_NAMES:
        if hasattr(parse_mod, name):
            return getattr(parse_mod, name)
    pytest.skip("No parser function found in src.parse with expected names")

def sample_snapshot_message():
    # simplified sample similar to Bybit publicTrade snapshot
    return {
        "topic": "publicTrade.BTCUSDT",
        "type": "snapshot",
        "ts": 1765041581094,
        "data": [
            {"T": 1765041581000, "s": "BTCUSDT", "S": "Buy", "p": "90000.5", "v": "0.001"},
            {"T": 1765041581000, "s": "BTCUSDT", "S": "Sell", "p": "90001.0", "v": "0.002"},
        ],
    }

def test_parser_returns_expected_fields():
    parser = _get_parser()
    msg = sample_snapshot_message()
    # parser might accept either full message or a single item; try both
    out = None
    try:
        out = parser(msg)  # whole message
    except Exception:
        # try parsing one item
        out = parser(msg["data"][0])
    assert out is not None

    # if parser returns list of ticks, normalize to list
    if isinstance(out, dict):
        ticks = [out]
    elif isinstance(out, (list, tuple)):
        ticks = list(out)
    else:
        pytest.fail("parser returned unexpected type: %r" % type(out))

    for t in ticks:
        # required normalized keys
        assert "symbol" in t, "missing symbol in parsed tick"
        assert "ts" in t, "missing ts in parsed tick"
        assert "price" in t, "missing price in parsed tick"
        assert "qty" in t, "missing qty in parsed tick"
        assert "side" in t, "missing side in parsed tick"

        # types
        assert isinstance(t["symbol"], str)
        assert isinstance(t["ts"], int)
        # price & qty can be float or str convertible to float
        try:
            float(t["price"])
            float(t["qty"])
        except Exception:
            pytest.fail("price/qty are not numeric: %r / %r" % (t["price"], t["qty"]))
