"""
Parser for Bybit V5 publicTrade items.

Function:
- parse_public_trade_item(item) -> dict with keys:
    - symbol (str)
    - ts (int) : epoch milliseconds or None
    - price (float) or None
    - qty (float) or None
    - side (str) or None (values like "Buy"/"Sell")
    - raw : original item (kept for debugging)
"""

from typing import Dict, Any, Optional


def _to_int_maybe(val) -> Optional[int]:
    try:
        return int(val)
    except Exception:
        return None


def _to_float_maybe(val) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None


def parse_public_trade_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single trade item from Bybit V5 publicTrade 'data' list.

    Example item fields (not exhaustive):
        "T": 1764945955448,    # timestamp ms
        "s": "BTCUSDT",        # symbol
        "S": "Buy",            # side
        "v": "0.001",          # volume
        "p": "90388.20",       # price
    """
    if not isinstance(item, dict):
        # Return safe fallback with raw stored
        return {"symbol": None, "ts": None, "price": None, "qty": None, "side": None, "raw": item}

    # Some call sites pass the full websocket message payload.
    # In this case, parse the first trade item from `data`.
    data = item.get("data")
    if isinstance(data, list) and data:
        first_item = data[0]
        if isinstance(first_item, dict):
            item = first_item

    # timestamp: common keys (T, t, ts)
    ts = None
    for k in ("T", "t", "ts"):
        if k in item:
            ts = _to_int_maybe(item.get(k))
            if ts is not None:
                break

    # symbol: prefer lower-case key 's' or 'symbol'
    symbol = item.get("s") or item.get("symbol") or item.get("symbolName") or None

    # side (Buy/Sell) typical key 'S' or 'side'
    side = item.get("S") or item.get("side") or None

    # price: 'p' or 'price'
    price = None
    if "p" in item:
        price = _to_float_maybe(item.get("p"))
    elif "price" in item:
        price = _to_float_maybe(item.get("price"))

    # qty: 'v' or 'qty' or 'q'
    qty = None
    for qk in ("v", "qty", "q"):
        if qk in item:
            qty = _to_float_maybe(item.get(qk))
            break

    return {
        "symbol": symbol,
        "ts": ts,
        "price": price,
        "qty": qty,
        "side": side,
        "raw": item,
    }
