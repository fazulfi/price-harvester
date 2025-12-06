"""
src.parse - robust normalization of Bybit publicTrade messages

Exports:
 - parse_public_trade(msg) -> List[dict]   # list of normalized ticks
 - parse_public_trade_item(item) -> dict   # single normalized tick (for tests)
 - aliases: parse_trade, parse_tick, parse_public_trade_item_list, etc.

Normalized tick dict keys:
  "symbol": str or None
  "ts": int or None (ms)
  "price": float or None
  "qty": float or None
  "side": str or None
  "_raw": original item
"""
from typing import List, Dict, Any, Iterable, Union

def _get(obj: Dict[str, Any], keys: Iterable[str], default=None):
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return default

def _to_int(x):
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None

def _to_float(x):
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return None

def _normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    # symbol
    symbol = _get(item, ("s", "symbol", "sym"))
    if symbol is not None:
        symbol = str(symbol).upper()

    # timestamp (ms)
    ts = _get(item, ("T", "t", "ts", "time", "timestamp"))
    ts_val = _to_int(ts)

    # price
    price = _get(item, ("p", "price", "Px", "price_str"))
    price_val = _to_float(price)

    # quantity / volume
    qty = _get(item, ("v", "size", "qty", "volume"))
    qty_val = _to_float(qty)

    # side
    side = _get(item, ("S", "side", "side_str"))
    if side is not None:
        side = str(side)

    normalized = {
        "symbol": symbol,
        "ts": ts_val if ts_val is not None else None,
        "price": price_val if price_val is not None else None,
        "qty": qty_val if qty_val is not None else None,
        "side": side if side is not None else None,
        "_raw": item,
    }
    return normalized

def parse_public_trade(msg: Union[Dict[str, Any], str]) -> List[Dict[str, Any]]:
    """Return list of normalized ticks for a message (snapshot or single item or nested)."""
    if isinstance(msg, str):
        import json
        try:
            msg = json.loads(msg)
        except Exception:
            return []

    if not isinstance(msg, dict):
        return []

    data = _get(msg, ("data", "items", "tick"))
    if isinstance(data, list):
        out = []
        for it in data:
            if isinstance(it, dict):
                out.append(_normalize_item(it))
        return out

    # message itself might be a single item
    if any(k in msg for k in ("T", "t", "s", "S", "p", "v", "qty", "price")):
        return [_normalize_item(msg)]

    # scan nested lists
    for v in msg.values():
        if isinstance(v, list):
            items = []
            for it in v:
                if isinstance(it, dict) and any(k in it for k in ("T", "t", "s", "p", "v")):
                    items.append(_normalize_item(it))
            if items:
                return items
    return []

# --- NEW: single-item wrapper expected by tests ---
def parse_public_trade_item(item: Any) -> Dict[str, Any]:
    """
    Parse a single trade item and return a normalized dict.
    If input is a dict-like trade item -> normalized dict.
    If input is a full message with 'data' -> returns normalized first item.
    If input is not a dict (e.g. string) -> returns a dict with None values and _raw set.
    This makes tests expecting a single-dict return compatible.
    """
    # if not dict, try to handle gracefully
    if not isinstance(item, dict):
        # try to parse if it's a JSON string
        if isinstance(item, str):
            import json
            try:
                parsed = json.loads(item)
                # if parsed is dict, fallthrough to normal handling
                if isinstance(parsed, dict):
                    # if parsed contains data list, return first normalized
                    data = _get(parsed, ("data", "items"))
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                        return _normalize_item(data[0])
                    # else if parsed looks like single item:
                    if any(k in parsed for k in ("T", "t", "s", "p", "v")):
                        return _normalize_item(parsed)
            except Exception:
                pass
        # fallback: return None-fields with raw
        return {"symbol": None, "ts": None, "price": None, "qty": None, "side": None, "_raw": item}

    # if the dict looks like a full message with data list
    data = _get(item, ("data", "items"))
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return _normalize_item(data[0])

    # otherwise treat as single item dict
    return _normalize_item(item)

# convenience aliases
parse_trade = parse_public_trade
parse_tick = parse_public_trade
parse_public_trade_item_list = parse_public_trade
parse_public_trade_item_single = parse_public_trade_item
