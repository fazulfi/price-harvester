# src/cli.py
"""
Small CLI helper: parse args and load config.
"""
from argparse import ArgumentParser, ArgumentTypeError
from typing import List, Optional
import json
import os
from config.config import config

def csv_to_list(s: str) -> List[str]:
    if not s:
        return []
    return [x.strip().upper() for x in s.split(",") if x.strip()]

def existing_file(p: str) -> str:
    if not os.path.exists(p):
        raise ArgumentTypeError(f"Config file not found: {p}")
    return p

def build_parser() -> ArgumentParser:
    p = ArgumentParser(prog="price-harvester", description="Price harvester CLI")
    p.add_argument(
        "--symbols",
        "-s",
        type=str,
        default=",".join(getattr(config, "SYMBOLS", ["BTCUSDT"])),
        help="Comma-separated list of symbols (ex: BTCUSDT,ETHUSDT)",
    )
    p.add_argument(
        "--config",
        "-c",
        type=existing_file,
        default=None,
        help="Path to .env or config file (optional). If provided, will load env vars from it",
    )
    p.add_argument(
        "--mode",
        "-m",
        choices=["ws", "api", "both"],
        default="ws",
        help="Which component to run: ws (websocket), api (health endpoint), both (run ws + api in same process)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode: don't write to database (useful for debugging)",
    )
    p.add_argument(
        "--log-level",
        default=getattr(config, "LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    p.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for API server (only used when mode=api or both)",
    )
    return p

def load_env_file(path: Optional[str]):
    """Load key=value lines into environment (simple .env loader)."""
    if not path:
        return
    try:
        with open(path, "r", encoding="utf8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except Exception:
        # don't fail hard — CLI may still work with defaults
        pass

def parse_args(args: Optional[List[str]] = None):
    parser = build_parser()
    ns = parser.parse_args(args=args)
    # normalize symbols to list
    ns.symbols = csv_to_list(ns.symbols)
    # load .env if provided
    if ns.config:
        load_env_file(ns.config)
    return ns
