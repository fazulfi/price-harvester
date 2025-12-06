"""
src.main - CLI entrypoint for price-harvester

Usage examples:
  python -m src.main --mode ws --symbols BTCUSDT
  python -m src.main --mode api --port 8000
  python -m src.main --mode both --dry-run
"""
import asyncio
import logging
import os
from importlib import import_module

from src.cli import parse_args
from src.logging_setup import setup_logging

LOG = setup_logging("price_harvester.cli")

def apply_overrides(namespace):
    """
    Apply CLI overrides to runtime config object if available.
    """
    try:
        cfg_module = import_module("config.config")
        cfg = getattr(cfg_module, "config", None)
    except Exception:
        cfg = None

    if not cfg:
        return

    if getattr(namespace, "symbols", None):
        try:
            setattr(cfg, "SYMBOLS", namespace.symbols)
        except Exception:
            pass
    if getattr(namespace, "log_level", None):
        try:
            setattr(cfg, "LOG_LEVEL", namespace.log_level)
        except Exception:
            pass

async def run_ws(dry_run: bool = False):
    """
    Run the websocket reconnection loop.
    Note: storage/metrics must respect dry-run if provided by implementation.
    """
    LOG.info("Starting WS (dry_run=%s)...", dry_run)
    # lazy import to avoid startup side-effects
    from src.ws_client import run_with_reconnect
    # Some metrics modules may expose set_dry_run; call if present
    try:
        from src.metrics import set_dry_run as _set_dry
        if dry_run:
            _set_dry(True)
    except Exception:
        pass

    await run_with_reconnect()

def run_api_blocking(port: int = 8000):
    """Run FastAPI/uvicorn in blocking mode (process will block)."""
    import uvicorn
    LOG.info("Starting API on port %s", port)
    uvicorn.run("src.api:app", host="0.0.0.0", port=port, log_level="info")

async def run_both(dry_run: bool = False, port: int = 8000):
    """
    Run WS as asyncio task and uvicorn in background thread.
    This is a simple approach for small deployments.
    """
    import threading
    import uvicorn

    try:
        from src.metrics import set_dry_run as _set_dry
        if dry_run:
            _set_dry(True)
    except Exception:
        pass

    loop = asyncio.get_running_loop()
    ws_task = loop.create_task(run_ws(dry_run=dry_run))

    def _uvicorn_run():
        uvicorn.run("src.api:app", host="0.0.0.0", port=port, log_level="info")

    thread = threading.Thread(target=_uvicorn_run, daemon=True)
    thread.start()
    LOG.info("Started uvicorn in background thread; websocket task running in event loop.")

    try:
        await ws_task
    except asyncio.CancelledError:
        LOG.info("ws task cancelled")
    finally:
        LOG.info("run_both: exiting")

def main(argv=None):
    ns = parse_args(argv)
    apply_overrides(ns)

    # configure logging level override
    os.environ.setdefault("LOG_LEVEL", ns.log_level)
    logging.getLogger().setLevel(getattr(logging, ns.log_level.upper(), logging.INFO))

    LOG.info("CLI starting with mode=%s symbols=%s dry_run=%s", ns.mode, ns.symbols, ns.dry_run)

    if ns.mode == "ws":
        asyncio.run(run_ws(dry_run=ns.dry_run))
    elif ns.mode == "api":
        run_api_blocking(port=ns.port)
    elif ns.mode == "both":
        asyncio.run(run_both(dry_run=ns.dry_run, port=ns.port))
    else:
        LOG.error("Unknown mode: %s", ns.mode)

if __name__ == "__main__":
    main()
