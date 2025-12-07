"""
Main runner for price-harvester.

- starts WS client (if present)
- starts aggregator and wires a tick queue
- tries to start storage writer if available
- graceful shutdown on SIGINT/SIGTERM
"""

import asyncio
import logging
import signal
import sys
from typing import Any, Callable, List

# local imports (best-effort, optional)
from config.config import config

log = logging.getLogger("price_harvester.main")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# import optional modules (wrap in try/except to be robust)
try:
    from src.aggregator import OHLCVAggregator
except Exception:
    OHLCVAggregator = None
    log.exception("Could not import OHLCVAggregator")

try:
    import src.ws_client as ws_client
except Exception:
    ws_client = None
    log.exception("Could not import src.ws_client")

try:
    import src.storage as storage
except Exception:
    storage = None
    log.exception("Could not import src.storage")

try:
    # parser helper (used if we need to convert raw ws messages)
    from src.parse import parse_public_trade_item, parse_public_trade_items
except Exception:
    parse_public_trade_item = None
    parse_public_trade_items = None
    log.debug("parse_public_trade_item / parse_public_trade_items not available")


# ---- helpers ----
def _try_set_tick_consumer(queue: asyncio.Queue) -> bool:
    """
    Try a few strategies to wire ws_client -> queue.
    Return True if successfully wired.
    """
    if ws_client is None:
        return False

    # 1) If ws_client exposes a simple enqueue function
    if hasattr(ws_client, "enqueue_tick"):
        try:
            # set to put_nowait to avoid awaiting inside ws loop
            ws_client.enqueue_tick = lambda tick: queue.put_nowait(tick)
            log.info("Wired ws_client.enqueue_tick -> queue.put_nowait")
            return True
        except Exception:
            log.exception("Failed to assign ws_client.enqueue_tick")

    # 2) If ws_client exposes a setter
    if hasattr(ws_client, "set_tick_consumer"):
        try:
            ws_client.set_tick_consumer(queue.put_nowait)
            log.info("Called ws_client.set_tick_consumer(queue.put_nowait)")
            return True
        except Exception:
            log.exception("Failed to call ws_client.set_tick_consumer")

    # 3) If ws_client exposes a raw message handler we can wrap (best-effort)
    #    common name guesses: handle_message, on_message
    for name in ("handle_message", "on_message", "process_raw"):
        if hasattr(ws_client, name):
            orig = getattr(ws_client, name)

            # define wrapper — supports coroutine or regular function
            async def _wrapped(msg: Any):
                try:
                    # prefer parser utilities if available
                    ticks = None
                    if parse_public_trade_items:
                        try:
                            ticks = parse_public_trade_items(msg)
                        except Exception:
                            ticks = None
                    if ticks is None and parse_public_trade_item:
                        # parse single item (or try list)
                        try:
                            out = parse_public_trade_item(msg)
                            if isinstance(out, list):
                                ticks = out
                            else:
                                ticks = [out]
                        except Exception:
                            ticks = None

                    # fallback: if msg looks like {'data':[...]} try to iterate
                    if ticks is None:
                        # naive: if dict-like and has 'data'
                        if isinstance(msg, dict) and "data" in msg and isinstance(msg["data"], (list, tuple)):
                            # try parse items if parser exists, else pass raw items
                            tks = []
                            for it in msg["data"]:
                                if parse_public_trade_item:
                                    try:
                                        t = parse_public_trade_item(it)
                                        if isinstance(t, list):
                                            tks.extend(t)
                                        elif t is not None:
                                            tks.append(t)
                                    except Exception:
                                        tks.append(it)
                                else:
                                    tks.append(it)
                            ticks = tks
                        else:
                            # last resort, treat msg itself as a tick-like item
                            ticks = [msg]

                    # enqueue normalized ticks
                    for t in ticks:
                        try:
                            queue.put_nowait(t)
                        except Exception:
                            log.exception("Failed to put tick into queue")
                except Exception:
                    log.exception("wrapped ws message handler error")

                # call original handler (if it expects to run too)
                try:
                    r = orig(msg)
                    if asyncio.iscoroutine(r):
                        await r
                except Exception:
                    # ignore original handler errors so aggregator can keep working
                    log.debug("original ws handler raised; continuing", exc_info=True)

            # attach wrapper
            setattr(ws_client, name, _wrapped)
            log.info("Wrapped ws_client.%s to enqueue ticks to queue", name)
            return True

    # no strategy succeeded
    return False


async def _maybe_start_storage(queue: asyncio.Queue) -> asyncio.Task:
    """
    Try to start storage writer if available.
    Returns the created task or None.
    The storage is expected to have an async entry like:
      - run_storage(dry_run=False)  or
      - run_storage_loop()
    """
    if storage is None:
        return None

    # If storage exposes a function to accept a queue, try to give it
    if hasattr(storage, "set_tick_queue"):
        try:
            storage.set_tick_queue(queue)
            log.info("storage.set_tick_queue(queue) called")
        except Exception:
            log.exception("storage.set_tick_queue failed")

    # Look for run_storage(dry_run=...) or run_storage_loop
    if hasattr(storage, "run_storage"):
        fn = getattr(storage, "run_storage")
        # call as a task
        try:
            task = asyncio.create_task(fn(dry_run=False))
            log.info("Started storage.run_storage task")
            return task
        except Exception:
            log.exception("Failed to start storage.run_storage")
    elif hasattr(storage, "run_storage_loop"):
        fn = getattr(storage, "run_storage_loop")
        try:
            task = asyncio.create_task(fn())
            log.info("Started storage.run_storage_loop task")
            return task
        except Exception:
            log.exception("Failed to start storage.run_storage_loop")

    # nothing to start
    return None


async def _maybe_start_ws_client() -> asyncio.Task:
    """
    Try to find an entrypoint in ws_client and start it as a background task.
    Common names: run_ws, run, main, start
    """
    if ws_client is None:
        return None

    for name in ("run_ws", "run", "start", "main"):
        if hasattr(ws_client, name):
            fn = getattr(ws_client, name)
            # If it's a coroutine function we can task it
            try:
                if asyncio.iscoroutinefunction(fn):
                    task = asyncio.create_task(fn())
                    log.info("Started ws_client.%s (coroutine) as task", name)
                    return task
                else:
                    # maybe it's a regular function that starts its own loop; run in executor
                    loop = asyncio.get_event_loop()
                    task = loop.run_in_executor(None, fn)
                    log.info("Started ws_client.%s() in executor", name)
                    return asyncio.create_task(task)
            except Exception:
                log.exception("Failed to start ws_client.%s", name)
    log.warning("No ws_client entrypoint found to start automatically")
    return None


async def run() -> None:
    loop = asyncio.get_event_loop()
    tick_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)

    # aggregator
    agg_task = None
    aggregator = None
    if OHLCVAggregator is not None:
        aggregator = OHLCVAggregator(db_path=config.DATABASE_PATH, intervals=("1s", "1m", "5m", "1h"))
        await aggregator.start(tick_queue)
        log.info("Aggregator started with intervals: %s", aggregator.intervals)
    else:
        log.warning("OHLCVAggregator not available; skipping aggregator startup")

    # try to automatically wire ws_client -> queue
    wired = _try_set_tick_consumer(tick_queue)
    if not wired:
        log.warning(
            "Could not auto-wire ws_client to queue. "
            "Please ensure your ws_client enqueues normalized ticks into the provided queue."
        )

    # start storage (if available)
    storage_task = await _maybe_start_storage(tick_queue)

    # start ws client (if entrypoint present)
    ws_task = await _maybe_start_ws_client()

    # wait for stop signal
    stop_event = asyncio.Event()

    def _signal_handler(*_):
        log.info("Received stop signal, shutting down...")
        stop_event.set()

    # register signals
    try:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    except NotImplementedError:
        # Windows / certain environments: fallback to default behavior
        pass

    # also allow Ctrl-C to set event
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        log.info("Run cancelled")

    # shutdown sequence
    log.info("Shutting down: cancelling WS/storage tasks and stopping aggregator")

    tasks_to_cancel = []
    if ws_task:
        tasks_to_cancel.append(ws_task)
    if storage_task:
        tasks_to_cancel.append(storage_task)

    for t in tasks_to_cancel:
        try:
            t.cancel()
        except Exception:
            log.debug("Failed to cancel task", exc_info=True)

    # await cancellation
    for t in tasks_to_cancel:
        try:
            await asyncio.wait_for(t, timeout=5.0)
        except Exception:
            log.debug("Task did not shut down cleanly", exc_info=True)

    # stop aggregator (flush remaining)
    if aggregator:
        try:
            await aggregator.stop()
            log.info("Aggregator stopped and flushed")
        except Exception:
            log.exception("Error stopping aggregator")

    # close DB in storage if it has stop method
    if storage and hasattr(storage, "stop"):
        try:
            maybe_stop = storage.stop
            if asyncio.iscoroutinefunction(maybe_stop):
                await maybe_stop()
            else:
                # run in executor
                await loop.run_in_executor(None, maybe_stop)
            log.info("storage.stop() called")
        except Exception:
            log.exception("storage.stop() failed")

    log.info("Shutdown complete")


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt - exiting")
    except Exception:
        log.exception("Unexpected error in main")
        sys.exit(1)


if __name__ == "__main__":
    main()
