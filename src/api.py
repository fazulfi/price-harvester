from fastapi import FastAPI
import time
from src.metrics import snapshot as get_metrics
from src.state import get_last_ticks, get_start_time

app = FastAPI()

@app.get("/health")
def health():
    now = int(time.time() * 1000)
    start_time = get_start_time()
    uptime = now - start_time if start_time else None

    return {
        "status": "ok",
        "uptime_ms": uptime,
        "metrics": get_metrics(),
        "last_ticks": get_last_ticks(),
    }
