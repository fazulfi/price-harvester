import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from config.config import config

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "price_harvester.log"

def setup_logging(name: str = None) -> logging.Logger:
    """
    Configure root logger with console + rotating file handler.
    Returns a logger for `name`.
    """
    level = getattr(logging, getattr(config, "LOG_LEVEL", "INFO").upper(), logging.INFO)

    root = logging.getLogger()
    if not root.handlers:
        # console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root.addHandler(ch)

        # rotating file handler
        fh = RotatingFileHandler(str(LOG_FILE), maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root.addHandler(fh)

    root.setLevel(level)
    return logging.getLogger(name)
