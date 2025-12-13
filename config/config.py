from dataclasses import dataclass, field
import os
from dotenv import load_dotenv

# Load .env from project root if present
load_dotenv()


def _parse_symbols() -> list[str]:
    raw = os.getenv("SYMBOLS", "BTCUSDT")
    return [s.strip() for s in raw.split(",") if s.strip()]


@dataclass
class Config:
    BYBIT_API_KEY: str | None = os.getenv("BYBIT_API_KEY")
    BYBIT_API_SECRET: str | None = os.getenv("BYBIT_API_SECRET")
    TELEGRAM_TOKEN: str | None = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID: str | None = os.getenv("TELEGRAM_CHAT_ID")

    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "./data/price.db")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # ⬇️ INI YANG FIX ERROR
    SYMBOLS: list[str] = field(default_factory=_parse_symbols)

    HARVEST_INTERVAL: int = int(os.getenv("HARVEST_INTERVAL", "5"))


# single shared config instance
config = Config()
