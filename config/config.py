# config/config.py
from dataclasses import dataclass, field
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Config:
    BYBIT_API_KEY: str | None = os.getenv("BYBIT_API_KEY")
    BYBIT_API_SECRET: str | None = os.getenv("BYBIT_API_SECRET")
    TELEGRAM_TOKEN: str | None = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID: str | None = os.getenv("TELEGRAM_CHAT_ID")
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "./data/price.db")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    HARVEST_INTERVAL: int = int(os.getenv("HARVEST_INTERVAL", "5"))

    SYMBOLS: list[str] = field(
        default_factory=lambda: [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
    )

# single shared config instance
config = Config()
