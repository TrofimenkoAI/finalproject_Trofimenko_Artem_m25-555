from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from valutatrade_hub.infra.settings import SettingsLoader

SETTINGS = SettingsLoader()


@dataclass(frozen=True)
class ParserConfig:
    EXCHANGERATE_API_KEY: str | None = field(default_factory=lambda: os.getenv("EXCHANGERATE_API_KEY"))
    COINGECKO_API_KEY: str | None = field(default_factory=lambda: os.getenv("COINGECKO_API_KEY"))

    COINGECKO_URL: str = "https://api.coingecko.com/api/v3/simple/price"
    EXCHANGERATE_API_URL: str = "https://v6.exchangerate-api.com/v6"

    BASE_FIAT_CURRENCY: str = "USD"
    FIAT_CURRENCIES: tuple[str, ...] = ("EUR", "GBP", "RUB")

    CRYPTO_CURRENCIES: tuple[str, ...] = ("BTC", "ETH", "SOL")
    CRYPTO_ID_MAP: dict[str, str] = field(
        default_factory=lambda: {
            "BTC": "bitcoin",
            "ETH": "ethereum",
            "SOL": "solana",
        }
    )

    REQUEST_TIMEOUT: int = 10

    RATES_FILE_PATH: str = field(default_factory=lambda: str(Path(SETTINGS.get("RATES_JSON"))))
    HISTORY_FILE_PATH: str = field(default_factory=lambda: str(Path(SETTINGS.get("EXCHANGE_RATES_JSON", "data/exchange_rates.json"))))

    def validate(self) -> None:
        if not isinstance(self.EXCHANGERATE_API_URL, str) or not self.EXCHANGERATE_API_URL.startswith("http"):
            raise ValueError("EXCHANGERATE_API_URL invalid")
        if not isinstance(self.COINGECKO_URL, str) or not self.COINGECKO_URL.startswith("http"):
            raise ValueError("COINGECKO_URL invalid")

        if not isinstance(self.BASE_FIAT_CURRENCY, str) or not self.BASE_FIAT_CURRENCY.strip():
            raise ValueError("BASE_FIAT_CURRENCY invalid")

        if not isinstance(self.REQUEST_TIMEOUT, int) or self.REQUEST_TIMEOUT <= 0:
            raise ValueError("REQUEST_TIMEOUT invalid")

        if not isinstance(self.FIAT_CURRENCIES, tuple) or not self.FIAT_CURRENCIES:
            raise ValueError("FIAT_CURRENCIES invalid")
        if not isinstance(self.CRYPTO_CURRENCIES, tuple) or not self.CRYPTO_CURRENCIES:
            raise ValueError("CRYPTO_CURRENCIES invalid")
        if not isinstance(self.CRYPTO_ID_MAP, dict) or not self.CRYPTO_ID_MAP:
            raise ValueError("CRYPTO_ID_MAP invalid")

        for c in self.FIAT_CURRENCIES:
            if not isinstance(c, str) or not c.strip():
                raise ValueError("FIAT_CURRENCIES invalid")
        for c in self.CRYPTO_CURRENCIES:
            if not isinstance(c, str) or not c.strip():
                raise ValueError("CRYPTO_CURRENCIES invalid")
        for k, v in self.CRYPTO_ID_MAP.items():
            if not isinstance(k, str) or not k.strip():
                raise ValueError("CRYPTO_ID_MAP invalid")
            if not isinstance(v, str) or not v.strip():
                raise ValueError("CRYPTO_ID_MAP invalid")

        rf = Path(self.RATES_FILE_PATH)
        hf = Path(self.HISTORY_FILE_PATH)
        if not rf.suffix.lower() == ".json":
            raise ValueError("RATES_FILE_PATH invalid")
        if not hf.suffix.lower() == ".json":
            raise ValueError("HISTORY_FILE_PATH invalid")

    def exchangerate_latest_url(self, base: str | None = None) -> str:
        b = (base or self.BASE_FIAT_CURRENCY).strip().upper()
        if not self.EXCHANGERATE_API_KEY or not str(self.EXCHANGERATE_API_KEY).strip():
            raise ValueError("EXCHANGERATE_API_KEY is not set")
        return f"{self.EXCHANGERATE_API_URL}/{self.EXCHANGERATE_API_KEY}/latest/{b}"

    def coingecko_simple_price_params(self) -> dict[str, str]:
        ids = []
        for code in self.CRYPTO_CURRENCIES:
            c = code.strip().upper()
            raw_id = self.CRYPTO_ID_MAP.get(c)
            if isinstance(raw_id, str) and raw_id.strip():
                ids.append(raw_id.strip())
        params = {
            "vs_currencies": self.BASE_FIAT_CURRENCY.strip().lower(),
            "ids": ",".join(ids),
        }
        if self.COINGECKO_API_KEY and str(self.COINGECKO_API_KEY).strip():
            params["x_cg_demo_api_key"] = str(self.COINGECKO_API_KEY).strip()
        return params
