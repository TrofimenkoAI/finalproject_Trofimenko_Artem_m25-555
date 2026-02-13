from __future__ import annotations

from abc import ABC, abstractmethod
from time import perf_counter

import requests
from requests.exceptions import RequestException

from valutatrade_hub.core.exceptions import ApiRequestError
from valutatrade_hub.parser_service.config import ParserConfig


class BaseApiClient(ABC):
    def __init__(self, config: ParserConfig):
        self.config = config

    @abstractmethod
    def fetch_rates(self) -> dict[str, float]:
        raise NotImplementedError


class CoinGeckoClient(BaseApiClient):
    def fetch_rates(self) -> dict[str, float]:
        cfg = self.config
        cfg.validate()

        params = cfg.coingecko_simple_price_params()
        url = cfg.COINGECKO_URL

        started = perf_counter()
        try:
            resp = requests.get(url, params=params, timeout=cfg.REQUEST_TIMEOUT)
        except RequestException as e:
            raise ApiRequestError(f"CoinGecko network error: {e}")

        elapsed_ms = int((perf_counter() - started) * 1000)
        if resp.status_code != 200:
            msg = (resp.text or "").strip()
            if len(msg) > 300:
                msg = msg[:300]
            raise ApiRequestError(f"CoinGecko bad status: {resp.status_code} {msg}".strip())

        try:
            data = resp.json()
        except Exception as e:
            raise ApiRequestError(f"CoinGecko invalid JSON: {e}")

        if not isinstance(data, dict):
            raise ApiRequestError("CoinGecko invalid response format")

        vs = cfg.BASE_FIAT_CURRENCY.strip().upper()
        vs_l = vs.lower()

        id_to_code = {}
        for code, raw_id in cfg.CRYPTO_ID_MAP.items():
            if isinstance(code, str) and isinstance(raw_id, str):
                c = code.strip().upper()
                rid = raw_id.strip()
                if c and rid:
                    id_to_code[rid] = c

        out: dict[str, float] = {}
        for raw_id, obj in data.items():
            if not isinstance(raw_id, str):
                continue
            code = id_to_code.get(raw_id.strip())
            if not code:
                continue
            if not isinstance(obj, dict):
                continue
            val = obj.get(vs_l)
            if not isinstance(val, (int, float)):
                continue
            rate = float(val)
            if rate <= 0:
                continue
            out[f"{code}_{vs}"] = rate

        if not out:
            raise ApiRequestError("CoinGecko returned no usable rates")

        _ = elapsed_ms
        return out


class ExchangeRateApiClient(BaseApiClient):
    def fetch_rates(self) -> dict[str, float]:
        cfg = self.config
        cfg.validate()

        base = cfg.BASE_FIAT_CURRENCY.strip().upper()
        url = cfg.exchangerate_latest_url(base)

        started = perf_counter()
        try:
            resp = requests.get(url, timeout=cfg.REQUEST_TIMEOUT)
        except RequestException as e:
            raise ApiRequestError(f"ExchangeRate-API network error: {e}")

        elapsed_ms = int((perf_counter() - started) * 1000)
        if resp.status_code != 200:
            msg = (resp.text or "").strip()
            if len(msg) > 300:
                msg = msg[:300]
            raise ApiRequestError(f"ExchangeRate-API bad status: {resp.status_code} {msg}".strip())

        try:
            data = resp.json()
        except Exception as e:
            raise ApiRequestError(f"ExchangeRate-API invalid JSON: {e}")

        if not isinstance(data, dict):
            raise ApiRequestError("ExchangeRate-API invalid response format")

        rates = data.get("rates")
        if not isinstance(rates, dict):
            rates = data.get("conversion_rates")
        if not isinstance(rates, dict):
            raise ApiRequestError("ExchangeRate-API missing rates")

        out: dict[str, float] = {}
        for code in cfg.FIAT_CURRENCIES:
            if not isinstance(code, str) or not code.strip():
                continue
            c = code.strip().upper()
            if c == base:
                out[f"{c}_{base}"] = 1.0
                continue

            val = rates.get(c)
            if not isinstance(val, (int, float)):
                continue
            val = float(val)
            if val <= 0:
                continue

            rate = 1.0 / val
            out[f"{c}_{base}"] = rate

        if not out:
            raise ApiRequestError("ExchangeRate-API returned no usable rates")

        _ = elapsed_ms
        return out
