from __future__ import annotations
from abc import ABC, abstractmethod
from valutatrade_hub.core.exceptions import CurrencyNotFoundError



class Currency(ABC):
    def __init__(self, name: str, code: str):
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name cannot be empty")
        if not isinstance(code, str):
            raise ValueError("code invalid")
        code_norm = code.strip().upper()
        if " " in code_norm or not (2 <= len(code_norm) <= 5) or not code_norm.isalnum():
            raise ValueError("code invalid")

        self.name = name.strip()
        self.code = code_norm

    @abstractmethod
    def get_display_info(self) -> str:
        raise NotImplementedError


class FiatCurrency(Currency):
    def __init__(self, name: str, code: str, issuing_country: str):
        super().__init__(name, code)
        if not isinstance(issuing_country, str) or not issuing_country.strip():
            raise ValueError("issuing_country cannot be empty")
        self.issuing_country = issuing_country.strip()

    def get_display_info(self) -> str:
        return f"[FIAT] {self.code} — {self.name} (Issuing: {self.issuing_country})"


class CryptoCurrency(Currency):
    def __init__(self, name: str, code: str, algorithm: str, market_cap: float):
        super().__init__(name, code)
        if not isinstance(algorithm, str) or not algorithm.strip():
            raise ValueError("algorithm cannot be empty")
        if not isinstance(market_cap, (int, float)) or float(market_cap) < 0:
            raise ValueError("market_cap invalid")
        self.algorithm = algorithm.strip()
        self.market_cap = float(market_cap)

    def get_display_info(self) -> str:
        return f"[CRYPTO] {self.code} — {self.name} (Algo: {self.algorithm}, MCAP: {self.market_cap:.2e})"


_CURRENCIES: dict[str, Currency] = {
    "USD": FiatCurrency("US Dollar", "USD", "United States"),
    "EUR": FiatCurrency("Euro", "EUR", "Eurozone"),
    "RUB": FiatCurrency("Russian Ruble", "RUB", "Russia"),
    "BTC": CryptoCurrency("Bitcoin", "BTC", "SHA-256", 1.12e12),
    "ETH": CryptoCurrency("Ethereum", "ETH", "Ethash", 3.00e11),
}


def get_currency(code: str) -> Currency:
    if not isinstance(code, str) or not code.strip():
        raise ValueError("code invalid")
    key = code.strip().upper()
    cur = _CURRENCIES.get(key)
    if cur is None:
        raise CurrencyNotFoundError(key)
    return cur
