from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from valutatrade_hub.decorators import log_action
from valutatrade_hub.infra.settings import SettingsLoader
from valutatrade_hub.core.exceptions import InsufficientFundsError


SETTINGS = SettingsLoader()


def _read_json_list(path: Path) -> list:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]", encoding="utf-8")
        return []
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    data = json.loads(text)
    if isinstance(data, list):
        return data
    raise ValueError("json is not a list")


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _portfolios_path() -> Path:
    return Path(SETTINGS.get("PORTFOLIOS_JSON"))


def _ensure_portfolio(portfolios: list, user_id: int) -> tuple[int, dict]:
    for i, p in enumerate(portfolios):
        if isinstance(p, dict) and p.get("user_id") == user_id:
            return i, p
    p = {"user_id": user_id, "wallets": {}}
    portfolios.append(p)
    return len(portfolios) - 1, p


def _ensure_wallet(portfolio: dict, currency_code: str) -> dict:
    wallets = portfolio.get("wallets")
    if not isinstance(wallets, dict):
        wallets = {}
        portfolio["wallets"] = wallets
    entry = wallets.get(currency_code)
    if not isinstance(entry, dict):
        entry = {"balance": 0.0}
        wallets[currency_code] = entry
    return entry


def get_setting(key: str, default: Any = None) -> Any:
    return SETTINGS.get(key, default)


@log_action("BUY", verbose=False)
def buy(
    *,
    user_id: int,
    username: str | None,
    currency_code: str,
    amount: float,
    rate: float | None = None,
    base: str | None = None,
    verbose: bool = False,
) -> dict:
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id invalid")
    if not isinstance(currency_code, str) or not currency_code.strip():
        raise ValueError("currency_code invalid")
    code = currency_code.strip().upper()
    if " " in code or not (2 <= len(code) <= 5) or not code.isalnum():
        raise ValueError("currency_code invalid")

    if not isinstance(amount, (int, float)):
        raise ValueError("amount invalid")
    amount = float(amount)
    if amount <= 0:
        raise ValueError("amount invalid")

    portfolios_path = _portfolios_path()
    portfolios = _read_json_list(portfolios_path)
    idx, p = _ensure_portfolio(portfolios, user_id)

    entry = _ensure_wallet(p, code)
    before = entry.get("balance", 0.0)
    if not isinstance(before, (int, float)):
        before = 0.0
    before = float(before)

    after = before + amount
    entry["balance"] = after

    portfolios[idx] = p
    _write_json(portfolios_path, portfolios)

    result = {
        "action": "BUY",
        "user_id": user_id,
        "username": username,
        "currency_code": code,
        "amount": amount,
        "rate": rate,
        "base": base,
        "before_balance": before,
        "after_balance": after,
        "result": "OK",
    }
    if verbose:
        result["verbose"] = True
    return result


@log_action("SELL", verbose=False)
def sell(
    *,
    user_id: int,
    username: str | None,
    currency_code: str,
    amount: float,
    rate: float | None = None,
    base: str | None = None,
    verbose: bool = False,
) -> dict:
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id invalid")
    if not isinstance(currency_code, str) or not currency_code.strip():
        raise ValueError("currency_code invalid")
    code = currency_code.strip().upper()
    if " " in code or not (2 <= len(code) <= 5) or not code.isalnum():
        raise ValueError("currency_code invalid")

    if not isinstance(amount, (int, float)):
        raise ValueError("amount invalid")
    amount = float(amount)
    if amount <= 0:
        raise ValueError("amount invalid")

    portfolios_path = _portfolios_path()
    portfolios = _read_json_list(portfolios_path)

    idx = None
    p = None
    for i, item in enumerate(portfolios):
        if isinstance(item, dict) and item.get("user_id") == user_id:
            idx = i
            p = item
            break
    if idx is None or not isinstance(p, dict):
        raise InsufficientFundsError(available="0.00", required=f"{amount:.2f}", code=code)

    wallets = p.get("wallets")
    if not isinstance(wallets, dict):
        raise InsufficientFundsError(available="0.00", required=f"{amount:.2f}", code=code)

    entry = wallets.get(code)
    if not isinstance(entry, dict):
        raise InsufficientFundsError(available="0.00", required=f"{amount:.2f}", code=code)

    before = entry.get("balance", 0.0)
    if not isinstance(before, (int, float)):
        before = 0.0
    before = float(before)

    if amount > before:
        if code in ("BTC", "ETH"):
            raise InsufficientFundsError(available=f"{before:.4f}", required=f"{amount:.4f}", code=code)
        raise InsufficientFundsError(available=f"{before:.2f}", required=f"{amount:.2f}", code=code)

    after = before - amount
    entry["balance"] = after

    portfolios[idx] = p
    _write_json(portfolios_path, portfolios)

    result = {
        "action": "SELL",
        "user_id": user_id,
        "username": username,
        "currency_code": code,
        "amount": amount,
        "rate": rate,
        "base": base,
        "before_balance": before,
        "after_balance": after,
        "result": "OK",
    }
    if verbose:
        result["verbose"] = True
    return result
