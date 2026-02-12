from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from valutatrade_hub.core.currencies import get_currency
from valutatrade_hub.core.exceptions import ApiRequestError, CurrencyNotFoundError, InsufficientFundsError
from valutatrade_hub.core.models import Wallet
from valutatrade_hub.decorators import log_action
from valutatrade_hub.infra.settings import SettingsLoader


SETTINGS = SettingsLoader()


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _read_json(path: Path, default):
    if not path.exists():
        return default
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _write_json_atomic(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _portfolios_path() -> Path:
    return Path(SETTINGS.get("PORTFOLIOS_JSON"))


def _rates_path() -> Path:
    return Path(SETTINGS.get("RATES_JSON"))


def _ttl_seconds() -> int:
    ttl = SETTINGS.get("RATES_TTL_SECONDS", 300)
    try:
        ttl = int(ttl)
    except Exception:
        ttl = 300
    return ttl if ttl > 0 else 300


def _base_currency() -> str:
    base = SETTINGS.get("BASE_CURRENCY", "USD")
    if not isinstance(base, str) or not base.strip():
        return "USD"
    return base.strip().upper()


def _fetch_rates() -> dict[str, float]:
    return {
        "USD": 1.0,
        "EUR": 1.07,
        "BTC": 59300.0,
        "ETH": 2500.0,
        "RUB": 0.011,
    }


def _ensure_rates_fresh() -> tuple[dict[str, float], datetime]:
    path = _rates_path()
    cache = _read_json(path, {})
    if not isinstance(cache, dict):
        cache = {}

    updated_at = _parse_dt(cache.get("updated_at"))
    rates = cache.get("rates")
    if isinstance(rates, dict):
        ok_rates = {}
        for k, v in rates.items():
            if isinstance(k, str) and isinstance(v, (int, float)):
                ok_rates[k.strip().upper()] = float(v)
        rates = ok_rates
    else:
        rates = {}

    now = _now()
    ttl = _ttl_seconds()

    if updated_at is not None and rates and (now - updated_at).total_seconds() <= ttl:
        return rates, updated_at

    try:
        fresh = _fetch_rates()
    except Exception as e:
        raise ApiRequestError(str(e))

    if not isinstance(fresh, dict) or not fresh:
        raise ApiRequestError("empty rates")

    updated_at = now
    payload = {"updated_at": _format_dt(updated_at), "rates": fresh}
    _write_json_atomic(path, payload)
    return fresh, updated_at


def _load_portfolios() -> list[dict]:
    data = _read_json(_portfolios_path(), [])
    if isinstance(data, list):
        out = []
        for item in data:
            if isinstance(item, dict):
                out.append(item)
        return out
    return []


def _save_portfolios(portfolios: list[dict]) -> None:
    _write_json_atomic(_portfolios_path(), portfolios)


def _find_portfolio(portfolios: list[dict], user_id: int) -> tuple[int | None, dict | None]:
    for i, p in enumerate(portfolios):
        if isinstance(p, dict) and p.get("user_id") == user_id:
            return i, p
    return None, None


def _ensure_portfolio(portfolios: list[dict], user_id: int) -> tuple[int, dict]:
    idx, p = _find_portfolio(portfolios, user_id)
    if idx is not None and isinstance(p, dict):
        wallets = p.get("wallets")
        if not isinstance(wallets, dict):
            p["wallets"] = {}
        return idx, p
    p = {"user_id": user_id, "wallets": {}}
    portfolios.append(p)
    return len(portfolios) - 1, p


def _get_wallet_entry(portfolio: dict, code: str, create: bool) -> dict | None:
    wallets = portfolio.get("wallets")
    if not isinstance(wallets, dict):
        wallets = {}
        portfolio["wallets"] = wallets
    entry = wallets.get(code)
    if isinstance(entry, dict):
        return entry
    if create:
        entry = {"balance": 0.0}
        wallets[code] = entry
        return entry
    return None


def get_setting(key: str, default: Any = None) -> Any:
    return SETTINGS.get(key, default)


@log_action("BUY", verbose=True)
def buy(
    *,
    user_id: int,
    currency_code: str,
    amount: float,
    username: str | None = None,
    base: str | None = None,
) -> dict:
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id invalid")
    if not isinstance(amount, (int, float)) or float(amount) <= 0:
        raise ValueError("amount invalid")
    amount = float(amount)

    cur = get_currency(currency_code)
    code = cur.code

    portfolios = _load_portfolios()
    idx, p = _ensure_portfolio(portfolios, user_id)
    entry = _get_wallet_entry(p, code, create=True)
    if not isinstance(entry, dict):
        raise ValueError("wallet error")

    before = entry.get("balance", 0.0)
    if not isinstance(before, (int, float)):
        before = 0.0
    before = float(before)

    wallet = Wallet(code, before)
    wallet.deposit(amount)
    after = wallet.balance

    entry["balance"] = after
    portfolios[idx] = p
    _save_portfolios(portfolios)

    rates, _ = _ensure_rates_fresh()
    base_ccy = (base.strip().upper() if isinstance(base, str) and base.strip() else _base_currency())
    if base_ccy not in rates:
        base_ccy = "USD"
    if code not in rates or rates[base_ccy] == 0:
        raise ApiRequestError("rates unavailable")

    rate = float(rates[code]) / float(rates[base_ccy])
    cost_base = amount * rate

    return {
        "action": "BUY",
        "user_id": user_id,
        "username": username,
        "currency_code": code,
        "amount": amount,
        "rate": rate,
        "base": base_ccy,
        "estimated_cost": cost_base,
        "before_balance": before,
        "after_balance": after,
        "result": "OK",
    }


@log_action("SELL", verbose=True)
def sell(
    *,
    user_id: int,
    currency_code: str,
    amount: float,
    username: str | None = None,
    base: str | None = None,
) -> dict:
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id invalid")
    if not isinstance(amount, (int, float)) or float(amount) <= 0:
        raise ValueError("amount invalid")
    amount = float(amount)

    cur = get_currency(currency_code)
    code = cur.code

    portfolios = _load_portfolios()
    idx, p = _find_portfolio(portfolios, user_id)
    if idx is None or not isinstance(p, dict):
        if code in ("BTC", "ETH"):
            raise InsufficientFundsError(available="0.0000", required=f"{amount:.4f}", code=code)
        raise InsufficientFundsError(available="0.00", required=f"{amount:.2f}", code=code)

    entry = _get_wallet_entry(p, code, create=False)
    if not isinstance(entry, dict):
        if code in ("BTC", "ETH"):
            raise InsufficientFundsError(available="0.0000", required=f"{amount:.4f}", code=code)
        raise InsufficientFundsError(available="0.00", required=f"{amount:.2f}", code=code)

    before = entry.get("balance", 0.0)
    if not isinstance(before, (int, float)):
        before = 0.0
    before = float(before)

    wallet = Wallet(code, before)
    wallet.withdraw(amount)
    after = wallet.balance

    entry["balance"] = after
    portfolios[idx] = p
    _save_portfolios(portfolios)

    rates, _ = _ensure_rates_fresh()
    base_ccy = (base.strip().upper() if isinstance(base, str) and base.strip() else _base_currency())
    if base_ccy not in rates:
        base_ccy = "USD"
    if code not in rates or rates[base_ccy] == 0:
        raise ApiRequestError("rates unavailable")

    rate = float(rates[code]) / float(rates[base_ccy])
    revenue_base = amount * rate

    return {
        "action": "SELL",
        "user_id": user_id,
        "username": username,
        "currency_code": code,
        "amount": amount,
        "rate": rate,
        "base": base_ccy,
        "estimated_revenue": revenue_base,
        "before_balance": before,
        "after_balance": after,
        "result": "OK",
    }


def get_rate(from_code: str, to_code: str) -> dict:
    try:
        f = get_currency(from_code).code
    except CurrencyNotFoundError:
        raise
    except Exception:
        raise CurrencyNotFoundError(str(from_code).strip().upper() if isinstance(from_code, str) else str(from_code))

    try:
        t = get_currency(to_code).code
    except CurrencyNotFoundError:
        raise
    except Exception:
        raise CurrencyNotFoundError(str(to_code).strip().upper() if isinstance(to_code, str) else str(to_code))

    rates, updated_at = _ensure_rates_fresh()

    if f not in rates or t not in rates:
        raise ApiRequestError("rate pair unavailable")
    if float(rates[t]) == 0:
        raise ApiRequestError("division by zero")

    rate = float(rates[f]) / float(rates[t])

    return {
        "from": f,
        "to": t,
        "rate": rate,
        "updated_at": _format_dt(updated_at),
    }
