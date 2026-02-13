from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from valutatrade_hub.core.currencies import get_currency
from valutatrade_hub.core.exceptions import ApiRequestError, CurrencyNotFoundError, InsufficientFundsError
from valutatrade_hub.core.models import Wallet
from valutatrade_hub.decorators import log_action
from valutatrade_hub.infra.settings import SettingsLoader

SETTINGS = SettingsLoader()




def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0)

    if not isinstance(value, str) or not value.strip():
        return None

    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1]
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
        return dt.replace(tzinfo=timezone.utc).replace(microsecond=0)

    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def _format_dt(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


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
    ttl = SETTINGS.get("RATES_TTL_SECONDS", 3000)
    try:
        ttl = int(ttl)
    except Exception:
        ttl = 3000
    return ttl if ttl > 0 else 3000


def _base_currency() -> str:
    base = SETTINGS.get("BASE_CURRENCY", "USD")
    if not isinstance(base, str) or not base.strip():
        return "USD"
    return base.strip().upper()


def _read_rates_snapshot() -> dict:
    data = _read_json(_rates_path(), {})
    return data if isinstance(data, dict) else {}


def _ensure_rates_fresh() -> tuple[dict[str, dict], datetime]:
    snap = _read_rates_snapshot()
    pairs = snap.get("pairs")
    if not isinstance(pairs, dict) or not pairs:
        raise ApiRequestError("Локальный кеш курсов пуст. Выполните update-rates.")

    last_refresh = _parse_dt(snap.get("last_refresh"))
    if last_refresh is None:
        raise ApiRequestError("Локальный кеш курсов пуст или повреждён. Выполните update-rates.")

    age = (_now() - last_refresh).total_seconds()
    if age > _ttl_seconds():
        raise ApiRequestError("Локальный кеш курсов устарел. Выполните update-rates.")

    return pairs, last_refresh


def _pair_rate(pairs: dict, f: str, t: str) -> float | None:
    key = f"{f}_{t}"
    v = pairs.get(key)
    if isinstance(v, dict) and isinstance(v.get("rate"), (int, float)):
        r = float(v["rate"])
        return r if r > 0 else None

    inv = pairs.get(f"{t}_{f}")
    if isinstance(inv, dict) and isinstance(inv.get("rate"), (int, float)):
        r = float(inv["rate"])
        if r > 0:
            return 1.0 / r

    return None


def _rate_to_base(pairs: dict, code: str, base: str, pivot: str = "USD") -> float:
    c = code.strip().upper()
    b = base.strip().upper()

    if c == b:
        return 1.0

    if c == pivot:
        r = _pair_rate(pairs, pivot, b)
        if r is None:
            raise ApiRequestError("rates unavailable")
        return r

    if b == pivot:
        r = _pair_rate(pairs, c, pivot)
        if r is None:
            raise ApiRequestError("rates unavailable")
        return r

    c_p = _pair_rate(pairs, c, pivot)
    b_p = _pair_rate(pairs, b, pivot)
    if c_p is None or b_p is None or b_p == 0:
        raise ApiRequestError("rates unavailable")
    return c_p / b_p



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
            wallets = {}
            p["wallets"] = wallets
        if "USD" not in wallets or not isinstance(wallets.get("USD"), dict):
            wallets["USD"] = {"balance": 0.0}
        return idx, p

    p = {"user_id": user_id, "wallets": {"USD": {"balance": 0.0}}}
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

    if code == "USD":
        raise ValueError("Нельзя покупать USD. Используйте команду пополнения USD (deposit).")

    pairs, _ = _ensure_rates_fresh()

    rate_usd_per_unit = _rate_to_base(pairs, code, "USD", pivot="USD") 
    cost_usd = amount * float(rate_usd_per_unit)

    portfolios = _load_portfolios()
    idx, p = _ensure_portfolio(portfolios, user_id)

    usd_entry = _get_wallet_entry(p, "USD", create=True)
    if not isinstance(usd_entry, dict):
        raise ValueError("wallet error")

    usd_before = usd_entry.get("balance", 0.0)
    if not isinstance(usd_before, (int, float)):
        usd_before = 0.0
    usd_before = float(usd_before)

    usd_wallet = Wallet("USD", usd_before)
    usd_wallet.withdraw(cost_usd) 
    usd_after = usd_wallet.balance
    usd_entry["balance"] = usd_after

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

    return {
        "action": "BUY",
        "user_id": user_id,
        "username": username,
        "currency_code": code,
        "amount": amount,
        "rate": float(rate_usd_per_unit),
        "base": "USD",
        "estimated_cost": float(cost_usd),
        "before_balance": before,
        "after_balance": after,
        "usd_before": usd_before,
        "usd_after": usd_after,
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

    if code == "USD":
        raise ValueError("Нельзя продавать USD. Продавать можно только не-USD валюты, выручка зачисляется в USD.")

    pairs, _ = _ensure_rates_fresh()

    rate_usd_per_unit = _rate_to_base(pairs, code, "USD", pivot="USD")
    revenue_usd = amount * float(rate_usd_per_unit)

    portfolios = _load_portfolios()
    idx, p = _ensure_portfolio(portfolios, user_id) 

    entry = _get_wallet_entry(p, code, create=False)
    if not isinstance(entry, dict):
        if code in ("BTC", "ETH"):
            raise InsufficientFundsError(available="0.0000", required=f"{amount:.4f}", code=code)
        raise InsufficientFundsError(available="0.00", required=f"{amount:.2f}", code=code)

    before = entry.get("balance", 0.0)
    if not isinstance(before, (int, float)):
        before = 0.0
    before = float(before)

    sold_wallet = Wallet(code, before)
    sold_wallet.withdraw(amount)
    after = sold_wallet.balance
    entry["balance"] = after

    usd_entry = _get_wallet_entry(p, "USD", create=True)
    if not isinstance(usd_entry, dict):
        raise ValueError("wallet error")

    usd_before = usd_entry.get("balance", 0.0)
    if not isinstance(usd_before, (int, float)):
        usd_before = 0.0
    usd_before = float(usd_before)

    usd_wallet = Wallet("USD", usd_before)
    usd_wallet.deposit(revenue_usd)
    usd_after = usd_wallet.balance
    usd_entry["balance"] = usd_after

    portfolios[idx] = p
    _save_portfolios(portfolios)

    return {
        "action": "SELL",
        "user_id": user_id,
        "username": username,
        "currency_code": code,
        "amount": amount,
        "rate": float(rate_usd_per_unit),
        "base": "USD",
        "estimated_revenue": float(revenue_usd),
        "before_balance": before,
        "after_balance": after,
        "usd_before": usd_before,
        "usd_after": usd_after,
        "result": "OK",
    }


@log_action("DEPOSIT_USD", verbose=True)
def deposit_usd(
    *,
    user_id: int,
    amount: float,
    username: str | None = None,
) -> dict:
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id invalid")
    if not isinstance(amount, (int, float)) or float(amount) <= 0:
        raise ValueError("amount invalid")
    amount = float(amount)

    portfolios = _load_portfolios()
    idx, p = _ensure_portfolio(portfolios, user_id)

    usd_entry = _get_wallet_entry(p, "USD", create=True)
    if not isinstance(usd_entry, dict):
        raise ValueError("wallet error")

    before = usd_entry.get("balance", 0.0)
    if not isinstance(before, (int, float)):
        before = 0.0
    before = float(before)

    w = Wallet("USD", before)
    w.deposit(amount)
    after = w.balance
    usd_entry["balance"] = after

    portfolios[idx] = p
    _save_portfolios(portfolios)

    return {
        "action": "DEPOSIT_USD",
        "user_id": user_id,
        "username": username,
        "amount": amount,
        "before_balance": before,
        "after_balance": after,
        "result": "OK",
    }

@log_action("CASH_OUT_USD", verbose=True)
def cash_out_usd(
    *,
    user_id: int,
    amount: float,
    username: str | None = None,
) -> dict:
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id invalid")
    if not isinstance(amount, (int, float)) or float(amount) <= 0:
        raise ValueError("amount invalid")
    amount = float(amount)

    portfolios = _load_portfolios()
    idx, p = _ensure_portfolio(portfolios, user_id)

    usd_entry = _get_wallet_entry(p, "USD", create=True)
    if not isinstance(usd_entry, dict):
        raise ValueError("wallet error")

    before = usd_entry.get("balance", 0.0)
    if not isinstance(before, (int, float)):
        before = 0.0
    before = float(before)

    w = Wallet("USD", before)
    w.withdraw(amount)
    after = w.balance

    usd_entry["balance"] = after
    portfolios[idx] = p
    _save_portfolios(portfolios)

    return {
        "action": "CASH_OUT_USD",
        "user_id": user_id,
        "username": username,
        "amount": amount,
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

    pairs, refreshed_at = _ensure_rates_fresh()
    rate = _rate_to_base(pairs, f, t, pivot="USD")

    direct = pairs.get(f"{f}_{t}")
    updated_at = None
    if isinstance(direct, dict) and isinstance(direct.get("updated_at"), str) and direct.get("updated_at").strip():
        updated_at = direct.get("updated_at").strip()
    if not updated_at:
        updated_at = _format_dt(refreshed_at)

    return {
        "from": f,
        "to": t,
        "rate": float(rate),
        "updated_at": updated_at,
    }

