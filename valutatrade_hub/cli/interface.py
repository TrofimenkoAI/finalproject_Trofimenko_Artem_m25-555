import argparse
import hashlib
import json
import logging
import secrets
from datetime import datetime
from pathlib import Path

import valutatrade_hub.parser_service.storage as parser_storage
from valutatrade_hub.core.exceptions import ApiRequestError, CurrencyNotFoundError, InsufficientFundsError
from valutatrade_hub.core.usecases import buy as uc_buy
from valutatrade_hub.core.usecases import cash_out_usd as uc_cash_out_usd
from valutatrade_hub.core.usecases import deposit_usd as uc_deposit_usd
from valutatrade_hub.core.usecases import get_rate as uc_get_rate
from valutatrade_hub.core.usecases import sell as uc_sell
from valutatrade_hub.infra.settings import SettingsLoader
from valutatrade_hub.parser_service.api_clients import CoinGeckoClient, ExchangeRateApiClient
from valutatrade_hub.parser_service.config import ParserConfig
from valutatrade_hub.parser_service.updater import ClientSpec, RatesUpdater

SETTINGS = SettingsLoader()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _data_dir() -> Path:
    return _project_root() / "data"


def _session_path() -> Path:
    return _data_dir() / "session.json"


#def _rates_path() -> Path:
#    return _data_dir() / "rates.json"


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


def _read_json_dict(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    data = json.loads(text)
    if isinstance(data, dict):
        return data
    return {}


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((password + salt).encode("utf-8")).hexdigest()


#def _parse_dt(value: str) -> datetime | None:
#    if not isinstance(value, str) or not value.strip():
#        return None
#    s = value.strip()
#    try:
#        return datetime.fromisoformat(s)
#    except Exception:
#        pass
#    try:
#        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
#    except Exception:
#        return None


#def _format_dt(dt: datetime) -> str:
#    return dt.strftime("%Y-%m-%d %H:%M:%S")


def register(username: str, password: str) -> str:
    if not isinstance(username, str) or not username.strip():
        raise ValueError("username required")
    if not isinstance(password, str) or len(password) < 4:
        return "Пароль должен быть не короче 4 символов"

    username = username.strip()
    users_path = _data_dir() / "users.json"
    portfolios_path = _data_dir() / "portfolios.json"

    users = _read_json_list(users_path)
    for u in users:
        if isinstance(u, dict) and u.get("username") == username:
            return f"Имя пользователя '{username}' уже занято"

    max_id = 0
    for u in users:
        if isinstance(u, dict):
            uid = u.get("user_id")
            if isinstance(uid, int) and uid > max_id:
                max_id = uid
    user_id = max_id + 1

    salt = secrets.token_urlsafe(8)
    hashed_password = _hash_password(password, salt)
    registration_date = datetime.now().replace(microsecond=0).isoformat()

    users.append(
        {
            "user_id": user_id,
            "username": username,
            "hashed_password": hashed_password,
            "salt": salt,
            "registration_date": registration_date,
        }
    )
    _write_json(users_path, users)

    portfolios = _read_json_list(portfolios_path)
    portfolios.append({"user_id": user_id, "wallets": {"USD": {"balance": 0.0}}})
    _write_json(portfolios_path, portfolios)

    return f"Пользователь '{username}' зарегистрирован. Войдите: login --username {username} --password ****"


def login(username: str, password: str) -> str:
    if not isinstance(username, str) or not username.strip():
        raise ValueError("username required")
    if not isinstance(password, str):
        raise ValueError("password required")

    username = username.strip()
    users_path = _data_dir() / "users.json"
    users = _read_json_list(users_path)

    user = None
    for u in users:
        if isinstance(u, dict) and u.get("username") == username:
            user = u
            break

    if user is None:
        return f"Пользователь '{username}' не найден"

    salt = user.get("salt", "")
    hashed_password = user.get("hashed_password", "")
    if not isinstance(salt, str) or not isinstance(hashed_password, str):
        return "Неверный пароль"

    if _hash_password(password, salt) != hashed_password:
        return "Неверный пароль"

    user_id = user.get("user_id")
    if not isinstance(user_id, int):
        return "Неверный пароль"

    session = {
        "user_id": user_id,
        "username": username,
        "login_date": datetime.now().replace(microsecond=0).isoformat(),
    }
    _write_json(_session_path(), session)

    return f"Вы вошли как '{username}'"


def show_portfolio(base: str = "USD") -> str:
    session = _read_json_dict(_session_path())
    username = session.get("username")
    user_id = session.get("user_id")
    if not isinstance(username, str) or not isinstance(user_id, int):
        return "Сначала выполните login"

    if not isinstance(base, str) or not base.strip():
        base = "USD"
    base = base.strip().upper()

    portfolios_path = _data_dir() / "portfolios.json"
    portfolios = _read_json_list(portfolios_path)

    portfolio = None
    for p in portfolios:
        if isinstance(p, dict) and p.get("user_id") == user_id:
            portfolio = p
            break

    wallets = {}
    if isinstance(portfolio, dict):
        w = portfolio.get("wallets", {})
        if isinstance(w, dict):
            wallets = w

    if portfolio is None:
        portfolio = {"user_id": user_id, "wallets": {"USD": {"balance": 0.0}}}
        portfolios.append(portfolio)
        _write_json(portfolios_path, portfolios)

    elif isinstance(wallets, dict):
        changed = False
        if "USD" not in wallets or not isinstance(wallets.get("USD"), dict):
            wallets["USD"] = {"balance": 0.0}
            portfolio["wallets"] = wallets
            changed = True
        if changed:
            for i, p in enumerate(portfolios):
                if isinstance(p, dict) and p.get("user_id") == user_id:
                    portfolios[i] = portfolio
                    break
            _write_json(portfolios_path, portfolios)

    if not isinstance(wallets, dict) or not wallets:
        return f"Портфель пользователя '{username}' (база: {base}):\n- USD: 0.00 → 0.00 {base}\n---------------------------------\nИТОГО: 0.00 {base}"

    lines = [f"Портфель пользователя '{username}' (база: {base}):"]
    total_usd = 0.0

    for code in sorted(wallets.keys()):
        entry = wallets.get(code, {})
        if isinstance(entry, dict):
            bal = entry.get("balance", 0.0)
        else:
            bal = 0.0

        if not isinstance(bal, (int, float)):
            bal = 0.0
        bal = float(bal)

        c = str(code).strip().upper()

        cache = _read_rates_cache()
        pairs = cache.get("pairs")
        if not isinstance(pairs, dict) or not pairs:
            return "Локальный кеш курсов пуст. Выполните 'update-rates', чтобы загрузить данные."

        r = _convert_to_base(pairs, c, base, pivot="USD")
        if r is None or float(r) <= 0:
            return f"Курс для '{c}' не найден в кеше."

        base_value = bal * float(r)

        usd_r = _convert_to_base(pairs, c, "USD", pivot="USD")
        if usd_r is None or float(usd_r) <= 0:
            usd_value = 0.0
        else:
            usd_value = bal * float(usd_r)

        total_usd += usd_value


        if c in ("BTC", "ETH"):
            bal_str = f"{bal:.4f}"
        else:
            bal_str = f"{bal:.2f}"

        lines.append(f"- {c}: {bal_str}  → {base_value:.2f} {base}")

    pairs = _read_rates_cache().get("pairs")
    if not isinstance(pairs, dict) or not pairs:
        return "Локальный кеш курсов пуст. Выполните 'update-rates', чтобы загрузить данные."

    usd_to_base = _convert_to_base(pairs, "USD", base, pivot="USD")
    if usd_to_base is None or float(usd_to_base) <= 0:
        return f"Курс для 'USD→{base}' не найден в кеше."

    total_base = total_usd * float(usd_to_base)

    lines.append("---------------------------------")
    lines.append(f"ИТОГО: {total_base:,.2f} {base}")
    return "\n".join(lines)


def buy(currency: str, amount: float) -> str:
    session = _read_json_dict(_session_path())
    username = session.get("username")
    user_id = session.get("user_id")
    if not isinstance(username, str) or not isinstance(user_id, int):
        return "Сначала выполните login"

    if not isinstance(currency, str) or not currency.strip():
        raise ValueError("currency required")
    c = currency.strip().upper()
    if c == "USD":
        return "Нельзя покупать USD (это базовая валюта-кэш). Используйте: deposit <amount>"

    if not c.isalnum() or len(c) < 2:
        raise ValueError("currency required")

    if not isinstance(amount, (int, float)):
        return "'amount' должен быть положительным числом"
    amount = float(amount)
    if amount <= 0:
        return "'amount' должен быть положительным числом"

    base = SETTINGS.get("BASE_CURRENCY", "USD")
    if not isinstance(base, str) or not base.strip():
        base = "USD"
    base = base.strip().upper()

    result = uc_buy(
        user_id=user_id,
        username=username,
        currency_code=c,
        amount=amount,
        base=base,
    )

    code = str(result.get("currency_code", c)).upper()
    rate = float(result.get("rate", 0.0))
    used_base = str(result.get("base", base)).upper()
    cost = result.get("estimated_cost", None)

    old_balance = float(result.get("before_balance", 0.0))
    new_balance = float(result.get("after_balance", 0.0))

    if code in ("BTC", "ETH"):
        a_str = f"{amount:.4f}"
        old_str = f"{old_balance:.4f}"
        new_str = f"{new_balance:.4f}"
    else:
        a_str = f"{amount:.2f}"
        old_str = f"{old_balance:.2f}"
        new_str = f"{new_balance:.2f}"

    cost_str = ""
    if isinstance(cost, (int, float)):
        cost_str = f"{float(cost):,.2f} {used_base}"

    lines = [
        f"Покупка выполнена: {a_str} {code}",
        f"Курс: {rate:.8f} {used_base}/{code}",
        "Изменения в портфеле:",
        f"- {code}: было {old_str} → стало {new_str}",
    ]
    if cost_str:
        lines.append(f"Оценочная стоимость: {cost_str}")
    return "\n".join(lines)


def sell(currency: str, amount: float) -> str:
    session = _read_json_dict(_session_path())
    username = session.get("username")
    user_id = session.get("user_id")
    if not isinstance(username, str) or not isinstance(user_id, int):
        return "Сначала выполните login"

    if not isinstance(currency, str) or not currency.strip():
        raise ValueError("currency required")
    c = currency.strip().upper()
    if c == "USD":
        return "Нельзя продавать USD (это базовая валюта-кэш). Продавать можно только не-USD валюты, выручка зачисляется в USD."

    if not c.isalnum() or len(c) < 2:
        raise ValueError("currency required")

    if not isinstance(amount, (int, float)):
        return "'amount' должен быть положительным числом"
    amount = float(amount)
    if amount <= 0:
        return "'amount' должен быть положительным числом"

    base = SETTINGS.get("BASE_CURRENCY", "USD")
    if not isinstance(base, str) or not base.strip():
        base = "USD"
    base = base.strip().upper()

    result = uc_sell(
        user_id=user_id,
        username=username,
        currency_code=c,
        amount=amount,
        base=base,
    )

    code = str(result.get("currency_code", c)).upper()
    rate = float(result.get("rate", 0.0))
    used_base = str(result.get("base", base)).upper()
    revenue = result.get("estimated_revenue", None)

    old_balance = float(result.get("before_balance", 0.0))
    new_balance = float(result.get("after_balance", 0.0))

    if code in ("BTC", "ETH"):
        a_str = f"{amount:.4f}"
        old_str = f"{old_balance:.4f}"
        new_str = f"{new_balance:.4f}"
    else:
        a_str = f"{amount:.2f}"
        old_str = f"{old_balance:.2f}"
        new_str = f"{new_balance:.2f}"

    rev_str = ""
    if isinstance(revenue, (int, float)):
        rev_str = f"{float(revenue):,.2f} {used_base}"

    lines = [
        f"Продажа выполнена: {a_str} {code}",
        f"Курс: {rate:.8f} {used_base}/{code}",
        "Изменения в портфеле:",
        f"- {code}: было {old_str} → стало {new_str}",
    ]
    if rev_str:
        lines.append(f"Оценочная выручка: {rev_str}")
    return "\n".join(lines)


def deposit(amount: float) -> str:
    session = _read_json_dict(_session_path())
    username = session.get("username")
    user_id = session.get("user_id")
    if not isinstance(username, str) or not isinstance(user_id, int):
        return "Сначала выполните login"

    if not isinstance(amount, (int, float)):
        return "'amount' должен быть положительным числом"
    amount = float(amount)
    if amount <= 0:
        return "'amount' должен быть положительным числом"

    result = uc_deposit_usd(user_id=user_id, username=username, amount=amount)
    before = float(result.get("before_balance", 0.0))
    after = float(result.get("after_balance", 0.0))

    return "\n".join(
        [
            f"USD пополнен на: {amount:.2f} USD",
            "Изменения в портфеле:",
            f"- USD: было {before:.2f} → стало {after:.2f}",
        ]
    )

def cash_out(amount: float) -> str:
    session = _read_json_dict(_session_path())
    username = session.get("username")
    user_id = session.get("user_id")
    if not isinstance(username, str) or not isinstance(user_id, int):
        return "Сначала выполните login"

    if not isinstance(amount, (int, float)):
        return "'amount' должен быть положительным числом"
    amount = float(amount)
    if amount <= 0:
        return "'amount' должен быть положительным числом"

    result = uc_cash_out_usd(user_id=user_id, username=username, amount=amount)
    before = float(result.get("before_balance", 0.0))
    after = float(result.get("after_balance", 0.0))

    return "\n".join(
        [
            f"USD выведен: {amount:.2f} USD",
            "Изменения в портфеле:",
            f"- USD: было {before:.2f} → стало {after:.2f}",
        ]
    )


def get_rate(from_currency: str, to_currency: str) -> str:
    data = uc_get_rate(from_currency, to_currency)
    rate = float(data["rate"])
    updated_at = str(data.get("updated_at", ""))
    fc = str(data["from"])
    tc = str(data["to"])
    inv = 1.0 / rate if rate != 0 else 0.0
    return f"Курс {fc}→{tc}: {rate:.8f} (обновлено: {updated_at})\nОбратный курс {tc}→{fc}: {inv:.8f}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="valutatrade")
    subparsers = parser.add_subparsers(dest="command")

    p_register = subparsers.add_parser("register")
    p_register.add_argument("username", nargs="?", default=None)
    p_register.add_argument("password", nargs="?", default=None)
    p_register.add_argument("--username", dest="username_opt", default=None)
    p_register.add_argument("--password", dest="password_opt", default=None)

    p_login = subparsers.add_parser("login")
    p_login.add_argument("username", nargs="?", default=None)
    p_login.add_argument("password", nargs="?", default=None)
    p_login.add_argument("--username", dest="username_opt", default=None)
    p_login.add_argument("--password", dest="password_opt", default=None)

    p_show = subparsers.add_parser("show-portfolio")
    p_show.add_argument("--base", default="USD")

    p_dep = subparsers.add_parser("deposit")
    p_dep.add_argument("amount", nargs="?", default=None)
    p_dep.add_argument("--amount", dest="amount_opt", default=None)

    p_co = subparsers.add_parser("cash-out")
    p_co.add_argument("amount", nargs="?", default=None)
    p_co.add_argument("--amount", dest="amount_opt", default=None)

    p_buy = subparsers.add_parser("buy")
    p_buy.add_argument("currency", nargs="?", default=None)
    p_buy.add_argument("amount", nargs="?", default=None)
    p_buy.add_argument("--currency", dest="currency_opt", default=None)
    p_buy.add_argument("--amount", dest="amount_opt", default=None)

    p_sell = subparsers.add_parser("sell")
    p_sell.add_argument("currency", nargs="?", default=None)
    p_sell.add_argument("amount", nargs="?", default=None)
    p_sell.add_argument("--currency", dest="currency_opt", default=None)
    p_sell.add_argument("--amount", dest="amount_opt", default=None)

    p_rate = subparsers.add_parser("get-rate")
    p_rate.add_argument("from_currency", nargs="?", default=None)
    p_rate.add_argument("to_currency", nargs="?", default=None)
    p_rate.add_argument("--from", dest="from_opt", default=None)
    p_rate.add_argument("--to", dest="to_opt", default=None)

    p_update = subparsers.add_parser("update-rates")
    p_update.add_argument("--source", default=None)

    p_show_rates = subparsers.add_parser("show-rates")
    p_show_rates.add_argument("--currency", default=None)
    p_show_rates.add_argument("--top", default=None)
    p_show_rates.add_argument("--base", default=None)


    return parser


def _pick_arg(primary, opt):
    return opt if opt not in (None, "") else primary


def _supported_codes_str() -> str:
    cfg = ParserConfig()
    codes = set()
    for x in getattr(cfg, "FIAT_CURRENCIES", ()):
        if isinstance(x, str) and x.strip():
            codes.add(x.strip().upper())
    for x in getattr(cfg, "CRYPTO_CURRENCIES", ()):
        if isinstance(x, str) and x.strip():
            codes.add(x.strip().upper())
    codes.add("USD")
    return ", ".join(sorted(codes))


def _read_rates_cache() -> dict:
    path = Path(SETTINGS.get("RATES_JSON"))
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _fmt_rate(x: float) -> str:
    try:
        v = float(x)
    except Exception:
        return "?"
    s = f"{v:.8f}"
    s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def _norm_code(code: str) -> str | None:
    if not isinstance(code, str) or not code.strip():
        return None
    s = code.strip().upper()
    if " " in s or not (2 <= len(s) <= 5) or not s.isalnum():
        return None
    return s


def _get_pair_rate(pairs: dict, f: str, t: str) -> float | None:
    key = f"{f}_{t}"
    val = pairs.get(key)
    if isinstance(val, dict) and isinstance(val.get("rate"), (int, float)):
        r = float(val["rate"])
        return r if r > 0 else None
    inv = pairs.get(f"{t}_{f}")
    if isinstance(inv, dict) and isinstance(inv.get("rate"), (int, float)):
        r = float(inv["rate"])
        if r > 0:
            return 1.0 / r
    return None


def _convert_to_base(pairs: dict, from_code: str, target_base: str, pivot: str = "USD") -> float | None:
    f = from_code
    b = target_base
    if f == b:
        return 1.0
    if b == pivot:
        return _get_pair_rate(pairs, f, pivot)

    f_p = _get_pair_rate(pairs, f, pivot)
    b_p = _get_pair_rate(pairs, b, pivot)
    if f_p is None or b_p is None or b_p == 0:
        return None
    return f_p / b_p


def execute(args) -> str:
    try:
        if args.command == "register":
            u = _pick_arg(getattr(args, "username", None), getattr(args, "username_opt", None))
            p = _pick_arg(getattr(args, "password", None), getattr(args, "password_opt", None))
            if not u or not p:
                return "Использование: register --username <name> --password <pass>"
            return register(u, p)

        if args.command == "login":
            u = _pick_arg(getattr(args, "username", None), getattr(args, "username_opt", None))
            p = _pick_arg(getattr(args, "password", None), getattr(args, "password_opt", None))
            if not u or not p:
                return "Использование: login --username <name> --password <pass>"
            return login(u, p)

        if args.command == "show-portfolio":
            return show_portfolio(args.base)

        if args.command == "buy":
            c = _pick_arg(getattr(args, "currency", None), getattr(args, "currency_opt", None))
            a = _pick_arg(getattr(args, "amount", None), getattr(args, "amount_opt", None))
            if not c or a in (None, ""):
                return "Использование: buy --currency <code> --amount <num>"
            try:
                a = float(a)
            except Exception:
                return "'amount' должен быть положительным числом"
            return buy(c, a)

        if args.command == "sell":
            c = _pick_arg(getattr(args, "currency", None), getattr(args, "currency_opt", None))
            a = _pick_arg(getattr(args, "amount", None), getattr(args, "amount_opt", None))
            if not c or a in (None, ""):
                return "Использование: sell --currency <code> --amount <num>"
            try:
                a = float(a)
            except Exception:
                return "'amount' должен быть положительным числом"
            return sell(c, a)
        
        if args.command == "deposit":
            a = _pick_arg(getattr(args, "amount", None), getattr(args, "amount_opt", None))
            try:
                amount = float(a)
            except Exception:
                return "Использование: deposit <amount>"
            return deposit(amount)

        if args.command == "cash-out":
            a = _pick_arg(getattr(args, "amount", None), getattr(args, "amount_opt", None))
            try:
                amount = float(a)
            except Exception:
                return "Использование: cash-out <amount>"
            return cash_out(amount)

        if args.command == "get-rate":
            f = _pick_arg(getattr(args, "from_currency", None), getattr(args, "from_opt", None))
            t = _pick_arg(getattr(args, "to_currency", None), getattr(args, "to_opt", None))
            if not f or not t:
                return "Использование: get-rate --from <code> --to <code>"
            return get_rate(f, t)

        if args.command == "update-rates":
            src = getattr(args, "source", None)
            if isinstance(src, str) and src.strip():
                src = src.strip().lower()
            else:
                src = None

            cfg = ParserConfig()
            clients = []
            if src is None or src == "coingecko":
                clients.append(ClientSpec("CoinGecko", CoinGeckoClient(cfg)))
            if src is None or src == "exchangerate":
                clients.append(ClientSpec("ExchangeRate-API", ExchangeRateApiClient(cfg)))

            if not clients:
                return "Неизвестный источник. Допустимо: coingecko, exchangerate"

            logging.getLogger("valutatrade.parser.cli").info("Starting rates update...")

            updater = RatesUpdater(clients=clients, storage=parser_storage)

            try:
                summary = updater.run_update()
            except ApiRequestError as e:
                return f"{str(e)}\nUpdate failed."

            lines = []
            total_ok = 0
            has_errors = False

            for item in summary.get("clients", []):
                if not isinstance(item, dict):
                    continue
                name = str(item.get("source", "UNKNOWN"))
                status = str(item.get("status", "UNKNOWN"))
                if status == "OK":
                    pairs = item.get("pairs", 0)
                    if not isinstance(pairs, int):
                        pairs = 0
                    total_ok += pairs
                    lines.append(f"INFO: Fetching from {name}... OK ({pairs} rates)")
                else:
                    has_errors = True
                    msg = str(item.get("error_message", "")).strip()
                    if msg:
                        lines.append(f"ERROR: Failed to fetch from {name}: {msg}")
                    else:
                        lines.append(f"ERROR: Failed to fetch from {name}")

            last_refresh = str(summary.get("last_refresh", "")).strip()
            if total_ok > 0:
                lines.append(f"Update successful. Total rates updated: {total_ok}. Last refresh: {last_refresh}")
            else:
                lines.append("Update completed with errors. Check logs/actions.log for details.")
            if has_errors and total_ok > 0:
                lines.append("Update completed with errors. Check logs/actions.log for details.")
            return "\n".join(lines)
        
        if args.command == "show-rates":
            data = _read_rates_cache()
            pairs = data.get("pairs")
            last_refresh = data.get("last_refresh")

            if not isinstance(pairs, dict) or not pairs:
                return "Локальный кеш курсов пуст. Выполните 'update-rates', чтобы загрузить данные."

            base_arg = getattr(args, "base", None)
            base = _norm_code(base_arg) if isinstance(base_arg, str) and base_arg.strip() else None
            if base is None:
                base = SETTINGS.get("BASE_CURRENCY", "USD")
                if not isinstance(base, str) or not base.strip():
                    base = "USD"
                base = base.strip().upper()

            cur_arg = getattr(args, "currency", None)
            currency = _norm_code(cur_arg) if isinstance(cur_arg, str) and cur_arg.strip() else None

            top_arg = getattr(args, "top", None)
            top_n = None
            if top_arg not in (None, ""):
                try:
                    top_n = int(top_arg)
                except Exception:
                    top_n = None
                if top_n is not None and top_n <= 0:
                    top_n = None

            updated_str = str(last_refresh).strip() if isinstance(last_refresh, str) and last_refresh.strip() else "unknown"
            header = f"Rates from cache (updated at {updated_str}):"

            cfg = ParserConfig()
            crypto_set = {c.strip().upper() for c in cfg.CRYPTO_CURRENCIES if isinstance(c, str) and c.strip()}

            view_rows = []

            if top_n is not None:
                items = []
                for c in sorted(crypto_set):
                    r = _convert_to_base(pairs, c, base, pivot="USD")
                    if isinstance(r, (int, float)) and float(r) > 0:
                        items.append((c, float(r)))
                items.sort(key=lambda x: x[1], reverse=True)
                items = items[:top_n]
                for c, r in items:
                    view_rows.append((f"{c}_{base}", r))
                if not view_rows:
                    return "Локальный кеш курсов пуст. Выполните 'update-rates', чтобы загрузить данные."
                lines = [header]
                for k, r in view_rows:
                    lines.append(f"- {k}: {_fmt_rate(r)}")
                return "\n".join(lines)

            if currency is not None:
                for key, obj in pairs.items():
                    if not isinstance(key, str) or not isinstance(obj, dict):
                        continue
                    parts = key.strip().upper().split("_", 1)
                    if len(parts) != 2:
                        continue
                    f, t = parts[0], parts[1]
                    if f != currency and t != currency:
                        continue

                    r = _convert_to_base(pairs, f, base, pivot="USD") if t == "USD" else None
                    if t == base and isinstance(obj.get("rate"), (int, float)):
                        r = float(obj["rate"])
                    if r is None:
                        if isinstance(obj.get("rate"), (int, float)) and t == "USD":
                            rr = float(obj["rate"])
                            if rr > 0:
                                r = _convert_to_base(pairs, f, base, pivot="USD")
                    if r is None:
                        continue
                    view_rows.append((f"{f}_{base}", float(r)))

                if not view_rows:
                    return f"Курс для '{currency}' не найден в кеше."

                view_rows.sort(key=lambda x: x[0])
                lines = [header]
                for k, r in view_rows:
                    lines.append(f"- {k}: {_fmt_rate(r)}")
                return "\n".join(lines)

            for key, obj in pairs.items():
                if not isinstance(key, str) or not isinstance(obj, dict):
                    continue
                parts = key.strip().upper().split("_", 1)
                if len(parts) != 2:
                    continue
                f, t = parts[0], parts[1]
                if t != "USD":
                    continue
                r = _convert_to_base(pairs, f, base, pivot="USD")
                if r is None:
                    continue
                view_rows.append((f"{f}_{base}", float(r)))

            if not view_rows:
                return "Локальный кеш курсов пуст. Выполните 'update-rates', чтобы загрузить данные."

            view_rows.sort(key=lambda x: x[0])
            lines = [header]
            for k, r in view_rows:
                lines.append(f"- {k}: {_fmt_rate(r)}")
            return "\n".join(lines)

        raise ValueError("unknown command")
    except InsufficientFundsError as e:
        return str(e)
    except CurrencyNotFoundError as e:
        return f"{str(e)}\nПоддерживаемые коды: {_supported_codes_str()}\nПодсказка: get-rate --from USD --to BTC"
    except ApiRequestError as e:
        return f"{str(e)}\nПовторите попытку позже."
