import argparse
import hashlib
import json
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from valutatrade_hub.core.exceptions import ApiRequestError, CurrencyNotFoundError, InsufficientFundsError



def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _data_dir() -> Path:
    return _project_root() / "data"


def _session_path() -> Path:
    return _data_dir() / "session.json"


def _rates_path() -> Path:
    return _data_dir() / "rates.json"


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


def _exchange_rates() -> dict:
    return {
        "USD": 1.0,
        "EUR": 1.07,
        "BTC": 59300.0,
        "ETH": 2500.0,
        "RUB": 0.011,
    }


def _parse_dt(value: str) -> datetime | None:
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
    portfolios.append({"user_id": user_id, "wallets": {}})
    _write_json(portfolios_path, portfolios)

    return f"Пользователь '{username}' зарегистрирован (id={user_id}). Войдите: login --username {username} --password ****"


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

    rates = _exchange_rates()
    if base not in rates:
        return f"Неизвестная базовая валюта '{base}'"

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

    if not wallets:
        return f"Портфель пользователя '{username}' (база: {base}):\nКошельков нет"

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
        if c not in rates:
            return f"Неизвестная базовая валюта '{base}'"

        usd_value = bal * rates[c]
        total_usd += usd_value
        base_value = usd_value / rates[base]

        if c in ("BTC", "ETH"):
            bal_str = f"{bal:.4f}"
        else:
            bal_str = f"{bal:.2f}"

        lines.append(f"- {c}: {bal_str}  → {base_value:.2f} {base}")

    total_base = total_usd / rates[base]
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
    if not c.isalnum() or len(c) < 2:
        raise ValueError("currency required")

    if not isinstance(amount, (int, float)):
        return "'amount' должен быть положительным числом"
    amount = float(amount)
    if amount <= 0:
        return "'amount' должен быть положительным числом"

    rates = _exchange_rates()
    base = "USD"
    if c not in rates or base not in rates:
        return f"Не удалось получить курс для {c}→{base}"

    portfolios_path = _data_dir() / "portfolios.json"
    portfolios = _read_json_list(portfolios_path)

    idx = None
    for i, p in enumerate(portfolios):
        if isinstance(p, dict) and p.get("user_id") == user_id:
            idx = i
            break

    if idx is None:
        portfolios.append({"user_id": user_id, "wallets": {}})
        idx = len(portfolios) - 1

    p = portfolios[idx]
    wallets = p.get("wallets")
    if not isinstance(wallets, dict):
        wallets = {}
        p["wallets"] = wallets

    entry = wallets.get(c)
    if not isinstance(entry, dict):
        entry = {"balance": 0.0}
        wallets[c] = entry

    old_balance = entry.get("balance", 0.0)
    if not isinstance(old_balance, (int, float)):
        old_balance = 0.0
    old_balance = float(old_balance)

    new_balance = old_balance + amount
    entry["balance"] = new_balance

    portfolios[idx] = p
    _write_json(portfolios_path, portfolios)

    rate = float(rates[c]) / float(rates[base])
    cost = amount * rate

    if c in ("BTC", "ETH"):
        a_str = f"{amount:.4f}"
        old_str = f"{old_balance:.4f}"
        new_str = f"{new_balance:.4f}"
    else:
        a_str = f"{amount:.2f}"
        old_str = f"{old_balance:.2f}"
        new_str = f"{new_balance:.2f}"

    lines = [
        f"Покупка выполнена: {a_str} {c} по курсу {rate:.2f} {base}/{c}",
        "Изменения в портфеле:",
        f"- {c}: было {old_str} → стало {new_str}",
        f"Оценочная стоимость покупки: {cost:,.2f} {base}",
    ]
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
    if not c.isalnum() or len(c) < 2:
        raise ValueError("currency required")

    if not isinstance(amount, (int, float)):
        return "'amount' должен быть положительным числом"
    amount = float(amount)
    if amount <= 0:
        return "'amount' должен быть положительным числом"

    rates = _exchange_rates()
    base = "USD"
    if c not in rates or base not in rates:
        return f"Не удалось получить курс для {c}→{base}"

    portfolios_path = _data_dir() / "portfolios.json"
    portfolios = _read_json_list(portfolios_path)

    idx = None
    for i, p in enumerate(portfolios):
        if isinstance(p, dict) and p.get("user_id") == user_id:
            idx = i
            break

    if idx is None:
        return f"У вас нет кошелька '{c}'. Добавьте валюту: она создаётся автоматически при первой покупке."

    p = portfolios[idx]
    wallets = p.get("wallets")
    if not isinstance(wallets, dict):
        return f"У вас нет кошелька '{c}'. Добавьте валюту: она создаётся автоматически при первой покупке."

    entry = wallets.get(c)
    if not isinstance(entry, dict):
        return f"У вас нет кошелька '{c}'. Добавьте валюту: она создаётся автоматически при первой покупке."

    old_balance = entry.get("balance", 0.0)
    if not isinstance(old_balance, (int, float)):
        old_balance = 0.0
    old_balance = float(old_balance)

    if amount > old_balance:
        if c in ("BTC", "ETH"):
            avail_str = f"{old_balance:.4f}"
            req_str = f"{amount:.4f}"
        else:
            avail_str = f"{old_balance:.2f}"
            req_str = f"{amount:.2f}"
        return f"Недостаточно средств: доступно {avail_str} {c}, требуется {req_str} {c}"

    new_balance = old_balance - amount
    entry["balance"] = new_balance

    portfolios[idx] = p
    _write_json(portfolios_path, portfolios)

    rate = float(rates[c]) / float(rates[base])
    revenue = amount * rate

    if c in ("BTC", "ETH"):
        a_str = f"{amount:.4f}"
        old_str = f"{old_balance:.4f}"
        new_str = f"{new_balance:.4f}"
    else:
        a_str = f"{amount:.2f}"
        old_str = f"{old_balance:.2f}"
        new_str = f"{new_balance:.2f}"

    lines = [
        f"Продажа выполнена: {a_str} {c} по курсу {rate:.2f} {base}/{c}",
        "Изменения в портфеле:",
        f"- {c}: было {old_str} → стало {new_str}",
        f"Оценочная выручка: {revenue:,.2f} {base}",
    ]
    return "\n".join(lines)


def get_rate(from_currency: str, to_currency: str) -> str:
    if not isinstance(from_currency, str) or not from_currency.strip():
        raise ValueError("from required")
    if not isinstance(to_currency, str) or not to_currency.strip():
        raise ValueError("to required")

    f = from_currency.strip().upper()
    t = to_currency.strip().upper()
    if " " in f or " " in t or not (2 <= len(f) <= 5) or not (2 <= len(t) <= 5) or not f.isalnum() or not t.isalnum():
        raise ValueError("invalid currency code")

    base_rates = _exchange_rates()
    if f not in base_rates:
        raise CurrencyNotFoundError(f)
    if t not in base_rates:
        raise CurrencyNotFoundError(t)

    now = datetime.now().replace(microsecond=0)
    cache = _read_json_dict(_rates_path())
    pairs = cache.get("pairs")
    if not isinstance(pairs, dict):
        pairs = {}

    key = f"{f}_{t}"
    pair = pairs.get(key)
    cached_rate = None
    cached_dt = None
    if isinstance(pair, dict):
        cached_rate = pair.get("rate")
        cached_dt = _parse_dt(pair.get("updated_at", ""))

    rate = None
    updated_at = None
    if isinstance(cached_rate, (int, float)) and isinstance(cached_dt, datetime):
        if now - cached_dt <= timedelta(minutes=5):
            rate = float(cached_rate)
            updated_at = cached_dt

    if rate is None:
        try:
            if base_rates[t] == 0:
                raise ApiRequestError("деление на ноль")
            rate = float(base_rates[f]) / float(base_rates[t])
            updated_at = now
            pairs[key] = {"rate": rate, "updated_at": _format_dt(updated_at)}
            cache["pairs"] = pairs
            _write_json(_rates_path(), cache)
        except ApiRequestError:
            raise
        except Exception as e:
            raise ApiRequestError(str(e))

    inv = 1.0 / rate if rate != 0 else 0.0
    return (
        f"Курс {f}→{t}: {rate:.8f} (обновлено: {_format_dt(updated_at)})\n"
        f"Обратный курс {t}→{f}: {inv:.2f}"
    )



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

    return parser



def _pick_arg(primary, opt):
    return opt if opt not in (None, "") else primary


def _supported_codes_str() -> str:
    return ", ".join(sorted(_exchange_rates().keys()))


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

        if args.command == "get-rate":
            f = _pick_arg(getattr(args, "from_currency", None), getattr(args, "from_opt", None))
            t = _pick_arg(getattr(args, "to_currency", None), getattr(args, "to_opt", None))
            if not f or not t:
                return "Использование: get-rate --from <code> --to <code>"
            return get_rate(f, t)

        raise ValueError("unknown command")
    except InsufficientFundsError as e:
        return str(e)
    except CurrencyNotFoundError as e:
        return f"{str(e)}\nПоддерживаемые коды: {_supported_codes_str()}\nПодсказка: get-rate --from USD --to BTC"
    except ApiRequestError as e:
        return f"{str(e)}\nПовторите попытку позже."
