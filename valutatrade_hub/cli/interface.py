import argparse
import hashlib
import json
import secrets
from datetime import datetime
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _data_dir() -> Path:
    return _project_root() / "data"


def _session_path() -> Path:
    return _data_dir() / "session.json"


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="valutatrade")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_register = subparsers.add_parser("register")
    p_register.add_argument("--username", required=True)
    p_register.add_argument("--password", required=True)

    p_login = subparsers.add_parser("login")
    p_login.add_argument("--username", required=True)
    p_login.add_argument("--password", required=True)

    p_show = subparsers.add_parser("show-portfolio")
    p_show.add_argument("--base", default="USD")

    return parser


def execute(args) -> str:
    if args.command == "register":
        return register(args.username, args.password)
    if args.command == "login":
        return login(args.username, args.password)
    if args.command == "show-portfolio":
        return show_portfolio(args.base)
    raise ValueError("unknown command")
