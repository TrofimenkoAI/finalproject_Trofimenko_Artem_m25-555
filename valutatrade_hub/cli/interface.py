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


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((password + salt).encode("utf-8")).hexdigest()


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

    return f"Вы вошли как '{username}'"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="valutatrade")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_register = subparsers.add_parser("register")
    p_register.add_argument("--username", required=True)
    p_register.add_argument("--password", required=True)

    p_login = subparsers.add_parser("login")
    p_login.add_argument("--username", required=True)
    p_login.add_argument("--password", required=True)

    return parser


def execute(args) -> str:
    if args.command == "register":
        return register(args.username, args.password)
    if args.command == "login":
        return login(args.username, args.password)
    raise ValueError("unknown command")
