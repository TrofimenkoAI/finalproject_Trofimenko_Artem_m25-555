from __future__ import annotations
import hashlib
from datetime import datetime
from valutatrade_hub.core.exceptions import InsufficientFundsError



class User:
    def __init__(
        self,
        user_id: int,
        username: str,
        hashed_password: str,
        salt: str,
        registration_date: datetime,
    ):
        self._user_id = None
        self._username = None
        self._hashed_password = None
        self._salt = None
        self._registration_date = None

        self.user_id = user_id
        self.username = username
        self.salt = salt
        self.registration_date = registration_date
        self.hashed_password = hashed_password

    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        return hashlib.sha256((password + salt).encode("utf-8")).hexdigest()

    @property
    def user_id(self) -> int:
        return self._user_id

    @user_id.setter
    def user_id(self, value: int) -> None:
        if not isinstance(value, int) or value <= 0:
            raise ValueError("user_id must be a positive int")
        self._user_id = value

    @property
    def username(self) -> str:
        return self._username

    @username.setter
    def username(self, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("username cannot be empty")
        self._username = value.strip()

    @property
    def hashed_password(self) -> str:
        return self._hashed_password

    @hashed_password.setter
    def hashed_password(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("hashed_password cannot be empty")
        self._hashed_password = value

    @property
    def salt(self) -> str:
        return self._salt

    @salt.setter
    def salt(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("salt cannot be empty")
        self._salt = value

    @property
    def registration_date(self) -> datetime:
        return self._registration_date

    @registration_date.setter
    def registration_date(self, value: datetime) -> None:
        if not isinstance(value, datetime):
            raise ValueError("registration_date must be datetime")
        self._registration_date = value

    def get_user_info(self) -> dict:
        return {
            "user_id": self._user_id,
            "username": self._username,
            "salt": self._salt,
            "registration_date": self._registration_date,
        }

    def change_password(self, new_password: str) -> None:
        if not isinstance(new_password, str) or len(new_password) < 4:
            raise ValueError("password must be at least 4 characters")
        self._hashed_password = self._hash_password(new_password, self._salt)

    def verify_password(self, password: str) -> bool:
        if not isinstance(password, str) or len(password) < 4:
            return False
        return self._hashed_password == self._hash_password(password, self._salt)


class Wallet:
    def __init__(self, currency_code: str, balance: float = 0.0):
        self._currency_code = None
        self._balance = 0.0

        self.currency_code = currency_code
        self.balance = balance

    @property
    def currency_code(self) -> str:
        return self._currency_code

    @currency_code.setter
    def currency_code(self, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("currency_code cannot be empty")
        self._currency_code = value.strip().upper()

    @property
    def balance(self) -> float:
        return self._balance

    @balance.setter
    def balance(self, value: float) -> None:
        if not isinstance(value, (int, float)):
            raise ValueError("balance must be a number")
        value = float(value)
        if value < 0:
            raise ValueError("balance cannot be negative")
        self._balance = value

    def deposit(self, amount: float) -> None:
        if not isinstance(amount, (int, float)):
            raise ValueError("amount must be a number")
        amount = float(amount)
        if amount <= 0:
            raise ValueError("amount must be positive")
        self._balance = self._balance + amount

    def withdraw(self, amount: float) -> None:
        if not isinstance(amount, (int, float)):
            raise ValueError("amount must be a number")
        amount = float(amount)
        if amount <= 0:
            raise ValueError("amount must be positive")
        if amount > self._balance:
            code = self._currency_code
            if code in ("BTC", "ETH"):
                available = f"{self._balance:.4f}"
                required = f"{amount:.4f}"
            else:
                available = f"{self._balance:.2f}"
                required = f"{amount:.2f}"
            raise InsufficientFundsError(available=available, required=required, code=code)
        self._balance = self._balance - amount


    def get_balance_info(self) -> dict:
        return {"currency_code": self._currency_code, "balance": self._balance}

class Portfolio:
    def __init__(self, user: User, wallets: dict[str, Wallet] | None = None):
        if not isinstance(user, User):
            raise ValueError("user must be User")
        self._user = user
        self._user_id = user.user_id
        self._wallets: dict[str, Wallet] = {}

        if wallets:
            if not isinstance(wallets, dict):
                raise ValueError("wallets must be dict")
            for code, wallet in wallets.items():
                if not isinstance(code, str) or not code.strip():
                    raise ValueError("wallet key must be non-empty str")
                if not isinstance(wallet, Wallet):
                    raise ValueError("wallet value must be Wallet")
                key = code.strip().upper()
                if key in self._wallets:
                    raise ValueError("duplicate currency_code")
                self._wallets[key] = wallet

    @property
    def user(self) -> User:
        return self._user

    @property
    def wallets(self) -> dict[str, Wallet]:
        return dict(self._wallets)

    def add_currency(self, currency_code: str) -> Wallet:
        if not isinstance(currency_code, str) or not currency_code.strip():
            raise ValueError("currency_code cannot be empty")
        code = currency_code.strip().upper()
        if code in self._wallets:
            raise ValueError("currency_code already exists")
        wallet = Wallet(code, 0.0)
        self._wallets[code] = wallet
        return wallet

    def get_wallet(self, currency_code: str) -> Wallet:
        if not isinstance(currency_code, str) or not currency_code.strip():
            raise ValueError("currency_code cannot be empty")
        code = currency_code.strip().upper()
        if code not in self._wallets:
            raise ValueError("wallet not found")
        return self._wallets[code]

    def get_total_value(self, base_currency: str = "USD") -> float:
        if not isinstance(base_currency, str) or not base_currency.strip():
            raise ValueError("base_currency cannot be empty")
        base = base_currency.strip().upper()

        exchange_rates = {
            "USD": 1.0,
            "EUR": 1.1,
            "BTC": 50000.0,
            "ETH": 2500.0,
            "RUB": 0.011,
        }

        if base not in exchange_rates:
            raise ValueError("unknown base_currency")

        total_usd = 0.0
        for code, wallet in self._wallets.items():
            if code not in exchange_rates:
                raise ValueError("unknown currency in portfolio")
            total_usd += wallet.balance * exchange_rates[code]

        return total_usd / exchange_rates[base]