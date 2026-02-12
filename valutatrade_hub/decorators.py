from __future__ import annotations

import logging
from datetime import datetime
from functools import wraps
from typing import Any, Callable


def log_action(action: str, verbose: bool = False) -> Callable:
    act = str(action).strip().upper()
    logger = logging.getLogger("valutatrade.actions")

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ts = datetime.now().replace(microsecond=0).isoformat()
            username = kwargs.get("username", None)
            user_id = kwargs.get("user_id", None)
            currency_code = kwargs.get("currency_code", kwargs.get("currency", None))
            amount = kwargs.get("amount", None)
            rate = kwargs.get("rate", None)
            base = kwargs.get("base", None)

            def _fmt_user():
                if isinstance(username, str) and username.strip():
                    return f"user='{username.strip()}'"
                if isinstance(user_id, int):
                    return f"user_id={user_id}"
                return "user='?'"

            def _fmt_ccy():
                if isinstance(currency_code, str) and currency_code.strip():
                    return f"currency='{currency_code.strip().upper()}'"
                return "currency='?'"

            def _fmt_amount():
                if isinstance(amount, (int, float)):
                    a = float(amount)
                    code = str(currency_code).strip().upper() if isinstance(currency_code, str) else ""
                    if code in ("BTC", "ETH"):
                        return f"amount={a:.4f}"
                    return f"amount={a:.2f}"
                return "amount=?"

            def _fmt_rate_base():
                parts = []
                if isinstance(rate, (int, float)):
                    parts.append(f"rate={float(rate):.2f}")
                if isinstance(base, str) and base.strip():
                    parts.append(f"base='{base.strip().upper()}'")
                return (" " + " ".join(parts)) if parts else ""

            try:
                result = func(*args, **kwargs)

                extra = ""
                if verbose and isinstance(result, dict):
                    before = result.get("before_balance", None)
                    after = result.get("after_balance", None)
                    if isinstance(before, (int, float)) and isinstance(after, (int, float)):
                        code = str(currency_code).strip().upper() if isinstance(currency_code, str) else ""
                        if code in ("BTC", "ETH"):
                            extra = f" balance={float(before):.4f}→{float(after):.4f}"
                        else:
                            extra = f" balance={float(before):.2f}→{float(after):.2f}"

                logger.info(
                    f"{ts} {act} {_fmt_user()} {_fmt_ccy()} {_fmt_amount()}{_fmt_rate_base()} result=OK{extra}"
                )
                return result
            except Exception as e:
                err_type = type(e).__name__
                err_msg = str(e).replace("\n", " ").strip()
                logger.info(
                    f"{ts} {act} {_fmt_user()} {_fmt_ccy()} {_fmt_amount()}{_fmt_rate_base()} result=ERROR error_type={err_type} error_message='{err_msg}'"
                )
                raise

        return wrapper

    return decorator
