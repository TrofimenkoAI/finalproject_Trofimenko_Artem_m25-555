from __future__ import annotations

from typing import Any
from valutatrade_hub.infra.settings import SettingsLoader

SETTINGS = SettingsLoader()


def get_setting(key: str, default: Any = None) -> Any:
    return SETTINGS.get(key, default)


def data_paths() -> dict[str, str]:
    return {
        "USERS_JSON": SETTINGS.get("USERS_JSON"),
        "PORTFOLIOS_JSON": SETTINGS.get("PORTFOLIOS_JSON"),
        "RATES_JSON": SETTINGS.get("RATES_JSON"),
        "SESSION_JSON": SETTINGS.get("SESSION_JSON"),
    }


def rates_policy() -> dict[str, Any]:
    return {
        "RATES_TTL_SECONDS": SETTINGS.get("RATES_TTL_SECONDS"),
        "BASE_CURRENCY": SETTINGS.get("BASE_CURRENCY"),
    }
