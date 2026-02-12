from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from valutatrade_hub.infra.settings import SettingsLoader

_CONFIGURED = False


def configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    settings = SettingsLoader()
    log_dir = Path(settings.get("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    level_name = str(settings.get("LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)

    fmt = str(settings.get("LOG_FORMAT", "%(levelname)s %(message)s"))
    formatter = logging.Formatter(fmt=fmt)

    root = logging.getLogger()
    root.setLevel(level)

    actions_path = log_dir / "actions.log"
    file_handler = RotatingFileHandler(
        filename=str(actions_path),
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    _CONFIGURED = True
