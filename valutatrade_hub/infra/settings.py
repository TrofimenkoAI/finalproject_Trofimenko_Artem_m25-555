from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import tomllib
except Exception:
    tomllib = None


_MISSING = object()


class SettingsLoader:
    _instance = None
    _loaded = False

    def __new__(cls, *args, **kwargs):
        # Singleton через __new__: самый простой и читаемый способ, без метаклассов и без риска усложнить импорт/тестирование.
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self.__class__._loaded:
            return
        self._config: dict[str, Any] = {}
        self.reload()
        self.__class__._loaded = True

    def _project_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    def _read_pyproject(self) -> dict[str, Any]:
        root = self._project_root()
        path = root / "pyproject.toml"
        if not path.exists() or tomllib is None:
            return {}
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        tool = data.get("tool", {})
        if not isinstance(tool, dict):
            return {}
        cfg = tool.get("valutatrade", {})
        if not isinstance(cfg, dict):
            return {}
        return cfg

    def _read_config_json(self, root: Path, cfg: dict[str, Any]) -> dict[str, Any]:
        candidate = cfg.get("CONFIG_JSON", cfg.get("config_json", None))
        if isinstance(candidate, str) and candidate.strip():
            p = Path(candidate.strip())
            path = p if p.is_absolute() else (root / p)
        else:
            path = root / "config.json"

        if not path.exists():
            return {}

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    def _normalize(self, cfg: dict[str, Any]) -> dict[str, Any]:
        root = self._project_root()

        data_dir = cfg.get("DATA_DIR", cfg.get("data_dir", None))
        if not isinstance(data_dir, str) or not data_dir.strip():
            data_dir_path = root / "data"
        else:
            p = Path(data_dir.strip())
            data_dir_path = p if p.is_absolute() else (root / p)

        ttl = cfg.get("RATES_TTL_SECONDS", cfg.get("rates_ttl_seconds", 3000))
        try:
            ttl = int(ttl)
        except Exception:
            ttl = 3000
        if ttl < 0:
            ttl = 3000

        base = cfg.get("BASE_CURRENCY", cfg.get("base_currency", "USD"))
        if not isinstance(base, str) or not base.strip():
            base = "USD"
        base = base.strip().upper()

        log_dir = cfg.get("LOG_DIR", cfg.get("log_dir", None))
        if not isinstance(log_dir, str) or not log_dir.strip():
            log_dir_path = root / "logs"
        else:
            p = Path(log_dir.strip())
            log_dir_path = p if p.is_absolute() else (root / p)

        log_level = cfg.get("LOG_LEVEL", cfg.get("log_level", "INFO"))
        if not isinstance(log_level, str) or not log_level.strip():
            log_level = "INFO"
        log_level = log_level.strip().upper()

        log_format = cfg.get(
            "LOG_FORMAT",
            cfg.get("log_format", "%(asctime)s %(levelname)s %(name)s: %(message)s"),
        )
        if not isinstance(log_format, str) or not log_format:
            log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"

        users_json = cfg.get("USERS_JSON", cfg.get("users_json", str(data_dir_path / "users.json")))
        portfolios_json = cfg.get("PORTFOLIOS_JSON", cfg.get("portfolios_json", str(data_dir_path / "portfolios.json")))
        rates_json = cfg.get("RATES_JSON", cfg.get("rates_json", str(data_dir_path / "rates.json")))
        session_json = cfg.get("SESSION_JSON", cfg.get("session_json", str(data_dir_path / "session.json")))

        def _as_path(value, default_path: Path) -> str:
            if not isinstance(value, str) or not value.strip():
                return str(default_path)
            p = Path(value.strip())
            return str(p if p.is_absolute() else (root / p))

        return {
            "PROJECT_ROOT": str(root),
            "DATA_DIR": str(data_dir_path),
            "USERS_JSON": _as_path(users_json, data_dir_path / "users.json"),
            "PORTFOLIOS_JSON": _as_path(portfolios_json, data_dir_path / "portfolios.json"),
            "RATES_JSON": _as_path(rates_json, data_dir_path / "rates.json"),
            "SESSION_JSON": _as_path(session_json, data_dir_path / "session.json"),
            "RATES_TTL_SECONDS": ttl,
            "BASE_CURRENCY": base,
            "LOG_DIR": str(log_dir_path),
            "LOG_LEVEL": log_level,
            "LOG_FORMAT": log_format,
        }

    def reload(self) -> None:
        root_cfg = self._read_pyproject()
        root = self._project_root()
        json_cfg = self._read_config_json(root, root_cfg)
        merged = {}
        if isinstance(json_cfg, dict):
            merged.update(json_cfg)
        if isinstance(root_cfg, dict):
            merged.update(root_cfg)
        self._config = self._normalize(merged)

    def get(self, key: str, default: Any = _MISSING) -> Any:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("key must be non-empty str")
        k = key.strip()
        if k in self._config:
            return self._config[k]
        if default is _MISSING:
            raise KeyError(k)
        return default
