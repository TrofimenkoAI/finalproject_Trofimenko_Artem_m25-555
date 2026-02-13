from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from valutatrade_hub.infra.settings import SettingsLoader

SETTINGS = SettingsLoader()


def _project_root() -> Path:
    return Path(SETTINGS.get("PROJECT_ROOT"))


def _exchange_rates_path() -> Path:
    p = SETTINGS.get("EXCHANGE_RATES_JSON", None)
    if isinstance(p, str) and p.strip():
        path = Path(p.strip())
        return path if path.is_absolute() else (_project_root() / path)
    return _project_root() / "data" / "exchange_rates.json"


def _rates_snapshot_path() -> Path:
    return Path(SETTINGS.get("RATES_JSON"))


def _ttl_seconds() -> int:
    ttl = SETTINGS.get("RATES_TTL_SECONDS", 3000)
    try:
        ttl = int(ttl)
    except Exception:
        ttl = 3000
    return ttl if ttl > 0 else 3000


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _format_dt(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


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
        s2 = s[:-1]
        try:
            dt = datetime.fromisoformat(s2)
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


def _write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _read_json_list(path: Path) -> list[dict]:
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out = []
    for x in data:
        if isinstance(x, dict):
            out.append(x)
    return out


def _read_json_dict(path: Path) -> dict:
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


def _is_code(code: Any) -> bool:
    if not isinstance(code, str):
        return False
    s = code.strip().upper()
    if " " in s or not (2 <= len(s) <= 5) or not s.isalnum():
        return False
    return True


def _normalize_code(code: Any) -> str:
    if not _is_code(code):
        raise ValueError("invalid currency code")
    return str(code).strip().upper()


def _normalize_timestamp(ts: Any) -> str:
    if isinstance(ts, datetime):
        dt = ts
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(microsecond=0)
        return _format_dt(dt)

    if not isinstance(ts, str) or not ts.strip():
        raise ValueError("invalid timestamp")

    s = ts.strip()

    if s.endswith("Z"):
        dt = _parse_dt(s)
        if dt is None:
            raise ValueError("invalid timestamp")
        return _format_dt(dt)

    dt = _parse_dt(s)
    if dt is None:
        raise ValueError("invalid timestamp")
    return _format_dt(dt)


def make_measurement_id(from_currency: str, to_currency: str, timestamp_utc: str) -> str:
    f = _normalize_code(from_currency)
    t = _normalize_code(to_currency)
    ts = _normalize_timestamp(timestamp_utc)
    return f"{f}_{t}_{ts}"


def validate_measurement(record: dict) -> dict:
    if not isinstance(record, dict):
        raise ValueError("record must be dict")

    f = _normalize_code(record.get("from_currency"))
    t = _normalize_code(record.get("to_currency"))
    ts = _normalize_timestamp(record.get("timestamp"))

    rate = record.get("rate")
    if not isinstance(rate, (int, float)):
        raise ValueError("rate invalid")
    rate = float(rate)
    if rate <= 0:
        raise ValueError("rate invalid")

    source = record.get("source")
    if not isinstance(source, str) or not source.strip():
        raise ValueError("source invalid")
    source = source.strip()

    meta = record.get("meta", {})
    if meta is None:
        meta = {}
    if not isinstance(meta, dict):
        raise ValueError("meta invalid")

    expected_id = make_measurement_id(f, t, ts)
    rec_id = record.get("id")
    if rec_id is None or (isinstance(rec_id, str) and not rec_id.strip()):
        rec_id = expected_id
    if not isinstance(rec_id, str) or rec_id.strip() != expected_id:
        raise ValueError("id invalid")

    return {
        "id": expected_id,
        "from_currency": f,
        "to_currency": t,
        "rate": rate,
        "timestamp": ts,
        "source": source,
        "meta": meta,
    }


@dataclass(frozen=True)
class AppendResult:
    inserted: bool
    record: dict


def append_measurement(record: dict) -> AppendResult:
    valid = validate_measurement(record)
    path = _exchange_rates_path()

    items = _read_json_list(path)
    seen = set()
    for x in items:
        rid = x.get("id") if isinstance(x, dict) else None
        if isinstance(rid, str) and rid:
            seen.add(rid)

    if valid["id"] in seen:
        return AppendResult(inserted=False, record=valid)

    items.append(valid)
    _write_json_atomic(path, items)
    return AppendResult(inserted=True, record=valid)


def load_measurements(
    from_currency: str | None = None,
    to_currency: str | None = None,
    source: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    items = _read_json_list(_exchange_rates_path())

    fc = _normalize_code(from_currency) if isinstance(from_currency, str) and from_currency.strip() else None
    tc = _normalize_code(to_currency) if isinstance(to_currency, str) and to_currency.strip() else None
    src = source.strip() if isinstance(source, str) and source.strip() else None

    out = []
    for x in items:
        if not isinstance(x, dict):
            continue
        try:
            v = validate_measurement(x)
        except Exception:
            continue
        if fc is not None and v["from_currency"] != fc:
            continue
        if tc is not None and v["to_currency"] != tc:
            continue
        if src is not None and v["source"] != src:
            continue
        out.append(v)

    out.sort(key=lambda r: r["timestamp"])
    if isinstance(limit, int) and limit > 0:
        return out[-limit:]
    return out


def upsert_rates_snapshot_pair(
    *,
    from_currency: str,
    to_currency: str,
    rate: float,
    updated_at: str | datetime,
    source: str,
) -> dict:
    f = _normalize_code(from_currency)
    t = _normalize_code(to_currency)

    if not isinstance(rate, (int, float)) or float(rate) <= 0:
        raise ValueError("rate invalid")
    rate = float(rate)

    if not isinstance(source, str) or not source.strip():
        raise ValueError("source invalid")
    source = source.strip()

    ts = _normalize_timestamp(updated_at)
    new_ts = _parse_dt(ts)
    if new_ts is None:
        raise ValueError("updated_at invalid")

    path = _rates_snapshot_path()
    snap = _read_json_dict(path)
    if not isinstance(snap, dict):
        snap = {}

    pairs = snap.get("pairs")
    if not isinstance(pairs, dict):
        pairs = {}

    key = f"{f}_{t}"
    current = pairs.get(key)
    current_ts = _parse_dt(current.get("updated_at")) if isinstance(current, dict) else None

    if current_ts is not None and new_ts <= current_ts:
        snap["pairs"] = pairs
        return snap

    pairs[key] = {"rate": rate, "updated_at": ts, "source": source}
    snap["pairs"] = pairs
    _write_json_atomic(path, snap)
    return snap


def set_rates_last_refresh(timestamp_utc: str | datetime | None = None) -> dict:
    path = _rates_snapshot_path()
    snap = _read_json_dict(path)
    if not isinstance(snap, dict):
        snap = {}

    pairs = snap.get("pairs")
    if not isinstance(pairs, dict):
        pairs = {}
    snap["pairs"] = pairs

    ts = _normalize_timestamp(_now() if timestamp_utc is None else timestamp_utc)
    snap["last_refresh"] = ts
    _write_json_atomic(path, snap)
    return snap


def is_rates_snapshot_stale() -> bool:
    snap = _read_json_dict(_rates_snapshot_path())
    if not isinstance(snap, dict):
        return True

    last_refresh = _parse_dt(snap.get("last_refresh"))
    if last_refresh is None:
        return True

    age = (_now() - last_refresh).total_seconds()
    return age > _ttl_seconds()


def get_rates_snapshot_pair(from_currency: str, to_currency: str) -> dict | None:
    f = _normalize_code(from_currency)
    t = _normalize_code(to_currency)

    snap = _read_json_dict(_rates_snapshot_path())
    pairs = snap.get("pairs")
    if not isinstance(pairs, dict):
        return None

    key = f"{f}_{t}"
    val = pairs.get(key)
    if not isinstance(val, dict):
        return None

    rate = val.get("rate")
    updated_at = val.get("updated_at")
    source = val.get("source")

    if not isinstance(rate, (int, float)):
        return None
    if not isinstance(updated_at, str) or not updated_at.strip():
        return None
    if not isinstance(source, str) or not source.strip():
        return None

    return {"pair": key, "rate": float(rate), "updated_at": updated_at.strip(), "source": source.strip()}
