from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from valutatrade_hub.core.exceptions import ApiRequestError
from valutatrade_hub.parser_service.api_clients import BaseApiClient


def _utc_ts() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_pair(pair_key: str) -> tuple[str, str]:
    if not isinstance(pair_key, str):
        raise ValueError("pair_key invalid")
    s = pair_key.strip().upper()
    if "_" not in s:
        raise ValueError("pair_key invalid")
    a, b = s.split("_", 1)
    a = a.strip().upper()
    b = b.strip().upper()
    if not a or not b:
        raise ValueError("pair_key invalid")
    return a, b


@dataclass(frozen=True)
class ClientSpec:
    name: str
    client: BaseApiClient


class RatesUpdater:
    def __init__(self, clients: list[ClientSpec], storage: Any):
        self.clients = clients
        self.storage = storage
        self.logger = logging.getLogger("valutatrade.parser.updater")

    def run_update(self) -> dict[str, Any]:
        ts = _utc_ts()
        self.logger.info(f"{ts} UPDATE_RATES start clients={len(self.clients)}")

        combined: dict[str, float] = {}
        per_source: dict[str, str] = {}
        client_results: list[dict[str, Any]] = []

        for spec in self.clients:
            started_ts = _utc_ts()
            try:
                self.logger.info(f"{started_ts} UPDATE_RATES client_start source='{spec.name}'")
                data = spec.client.fetch_rates()
                if not isinstance(data, dict):
                    raise ApiRequestError("client returned non-dict")

                ok = 0
                for k, v in data.items():
                    if not isinstance(k, str) or not isinstance(v, (int, float)):
                        continue
                    rate = float(v)
                    if rate <= 0:
                        continue
                    key = k.strip().upper()
                    combined[key] = rate
                    per_source[key] = spec.name
                    ok += 1

                done_ts = _utc_ts()
                self.logger.info(f"{done_ts} UPDATE_RATES client_ok source='{spec.name}' pairs={ok}")
                client_results.append({"source": spec.name, "status": "OK", "pairs": ok})
            except Exception as e:
                err_ts = _utc_ts()
                self.logger.info(
                    f"{err_ts} UPDATE_RATES source='{spec.name}' error_type={type(e).__name__} error_message='{str(e).replace(chr(10), ' ').strip()}'"
                )
                client_results.append(
                    {"source": spec.name, "status": "ERROR", "error_type": type(e).__name__, "error_message": str(e)}
                )
                continue

        if not combined:
            end_ts = _utc_ts()
            self.logger.info(f"{end_ts} UPDATE_RATES end result=ERROR error_type=ApiRequestError error_message='no rates collected'")
            raise ApiRequestError("no rates collected")

        inserted_history = 0
        updated_pairs = 0

        for pair_key, rate in combined.items():
            try:
                f, t = _split_pair(pair_key)
            except Exception:
                continue

            source = per_source.get(pair_key, "UNKNOWN")

            try:
                r = self.storage.append_measurement(
                    {
                        "from_currency": f,
                        "to_currency": t,
                        "rate": rate,
                        "timestamp": ts,
                        "source": source,
                        "meta": {},
                    }
                )
                if getattr(r, "inserted", False):
                    inserted_history += 1
            except Exception:
                pass

            try:
                self.storage.upsert_rates_snapshot_pair(
                    from_currency=f,
                    to_currency=t,
                    rate=rate,
                    updated_at=ts,
                    source=source,
                )
                updated_pairs += 1
            except Exception:
                continue

        try:
            self.storage.set_rates_last_refresh(ts)
        except Exception:
            pass

        end_ts = _utc_ts()
        self.logger.info(
            f"{end_ts} UPDATE_RATES end result=OK pairs={updated_pairs} history_inserted={inserted_history}"
        )

        return {
            "result": "OK",
            "last_refresh": ts,
            "pairs_updated": updated_pairs,
            "history_inserted": inserted_history,
            "clients": client_results,
        }
