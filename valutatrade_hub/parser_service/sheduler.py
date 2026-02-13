from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any

import valutatrade_hub.parser_service.storage as parser_storage
from valutatrade_hub.core.exceptions import ApiRequestError
from valutatrade_hub.parser_service.api_clients import CoinGeckoClient, ExchangeRateApiClient
from valutatrade_hub.parser_service.config import ParserConfig
from valutatrade_hub.parser_service.updater import ClientSpec, RatesUpdater


@dataclass
class SchedulerState:
    is_running: bool = False
    last_result: dict | None = None
    last_error: str | None = None


class RatesScheduler:
    def __init__(self, updater: RatesUpdater, interval_seconds: int = 300):
        self.updater = updater
        self.interval_seconds = int(interval_seconds) if isinstance(interval_seconds, int) and interval_seconds > 0 else 300
        self.logger = logging.getLogger("valutatrade.parser.scheduler")
        self._timer: threading.Timer | None = None
        self.state = SchedulerState()

    def _tick(self) -> None:
        if not self.state.is_running:
            return

        try:
            self.logger.info("Scheduled update tick start")
            result = self.updater.run_update()
            self.state.last_result = result
            self.state.last_error = None
            self.logger.info("Scheduled update tick OK")
        except ApiRequestError as e:
            self.state.last_error = str(e)
            self.logger.info(f"Scheduled update tick ERROR error_type=ApiRequestError error_message='{str(e)}'")
        except Exception as e:
            self.state.last_error = str(e)
            self.logger.info(f"Scheduled update tick ERROR error_type={type(e).__name__} error_message='{str(e)}'")

        self._schedule_next()

    def _schedule_next(self) -> None:
        if not self.state.is_running:
            return
        self._timer = threading.Timer(self.interval_seconds, self._tick)
        self._timer.daemon = True
        self._timer.start()

    def start(self) -> None:
        if self.state.is_running:
            return
        self.state.is_running = True
        self.logger.info(f"Scheduler started interval_seconds={self.interval_seconds}")
        self._schedule_next()

    def stop(self) -> None:
        self.state.is_running = False
        t = self._timer
        self._timer = None
        if t is not None:
            try:
                t.cancel()
            except Exception:
                pass
        self.logger.info("Scheduler stopped")


def build_default_updater(source: str | None = None, storage: Any = parser_storage) -> RatesUpdater:
    cfg = ParserConfig()
    src = source.strip().lower() if isinstance(source, str) and source.strip() else None

    clients = []
    if src is None or src == "coingecko":
        clients.append(ClientSpec("CoinGecko", CoinGeckoClient(cfg)))
    if src is None or src == "exchangerate":
        clients.append(ClientSpec("ExchangeRate-API", ExchangeRateApiClient(cfg)))

    return RatesUpdater(clients=clients, storage=storage)
