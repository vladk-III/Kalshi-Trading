"""Decide which markets to record.

Markets roll every 15 minutes, so the set of tickers to subscribe to changes
constantly. The registry polls each series for unopened and open markets, caches
their open/close times, and answers "what should be subscribed right now?"
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from kalshi_trading.common.config import SeriesRule

log = logging.getLogger(__name__)

POLL_STATUSES = ("unopened", "open")

# Market fields that define the contract or its outcome. A market object is recorded
# when first seen and again whenever any of these change (prices are recorded by the
# WebSocket feeds, so they are ignored here).
STABLE_FIELDS = (
    "event_ticker",
    "status",
    "open_time",
    "close_time",
    "expiration_time",
    "expected_expiration_time",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "functional_strike",
    "custom_strike",
    "rules_primary",
    "rules_secondary",
    "result",
    "expiration_value",
    "settlement_value_dollars",
    "can_close_early",
)


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass(frozen=True)
class MarketWindow:
    ticker: str
    series: str
    open_time: datetime
    close_time: datetime


def is_wanted(m: MarketWindow, rule: SeriesRule, now: datetime) -> bool:
    if m.open_time > now + timedelta(minutes=rule.lookahead_min):
        return False
    if rule.horizon_hours is not None and m.close_time > now + timedelta(hours=rule.horizon_hours):
        return False
    return now <= m.close_time + timedelta(minutes=rule.linger_min)


class MarketRegistry:
    def __init__(
        self,
        rest,
        rules: tuple[SeriesRule, ...],
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.rest = rest
        self.rules = {r.ticker: r for r in rules}
        self.now = now
        self.markets: dict[str, MarketWindow] = {}
        self._fingerprints: dict[str, tuple] = {}

    async def refresh(self) -> list[dict]:
        """Poll every series. Returns market objects that are new or whose contract
        fields changed since the last poll."""
        changed = []
        for series in self.rules:
            for status in POLL_STATUSES:
                try:
                    async for raw in self.rest.iter_markets(series, status):
                        self._ingest(series, raw)
                        if self._changed(raw):
                            changed.append(raw)
                except Exception:
                    log.exception("market poll failed for %s/%s", series, status)
        self._expire()
        return changed

    def _changed(self, raw: dict) -> bool:
        ticker = raw.get("ticker")
        fingerprint = tuple(repr(raw.get(f)) for f in STABLE_FIELDS)
        if self._fingerprints.get(ticker) == fingerprint:
            return False
        self._fingerprints[ticker] = fingerprint
        return True

    def _ingest(self, series: str, raw: dict) -> None:
        open_time, close_time = parse_ts(raw.get("open_time")), parse_ts(raw.get("close_time"))
        ticker = raw.get("ticker")
        if not ticker or open_time is None or close_time is None:
            return
        self.markets[ticker] = MarketWindow(ticker, series, open_time, close_time)

    def _expire(self) -> None:
        now = self.now()
        for ticker, m in list(self.markets.items()):
            if now > m.close_time + timedelta(minutes=self.rules[m.series].linger_min):
                del self.markets[ticker]
                self._fingerprints.pop(ticker, None)

    def wanted(self) -> set[str]:
        now = self.now()
        return {t for t, m in self.markets.items() if is_wanted(m, self.rules[m.series], now)}
