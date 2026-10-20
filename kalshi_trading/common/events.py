"""Recorder events (connects, gaps, errors, stats) and optional phone alerts via ntfy."""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from kalshi_trading.storage.raw_writer import RawWriter

log = logging.getLogger("kalshi.events")

ALERT_EVENTS = {"index_stale", "disconnected", "ws_error", "disk_low", "fatal"}


class Alerter:
    """Posts short alerts to an ntfy topic URL (free; https://ntfy.sh). At most one per
    event kind every `min_interval_s`."""

    def __init__(self, url: str | None, min_interval_s: float = 300.0):
        self.url = url
        self.min_interval_s = min_interval_s
        self._last: dict[str, float] = {}
        self._tasks: set[asyncio.Task] = set()

    def send(self, kind: str, text: str) -> None:
        if not self.url:
            return
        now = time.monotonic()
        if now - self._last.get(kind, -1e9) < self.min_interval_s:
            return
        self._last[kind] = now
        try:
            task = asyncio.get_running_loop().create_task(self._post(text))
        except RuntimeError:
            return
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _post(self, text: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    self.url, content=text.encode(), headers={"Title": "kalshi-recorder"}
                )
        except Exception:
            log.warning("alert post failed", exc_info=True)


class EventLog:
    def __init__(self, writer: RawWriter, alerter: Alerter | None = None):
        self.writer = writer
        self.alerter = alerter or Alerter(None)

    def emit(self, event: str, **fields) -> None:
        now_ns = time.time_ns()
        record = {"type": "recorder_event", "event": event, "ts_ns": now_ns, **fields}
        self.writer.write_obj(now_ns, record)
        level = logging.WARNING if event in ALERT_EVENTS or event == "seq_gap" else logging.INFO
        log.log(level, "%s %s", event, fields)
        if event in ALERT_EVENTS:
            self.alerter.send(event, f"{event}: {fields}")
