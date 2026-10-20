"""Entry point: record Kalshi market data and the CF Benchmarks index feeds 24/7.

    KALSHI_API_KEY_ID=... KALSHI_PRIVATE_KEY_PATH=/etc/kalshi/recorder.pem \\
        kalshi-recorder --config configs/recorder.yaml

Streams written under <data_dir>:
    kalshi_md/     orderbook_delta, ticker, trade, market_lifecycle_v2 (+ protocol responses)
    kalshi_index/  cfbenchmarks_value, cfbenchmarks_value_5hz
    market_meta/   REST market objects, written when new or when contract fields change
    events/        recorder events: connects, disconnects, seq gaps, errors, stats
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import shutil
import signal
import time

from kalshi_trading.collectors.kalshi_ws import IndexConnection, MarketDataConnection
from kalshi_trading.collectors.registry import MarketRegistry
from kalshi_trading.collectors.rest import KalshiRest
from kalshi_trading.common.auth import KalshiSigner
from kalshi_trading.common.config import RecorderConfig, load_config
from kalshi_trading.common.events import Alerter, EventLog
from kalshi_trading.storage.raw_writer import RawWriter, recover_partials

log = logging.getLogger("kalshi.recorder")

RECONCILE_INTERVAL_S = 5.0
MIN_REFRESH_GAP_S = 5.0
DISK_LOW_BYTES = 10e9


class Recorder:
    def __init__(self, cfg: RecorderConfig):
        self.cfg = cfg
        root, host = cfg.data_dir, cfg.host_label
        self.writers = {
            name: RawWriter(root, name, host)
            for name in ("kalshi_md", "kalshi_index", "market_meta", "events")
        }
        self.events = EventLog(self.writers["events"], Alerter(cfg.ntfy_url))
        signer = KalshiSigner.from_file(cfg.api_key_id, cfg.private_key_path)
        self.rest = KalshiRest(cfg.rest_base, signer, cfg.rest_reads_per_s)
        self.registry = MarketRegistry(self.rest, cfg.series)
        self._refresh_now = asyncio.Event()
        self.stop_requested = asyncio.Event()
        common = dict(signer=signer, events=self.events, ping_interval_s=cfg.ping_interval_s)
        self.md = MarketDataConnection(
            "md",
            cfg.ws_url,
            writer=self.writers["kalshi_md"],
            channels=cfg.market_channels,
            wanted=self.registry.wanted,
            max_markets_per_sub=cfg.max_markets_per_sub,
            record_lifecycle=cfg.record_lifecycle,
            on_lifecycle=self._on_lifecycle,
            **common,
        )
        self.index = IndexConnection(
            "index",
            cfg.ws_url,
            writer=self.writers["kalshi_index"],
            index_ids=cfg.index.ids,
            index_ids_5hz=cfg.index.ids_5hz,
            stale_after_s=cfg.index.stale_after_s,
            **common,
        )
        self._series_prefixes = tuple(f"{r.ticker}-" for r in cfg.series)

    def _on_lifecycle(self, msg: dict) -> None:
        ticker = (msg.get("msg") or {}).get("market_ticker", "")
        if ticker.startswith(self._series_prefixes):
            self._refresh_now.set()

    async def refresh_registry(self) -> None:
        changed = await self.registry.refresh()
        now_ns = time.time_ns()
        meta = self.writers["market_meta"]
        for market in changed:
            meta.write_obj(now_ns, {"type": "market_meta", "market": market})

    async def registry_loop(self) -> None:
        while True:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._refresh_now.wait(), self.cfg.registry_poll_s)
            self._refresh_now.clear()
            try:
                await self.refresh_registry()
            except Exception as e:
                self.events.emit("registry_error", error=repr(e)[:500])
            await self.md.reconcile()
            await asyncio.sleep(MIN_REFRESH_GAP_S)

    async def reconcile_loop(self) -> None:
        # Windows enter and leave the lookahead/linger range between REST polls.
        while True:
            await asyncio.sleep(RECONCILE_INTERVAL_S)
            await self.md.reconcile()

    async def flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.flush_interval_s)
            for w in self.writers.values():
                w.flush()

    async def stats_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.stats_interval_s)
            free = shutil.disk_usage(self.cfg.data_dir).free
            self.events.emit(
                "stats",
                md=dict(self.md.stats),
                index=dict(self.index.stats),
                subscribed=len(self.md.subscribed),
                tracked=len(self.registry.markets),
                lines={n: w.lines_written for n, w in self.writers.items()},
                disk_free_gb=round(free / 1e9, 1),
            )
            if free < DISK_LOW_BYTES:
                self.events.emit("disk_low", free_gb=round(free / 1e9, 1))

    async def run(self) -> None:
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        for path in recover_partials(self.cfg.data_dir):
            log.info("recovered partial file %s", path)
        self.events.emit("recorder_start", series=[r.ticker for r in self.cfg.series])
        try:
            await self.refresh_registry()
        except Exception as e:
            self.events.emit("registry_error", error=repr(e)[:500])
        tasks = [
            asyncio.create_task(coro, name=name)
            for name, coro in (
                ("md", self.md.run()),
                ("index", self.index.run()),
                ("registry", self.registry_loop()),
                ("reconcile", self.reconcile_loop()),
                ("flush", self.flush_loop()),
                ("stats", self.stats_loop()),
            )
        ]
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, self.stop_requested.set)
        stopper = asyncio.create_task(self.stop_requested.wait())
        done, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task is not stopper and task.exception() is not None:
                self.events.emit("fatal", task=task.get_name(), error=repr(task.exception()))
        await self.shutdown(tasks)

    async def shutdown(self, tasks: list[asyncio.Task]) -> None:
        self.events.emit("recorder_stop")
        await self.md.stop()
        await self.index.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.rest.aclose()
        for w in self.writers.values():
            w.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Record Kalshi market data 24/7")
    parser.add_argument("--config", default="configs/recorder.yaml")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    asyncio.run(Recorder(load_config(args.config)).run())


if __name__ == "__main__":
    main()
