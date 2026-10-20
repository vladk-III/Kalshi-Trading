"""Kalshi WebSocket recorders.

Every frame is written to the raw log *before* it is parsed, so nothing a parser
bug could drop is ever lost. Parsing is used only to run the subscription
protocol, check sequence numbers and spot stale feeds.

Two connection types:
  * MarketDataConnection: orderbook_delta / ticker / trade for a changing set of
    market tickers (sharded across subscriptions), plus market_lifecycle_v2.
  * IndexConnection: cfbenchmarks_value and cfbenchmarks_value_5hz index feeds.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

import orjson
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from kalshi_trading.common.auth import KalshiSigner
from kalshi_trading.common.config import WS_SIGN_PATH
from kalshi_trading.common.events import EventLog
from kalshi_trading.storage.raw_writer import RawWriter

log = logging.getLogger(__name__)

RESPONSE_TYPES = {"subscribed", "ok", "error", "unsubscribed"}
ERR_MARKET_LIMIT = 26
ERR_BUFFER_OVERFLOW = 25


class CommandError(Exception):
    def __init__(self, code: int | None, message: str):
        super().__init__(f"code={code}: {message}")
        self.code = code


class SeqTracker:
    """Tracks the last `seq` per subscription id and reports gaps."""

    def __init__(self):
        self._last: dict[int, int] = {}

    def reset(self, sid: int | None = None) -> None:
        if sid is None:
            self._last.clear()
        else:
            self._last.pop(sid, None)

    def check(self, sid: int, seq: int) -> tuple[int, int] | None:
        """Return (expected, received) on a gap, else None."""
        last = self._last.get(sid)
        self._last[sid] = seq
        if last is not None and seq != last + 1:
            return last + 1, seq
        return None


class KalshiWsConnection:
    """Reconnecting, authenticated connection that records every frame."""

    def __init__(
        self,
        name: str,
        url: str,
        signer: KalshiSigner,
        writer: RawWriter,
        events: EventLog,
        ping_interval_s: float = 10.0,
        connect: Callable = ws_connect,
        command_timeout_s: float = 10.0,
    ):
        self.name = name
        self.url = url
        self.signer = signer
        self.writer = writer
        self.events = events
        self.ping_interval_s = ping_interval_s
        self._connect = connect
        self.command_timeout_s = command_timeout_s
        self.seq = SeqTracker()
        self.stats: Counter = Counter()
        self.connected = asyncio.Event()
        self._ws = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future] = {}
        self._stopping = False
        self._close_tasks: set[asyncio.Task] = set()

    # ---- lifecycle -------------------------------------------------------------------

    async def run(self) -> None:
        backoff = 0.5
        while not self._stopping:
            started = time.monotonic()
            disconnected_ns = None
            try:
                headers = self.signer.headers("GET", WS_SIGN_PATH)
                async with self._connect(
                    self.url,
                    additional_headers=headers,
                    ping_interval=self.ping_interval_s,
                    ping_timeout=self.ping_interval_s,
                    max_size=64 * 1024 * 1024,
                    open_timeout=15,
                ) as ws:
                    self._ws = ws
                    self.seq.reset()
                    self.stats["connects"] += 1
                    self.events.emit("connected", conn=self.name)
                    reader = asyncio.create_task(self._read_loop(ws))
                    try:
                        await self.on_connected()
                        self.connected.set()
                        await reader
                    finally:
                        reader.cancel()
                        with contextlib.suppress(asyncio.CancelledError, Exception):
                            await reader
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.events.emit("ws_error", conn=self.name, error=repr(e)[:500])
            finally:
                disconnected_ns = time.time_ns()
                self.connected.clear()
                self._ws = None
                self._fail_pending(ConnectionError("disconnected"))
                await self.on_disconnected()
            if self._stopping:
                break
            self.stats["disconnects"] += 1
            if time.monotonic() - started > 60:
                backoff = 0.5
            delay = backoff * (0.5 + random.random())
            self.events.emit(
                "disconnected", conn=self.name, at_ns=disconnected_ns, retry_in_s=round(delay, 2)
            )
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, 30.0)

    async def stop(self) -> None:
        self._stopping = True
        if self._ws is not None:
            await self._ws.close()

    async def force_reconnect(self, reason: str) -> None:
        """Close the socket without waiting, so it is safe to call from the read loop."""
        self.events.emit("force_reconnect", conn=self.name, reason=reason)
        if self._ws is not None:
            task = asyncio.create_task(self._ws.close())
            self._close_tasks.add(task)
            task.add_done_callback(self._close_tasks.discard)

    # ---- hooks for subclasses --------------------------------------------------------

    async def on_connected(self) -> None:
        pass

    async def on_disconnected(self) -> None:
        pass

    async def on_message(self, msg: dict) -> None:
        pass

    async def on_gap(self, sid: int, expected: int, received: int) -> None:
        pass

    # ---- protocol --------------------------------------------------------------------

    async def command(self, cmd: str, params: dict) -> dict:
        if self._ws is None:
            raise ConnectionError("not connected")
        cmd_id = self._next_id
        self._next_id += 1
        fut = asyncio.get_running_loop().create_future()
        self._pending[cmd_id] = fut
        await self._ws.send(orjson.dumps({"id": cmd_id, "cmd": cmd, "params": params}).decode())
        try:
            resp = await asyncio.wait_for(fut, self.command_timeout_s)
        finally:
            self._pending.pop(cmd_id, None)
        if resp.get("type") == "error":
            body = resp.get("msg") or {}
            raise CommandError(body.get("code"), str(body.get("msg", body)))
        return resp

    async def _read_loop(self, ws) -> None:
        try:
            async for frame in ws:
                recv_ns = time.time_ns()
                payload = frame.encode() if isinstance(frame, str) else frame
                self.writer.write(recv_ns, payload)
                self.stats["frames"] += 1
                try:
                    msg = orjson.loads(payload)
                except orjson.JSONDecodeError:
                    self.stats["unparseable"] += 1
                    continue
                await self._dispatch(msg)
        except ConnectionClosed:
            pass

    async def _dispatch(self, msg: dict) -> None:
        mtype = msg.get("type")
        self.stats[f"type:{mtype}"] += 1
        cmd_id = msg.get("id")
        if mtype in RESPONSE_TYPES and cmd_id in self._pending:
            fut = self._pending[cmd_id]
            if not fut.done():
                fut.set_result(msg)
        elif mtype == "error":
            code = (msg.get("msg") or {}).get("code")
            self.events.emit("ws_error", conn=self.name, error=msg)
            if code == ERR_BUFFER_OVERFLOW:
                await self.force_reconnect("subscription buffer overflow")
                return
        sid, seq = msg.get("sid"), msg.get("seq")
        if isinstance(sid, int) and isinstance(seq, int):
            gap = self.seq.check(sid, seq)
            if gap is not None:
                self.stats["seq_gaps"] += 1
                self.events.emit(
                    "seq_gap", conn=self.name, sid=sid, expected=gap[0], received=gap[1]
                )
                await self.on_gap(sid, *gap)
        await self.on_message(msg)

    def _fail_pending(self, exc: Exception) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()


@dataclass
class Shard:
    """One group of tickers subscribed together; one sid per channel."""

    tickers: set[str] = field(default_factory=set)
    sids: dict[str, int] = field(default_factory=dict)


def chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class MarketDataConnection(KalshiWsConnection):
    def __init__(
        self,
        *args,
        channels: tuple[str, ...],
        wanted: Callable[[], set[str]],
        max_markets_per_sub: int = 200,
        record_lifecycle: bool = True,
        on_lifecycle: Callable[[dict], None] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.channels = channels
        self.wanted = wanted
        self.max_markets_per_sub = max_markets_per_sub
        self.record_lifecycle = record_lifecycle
        self.on_lifecycle = on_lifecycle
        self.shards: list[Shard] = []
        self._lock = asyncio.Lock()
        self._sid_shard: dict[int, Shard] = {}
        self._sid_channel: dict[int, str] = {}
        self._snapshot_tasks: set[asyncio.Task] = set()

    @property
    def subscribed(self) -> set[str]:
        return set().union(*(s.tickers for s in self.shards)) if self.shards else set()

    async def on_connected(self) -> None:
        self.shards = []
        self._sid_shard.clear()
        self._sid_channel.clear()
        if self.record_lifecycle:
            try:
                resp = await self.command("subscribe", {"channels": ["market_lifecycle_v2"]})
                self._remember_sid(resp, "market_lifecycle_v2", None)
            except CommandError as e:
                self.events.emit(
                    "subscribe_failed", conn=self.name, channel="lifecycle", error=str(e)
                )
        await self._reconcile_locked(self.wanted())

    async def on_disconnected(self) -> None:
        self.shards = []

    async def reconcile(self) -> None:
        """Bring subscriptions in line with `wanted()`. Safe to call at any time."""
        if not self.connected.is_set():
            return
        try:
            await self._reconcile_locked(self.wanted())
        except (TimeoutError, ConnectionError) as e:
            self.events.emit("reconcile_failed", conn=self.name, error=repr(e))

    async def _reconcile_locked(self, wanted: set[str]) -> None:
        async with self._lock:
            current = self.subscribed
            to_remove, to_add = current - wanted, sorted(wanted - current)
            if to_remove:
                await self._remove(to_remove)
            if to_add:
                await self._add(to_add)
            if to_remove or to_add:
                self.events.emit(
                    "subscriptions",
                    conn=self.name,
                    added=to_add,
                    removed=sorted(to_remove),
                    total=len(self.subscribed),
                    shards=len(self.shards),
                )

    async def _remove(self, tickers: set[str]) -> None:
        for shard in list(self.shards):
            gone = sorted(shard.tickers & tickers)
            if not gone:
                continue
            if gone == sorted(shard.tickers):
                for sid in shard.sids.values():
                    with contextlib.suppress(CommandError):
                        await self.command("unsubscribe", {"sids": [sid]})
                    self._forget_sid(sid)
                self.shards.remove(shard)
                continue
            for sid in shard.sids.values():
                await self._update(sid, "delete_markets", gone)
            shard.tickers.difference_update(gone)

    async def _add(self, tickers: list[str]) -> None:
        pending = list(tickers)
        for shard in self.shards:
            room = self.max_markets_per_sub - len(shard.tickers)
            if room <= 0 or not pending:
                continue
            batch, pending = pending[:room], pending[room:]
            try:
                for sid in shard.sids.values():
                    await self._update(sid, "add_markets", batch)
                shard.tickers.update(batch)
            except CommandError as e:
                if e.code != ERR_MARKET_LIMIT:
                    raise
                self.max_markets_per_sub = max(1, len(shard.tickers))
                self.events.emit("market_limit", conn=self.name, new_cap=self.max_markets_per_sub)
                pending = batch + pending
        while pending:
            batch, pending = (
                pending[: self.max_markets_per_sub],
                pending[self.max_markets_per_sub :],
            )
            shard = Shard()
            for channel in self.channels:
                try:
                    resp = await self.command(
                        "subscribe", {"channels": [channel], "market_tickers": batch}
                    )
                except CommandError as e:
                    self.events.emit(
                        "subscribe_failed", conn=self.name, channel=channel, error=str(e)
                    )
                    continue
                self._remember_sid(resp, channel, shard)
            if shard.sids:
                shard.tickers.update(batch)
                self.shards.append(shard)

    async def _update(self, sid: int, action: str, tickers: list[str]) -> None:
        await self.command(
            "update_subscription", {"sids": [sid], "market_tickers": tickers, "action": action}
        )

    def _remember_sid(self, resp: dict, channel: str, shard: Shard | None) -> None:
        sid = (resp.get("msg") or {}).get("sid")
        if not isinstance(sid, int):
            return
        self._sid_channel[sid] = channel
        if shard is not None:
            shard.sids[channel] = sid
            self._sid_shard[sid] = shard

    def _forget_sid(self, sid: int) -> None:
        self._sid_channel.pop(sid, None)
        self._sid_shard.pop(sid, None)
        self.seq.reset(sid)

    async def on_gap(self, sid: int, expected: int, received: int) -> None:
        if self._sid_channel.get(sid) != "orderbook_delta":
            return
        shard = self._sid_shard.get(sid)
        if shard is None or not shard.tickers:
            return
        # Ask for fresh snapshots without blocking the read loop that will deliver them.
        task = asyncio.create_task(self._resnapshot(sid, sorted(shard.tickers)))
        self._snapshot_tasks.add(task)
        task.add_done_callback(self._snapshot_tasks.discard)

    async def _resnapshot(self, sid: int, tickers: list[str]) -> None:
        try:
            await self._update(sid, "get_snapshot", tickers)
            self.stats["resnapshots"] += 1
            self.events.emit("resnapshot", conn=self.name, sid=sid, markets=len(tickers))
        except Exception as e:
            self.events.emit("resnapshot_failed", conn=self.name, sid=sid, error=repr(e))
            await self.force_reconnect("resnapshot failed")

    async def on_message(self, msg: dict) -> None:
        if msg.get("type") == "market_lifecycle_v2" and self.on_lifecycle is not None:
            self.on_lifecycle(msg)


class IndexConnection(KalshiWsConnection):
    def __init__(
        self,
        *args,
        index_ids: tuple[str, ...],
        index_ids_5hz: tuple[str, ...],
        stale_after_s: float = 3.0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.index_ids = index_ids
        self.index_ids_5hz = index_ids_5hz
        self.stale_after_s = stale_after_s
        self.last_tick: dict[str, float] = {}
        self._watchdog: asyncio.Task | None = None

    async def on_connected(self) -> None:
        self.last_tick.clear()
        for channel, ids in (
            ("cfbenchmarks_value", self.index_ids),
            ("cfbenchmarks_value_5hz", self.index_ids_5hz),
        ):
            if not ids:
                continue
            try:
                await self.command("subscribe", {"channels": [channel], "index_ids": list(ids)})
            except CommandError as e:
                # The staleness watchdog will keep reconnecting and alerting.
                self.events.emit("subscribe_failed", conn=self.name, channel=channel, error=str(e))
        self._connected_at = time.monotonic()
        self._watchdog = asyncio.create_task(self._watch())

    async def on_disconnected(self) -> None:
        if self._watchdog is not None:
            self._watchdog.cancel()
            self._watchdog = None

    async def on_message(self, msg: dict) -> None:
        if msg.get("type") in ("cfbenchmarks_value", "cfbenchmarks_value_5hz"):
            index_id = (msg.get("msg") or {}).get("index_id")
            if index_id:
                self.last_tick[index_id] = time.monotonic()

    def stale_indices(self, now: float | None = None) -> list[str]:
        now = time.monotonic() if now is None else now
        grace_from = getattr(self, "_connected_at", now)
        return [
            i
            for i in self.index_ids
            if now - self.last_tick.get(i, grace_from) > self.stale_after_s
        ]

    async def _watch(self) -> None:
        while True:
            await asyncio.sleep(self.stale_after_s / 2)
            stale = self.stale_indices()
            if stale:
                self.stats["stale_events"] += 1
                self.events.emit("index_stale", conn=self.name, indices=stale)
                await self.force_reconnect(f"stale index feed: {stale}")
                return
