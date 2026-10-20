"""Recorder connections against a local fake of Kalshi's WebSocket protocol."""

import asyncio
import contextlib

import orjson
import pytest
from websockets.asyncio.server import serve

from kalshi_trading.collectors.kalshi_ws import IndexConnection, MarketDataConnection
from kalshi_trading.common.events import EventLog
from kalshi_trading.storage.raw_reader import iter_envelopes
from kalshi_trading.storage.raw_writer import RawWriter


class FakeKalshi:
    """Speaks enough of the protocol: subscribe / update_subscription / unsubscribe."""

    def __init__(self, market_limit: int | None = None):
        self.market_limit = market_limit
        self.commands: list[dict] = []
        self.handshake_headers: list[dict] = []
        self.subs: dict[int, dict] = {}  # sid -> {"channel", "tickers"}
        self.next_sid = 1
        self.sockets = []
        self.connected = asyncio.Event()

    async def handler(self, ws):
        self.handshake_headers.append(dict(ws.request.headers))
        self.sockets.append(ws)
        self.subs.clear()
        self.connected.set()
        with contextlib.suppress(Exception):
            async for raw in ws:
                cmd = orjson.loads(raw)
                self.commands.append(cmd)
                await ws.send(orjson.dumps(self.respond(cmd)).decode())

    def respond(self, cmd):
        params, cid = cmd["params"], cmd["id"]
        if cmd["cmd"] == "subscribe":
            tickers = params.get("market_tickers", [])
            if self.market_limit and len(tickers) > self.market_limit:
                return {"id": cid, "type": "error", "msg": {"code": 26, "msg": "limit"}}
            sid = self.next_sid
            self.next_sid += 1
            self.subs[sid] = {"channel": params["channels"][0], "tickers": set(tickers)}
            return {
                "id": cid,
                "type": "subscribed",
                "msg": {"channel": params["channels"][0], "sid": sid},
            }
        if cmd["cmd"] == "update_subscription":
            (sid,) = params["sids"]
            sub = self.subs[sid]
            if params["action"] == "add_markets":
                if (
                    self.market_limit
                    and len(sub["tickers"]) + len(params["market_tickers"]) > self.market_limit
                ):
                    return {"id": cid, "type": "error", "msg": {"code": 26, "msg": "limit"}}
                sub["tickers"].update(params["market_tickers"])
            elif params["action"] == "delete_markets":
                sub["tickers"].difference_update(params["market_tickers"])
            return {
                "id": cid,
                "sid": sid,
                "type": "ok",
                "msg": {"market_tickers": sorted(sub["tickers"])},
            }
        if cmd["cmd"] == "unsubscribe":
            for sid in params["sids"]:
                self.subs.pop(sid, None)
            return {"id": cid, "type": "unsubscribed", "sid": params["sids"][0]}
        return {"id": cid, "type": "error", "msg": {"code": 5, "msg": "unknown"}}

    def sid_for(self, channel, ticker=None):
        for sid, sub in self.subs.items():
            if sub["channel"] == channel and (ticker is None or ticker in sub["tickers"]):
                return sid
        raise KeyError(channel)

    def tickers(self, channel):
        return set().union(*(s["tickers"] for s in self.subs.values() if s["channel"] == channel))

    async def push(self, msg):
        await self.sockets[-1].send(orjson.dumps(msg).decode())


@pytest.fixture
async def fake():
    fk = FakeKalshi()
    async with serve(fk.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        fk.url = f"ws://127.0.0.1:{port}"
        yield fk


async def wait_for(cond, timeout=3.0):
    async def poll():
        while not cond():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


def make_md(fake, signer, tmp_path, wanted, **kw):
    writer = RawWriter(tmp_path, "kalshi_md")
    events = EventLog(RawWriter(tmp_path, "events"))
    conn = MarketDataConnection(
        "md",
        fake.url,
        signer,
        writer,
        events,
        channels=("orderbook_delta", "ticker", "trade"),
        wanted=lambda: set(wanted),
        **kw,
    )
    return conn, writer, events


async def test_subscribes_records_and_resnapshots_on_gap(fake, signer, tmp_path):
    wanted = {"KXBTC15M-A", "KXETH15M-A"}
    conn, writer, events = make_md(fake, signer, tmp_path, wanted)
    task = asyncio.create_task(conn.run())
    await asyncio.wait_for(conn.connected.wait(), 3)

    assert fake.handshake_headers[0]["kalshi-access-key"] == "test-key-id"
    assert "kalshi-access-signature" in fake.handshake_headers[0]
    for ch in ("orderbook_delta", "ticker", "trade"):
        assert fake.tickers(ch) == wanted
    assert any(c["params"]["channels"] == ["market_lifecycle_v2"] for c in fake.commands)

    sid = fake.sid_for("orderbook_delta")
    await fake.push(
        {"type": "orderbook_snapshot", "sid": sid, "seq": 1, "msg": {"market_ticker": "KXBTC15M-A"}}
    )
    await fake.push(
        {"type": "orderbook_delta", "sid": sid, "seq": 2, "msg": {"market_ticker": "KXBTC15M-A"}}
    )
    await fake.push(
        {"type": "orderbook_delta", "sid": sid, "seq": 5, "msg": {"market_ticker": "KXBTC15M-A"}}
    )
    await wait_for(lambda: any(c["params"].get("action") == "get_snapshot" for c in fake.commands))
    snap = next(c for c in fake.commands if c["params"].get("action") == "get_snapshot")
    assert snap["params"]["sids"] == [sid]
    assert set(snap["params"]["market_tickers"]) == wanted
    assert conn.stats["seq_gaps"] == 1

    await conn.stop()
    await task
    writer.close()
    events.writer.close()
    types = [
        e["m"].get("type")
        for f in sorted(tmp_path.glob("kalshi_md/*/*"))
        for e in iter_envelopes(f)
    ]
    assert types.count("orderbook_delta") == 2 and "orderbook_snapshot" in types
    event_names = [e["m"]["event"] for f in tmp_path.glob("events/*/*") for e in iter_envelopes(f)]
    assert "seq_gap" in event_names and "resnapshot" in event_names


async def test_reconcile_adds_and_removes_markets(fake, signer, tmp_path):
    wanted = {"A", "B"}
    conn, *_ = make_md(fake, signer, tmp_path, wanted)
    task = asyncio.create_task(conn.run())
    await asyncio.wait_for(conn.connected.wait(), 3)

    wanted.discard("A")
    wanted.add("C")
    await conn.reconcile()
    for ch in ("orderbook_delta", "ticker", "trade"):
        assert fake.tickers(ch) == {"B", "C"}
    assert conn.subscribed == {"B", "C"}
    await conn.stop()
    await task


async def test_shards_when_market_limit_is_hit(fake, signer, tmp_path):
    fake.market_limit = 2
    wanted = {"A", "B"}
    conn, *_ = make_md(fake, signer, tmp_path, wanted, max_markets_per_sub=200)
    task = asyncio.create_task(conn.run())
    await asyncio.wait_for(conn.connected.wait(), 3)
    wanted.update({"C", "D", "E"})
    await conn.reconcile()
    assert conn.max_markets_per_sub == 2
    assert fake.tickers("orderbook_delta") == {"A", "B", "C", "D", "E"}
    assert len(conn.shards) == 3
    await conn.stop()
    await task


async def test_reconnects_and_resubscribes_after_drop(fake, signer, tmp_path):
    conn, *_ = make_md(fake, signer, tmp_path, {"A"})
    task = asyncio.create_task(conn.run())
    await asyncio.wait_for(conn.connected.wait(), 3)
    await fake.sockets[-1].close()
    await wait_for(lambda: len(fake.sockets) == 2 and conn.connected.is_set(), timeout=5)
    assert fake.tickers("orderbook_delta") == {"A"}
    assert conn.stats["disconnects"] == 1
    await conn.stop()
    await task


async def test_index_feed_reconnects_when_stale(fake, signer, tmp_path):
    events = EventLog(RawWriter(tmp_path, "events"))
    conn = IndexConnection(
        "index",
        fake.url,
        signer,
        RawWriter(tmp_path, "kalshi_index"),
        events,
        index_ids=("BRTI",),
        index_ids_5hz=("BRTI",),
        stale_after_s=0.3,
    )
    task = asyncio.create_task(conn.run())
    await asyncio.wait_for(conn.connected.wait(), 3)
    subs = [c["params"] for c in fake.commands if c["cmd"] == "subscribe"]
    assert {"channels": ["cfbenchmarks_value"], "index_ids": ["BRTI"]} in subs
    assert {"channels": ["cfbenchmarks_value_5hz"], "index_ids": ["BRTI"]} in subs
    # No ticks arrive, so the watchdog should force a reconnect.
    await wait_for(lambda: len(fake.sockets) >= 2, timeout=5)
    assert conn.stats["stale_events"] >= 1
    await conn.stop()
    await task
