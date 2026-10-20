"""Run the whole Recorder against the fake WebSocket server and a fake REST API."""

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
from cryptography.hazmat.primitives import serialization

from kalshi_trading.collectors.rest import KalshiRest
from kalshi_trading.common.config import load_config
from kalshi_trading.recorder.main import Recorder
from kalshi_trading.storage.raw_reader import iter_envelopes
from tests.test_kalshi_ws import fake, wait_for  # noqa: F401  (fixture re-export)


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


def fake_rest_transport():
    now = datetime.now(UTC)
    markets = {
        "open": [
            {
                "ticker": "KXBTC15M-NOW",
                "status": "open",
                "open_time": iso(now - timedelta(minutes=5)),
                "close_time": iso(now + timedelta(minutes=10)),
                "floor_strike": 68000.12,
            }
        ],
        "unopened": [],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["KALSHI-ACCESS-KEY"] == "test-key-id"
        assert request.url.path == "/trade-api/v2/markets"
        status = request.url.params["status"]
        series = request.url.params["series_ticker"]
        found = [m for m in markets[status] if m["ticker"].startswith(series + "-")]
        return httpx.Response(200, json={"markets": found, "cursor": ""})

    return httpx.MockTransport(handler)


async def test_recorder_end_to_end(fake, rsa_key, tmp_path):  # noqa: F811
    key_path = tmp_path / "k.pem"
    key_path.write_bytes(
        rsa_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    cfg_path = tmp_path / "recorder.yaml"
    cfg_path.write_text(
        f"""
rest_base: https://example.invalid/trade-api/v2
ws_url: {fake.url}
data_dir: {tmp_path / "raw"}
series: [KXBTC15M]
index: {{ids: [BRTI], ids_5hz: [BRTI], stale_after_s: 30}}
"""
    )
    cfg = load_config(
        cfg_path, {"KALSHI_API_KEY_ID": "test-key-id", "KALSHI_PRIVATE_KEY_PATH": str(key_path)}
    )
    rec = Recorder(cfg)
    rec.rest = KalshiRest(
        cfg.rest_base, rec.rest.signer, client=httpx.AsyncClient(transport=fake_rest_transport())
    )
    rec.registry.rest = rec.rest

    run = asyncio.create_task(rec.run())
    await wait_for(lambda: "KXBTC15M-NOW" in fake.tickers("orderbook_delta"), timeout=5)
    await wait_for(lambda: any(s["channel"] == "cfbenchmarks_value" for s in fake.subs.values()))
    sid = fake.sid_for("cfbenchmarks_value")
    await fake.push(
        {"type": "cfbenchmarks_value", "sid": sid, "seq": 1, "msg": {"index_id": "BRTI"}}
    )
    await asyncio.sleep(0.2)
    rec.stop_requested.set()  # what SIGTERM does
    await asyncio.wait_for(run, 10)

    raw = tmp_path / "raw"
    meta = [e["m"] for f in raw.glob("market_meta/*/*.zst") for e in iter_envelopes(f)]
    assert meta[0]["market"]["ticker"] == "KXBTC15M-NOW"
    events = [e["m"]["event"] for f in raw.glob("events/*/*.zst") for e in iter_envelopes(f)]
    assert "recorder_start" in events and "subscriptions" in events
