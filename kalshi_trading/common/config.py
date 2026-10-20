"""Recorder configuration: a YAML file plus credentials from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

ENDPOINTS = {
    "prod": {
        "rest_base": "https://external-api.kalshi.com/trade-api/v2",
        "ws_url": "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
    },
    "demo": {
        "rest_base": "https://external-api.demo.kalshi.co/trade-api/v2",
        "ws_url": "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
    },
}

WS_SIGN_PATH = "/trade-api/ws/v2"


@dataclass(frozen=True)
class SeriesRule:
    """Which markets of a series to record.

    A market is tracked while all of these hold:
      open_time  <= now + lookahead_min   (subscribe shortly before it opens)
      close_time <= now + horizon_hours   (skip far-dated expiries; None = no limit)
      now        <= close_time + linger_min (keep recording briefly after close)
    """

    ticker: str
    lookahead_min: float = 20.0
    horizon_hours: float | None = None
    linger_min: float = 3.0


@dataclass(frozen=True)
class IndexConfig:
    ids: tuple[str, ...] = ("BRTI", "ETHUSD_RTI", "SOLUSD_RTI", "XRPUSD_RTI")
    ids_5hz: tuple[str, ...] = ("BRTI", "ETHUSD_RTI", "SOLUSD_RTI", "XRPUSD_RTI")
    stale_after_s: float = 3.0


@dataclass(frozen=True)
class RecorderConfig:
    api_key_id: str
    private_key_path: Path
    rest_base: str
    ws_url: str
    data_dir: Path
    series: tuple[SeriesRule, ...]
    market_channels: tuple[str, ...] = ("orderbook_delta", "ticker", "trade")
    record_lifecycle: bool = True
    index: IndexConfig = field(default_factory=IndexConfig)
    max_markets_per_sub: int = 200
    registry_poll_s: float = 30.0
    rest_reads_per_s: float = 10.0
    flush_interval_s: float = 1.0
    stats_interval_s: float = 60.0
    ping_interval_s: float = 10.0
    ntfy_url: str | None = None
    host_label: str = "a"


def load_config(path: str | Path, env: dict[str, str] | None = None) -> RecorderConfig:
    """Load YAML config. Credentials come only from the environment, never from the file."""
    env = dict(os.environ) if env is None else env
    raw = yaml.safe_load(Path(path).read_text()) or {}

    kalshi_env = env.get("KALSHI_ENV", raw.get("env", "prod"))
    if kalshi_env not in ENDPOINTS:
        raise ValueError(f"unknown env {kalshi_env!r}; expected one of {sorted(ENDPOINTS)}")
    endpoints = ENDPOINTS[kalshi_env]

    try:
        api_key_id = env["KALSHI_API_KEY_ID"]
        private_key_path = Path(env["KALSHI_PRIVATE_KEY_PATH"])
    except KeyError as e:
        raise ValueError(f"missing required environment variable {e.args[0]}") from None

    series = tuple(
        SeriesRule(**s) if isinstance(s, dict) else SeriesRule(ticker=s)
        for s in raw.get("series", [])
    )
    if not series:
        raise ValueError("config must list at least one series")

    index_raw = raw.get("index", {})
    index = IndexConfig(
        ids=tuple(index_raw.get("ids", IndexConfig.ids)),
        ids_5hz=tuple(index_raw.get("ids_5hz", IndexConfig.ids_5hz)),
        stale_after_s=float(index_raw.get("stale_after_s", IndexConfig.stale_after_s)),
    )

    optional = {
        k: raw[k]
        for k in (
            "max_markets_per_sub",
            "registry_poll_s",
            "rest_reads_per_s",
            "flush_interval_s",
            "stats_interval_s",
            "ping_interval_s",
            "ntfy_url",
            "host_label",
            "record_lifecycle",
        )
        if k in raw
    }
    if "market_channels" in raw:
        optional["market_channels"] = tuple(raw["market_channels"])

    return RecorderConfig(
        api_key_id=api_key_id,
        private_key_path=private_key_path,
        rest_base=raw.get("rest_base", endpoints["rest_base"]),
        ws_url=raw.get("ws_url", endpoints["ws_url"]),
        data_dir=Path(env.get("KALSHI_DATA_DIR", raw.get("data_dir", "data/raw"))),
        series=series,
        index=index,
        **optional,
    )
