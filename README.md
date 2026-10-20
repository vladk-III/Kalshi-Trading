# Kalshi Trading

A research and trading system for Kalshi's 15-minute crypto markets, built around a
Bayesian model of the official CF Benchmarks settlement index.

- Research and strategy options: [docs/research/01-kalshi-15m-research-and-coas.md](docs/research/01-kalshi-15m-research-and-coas.md)
- System design (COAs 1 + 2 + 4): [docs/design/02-system-design-coa-1-2-4.md](docs/design/02-system-design-coa-1-2-4.md)
- Running the recorder for free: [deploy/ORACLE_FREE_SETUP.md](deploy/ORACLE_FREE_SETUP.md)

## Status

| Component | State |
|---|---|
| Recorder (Kalshi market data + index feeds → compressed raw logs) | Built, with unit and fake-server tests; **not yet run against the live Kalshi API** |
| Backfill (settled markets, trades, candles, index history) | Next |
| Parquet compactor, models, backtester, execution | Later (see design schedule) |

## Recorder

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
export KALSHI_API_KEY_ID=... KALSHI_PRIVATE_KEY_PATH=/path/to/key.pem KALSHI_DATA_DIR=data/raw
.venv/bin/kalshi-recorder --config configs/recorder.yaml
```

It writes hourly zstd-compressed NDJSON files under `data/raw/<stream>/<date>/`:

| Stream | Contents |
|---|---|
| `kalshi_md` | `orderbook_snapshot`/`orderbook_delta`, `ticker`, `trade`, `market_lifecycle_v2` for the tracked markets |
| `kalshi_index` | `cfbenchmarks_value` (1 Hz) and `cfbenchmarks_value_5hz` for BTC/ETH/SOL/XRP |
| `market_meta` | REST market objects (strikes, times, rules, results), written when new or changed |
| `events` | Connects, disconnects, sequence gaps, re-snapshots, errors, per-minute stats |

Each line is `{"rt": <receive time ns>, "h": "<host>", "m": <original message>}`. To read them:

```bash
.venv/bin/kalshi-read-raw data/raw/kalshi_index/2026-10-20/*.zst --type cfbenchmarks_value
```

## Tests

```bash
.venv/bin/ruff check . && .venv/bin/pytest -q
```

The WebSocket tests run the recorder against a local fake of Kalshi's protocol
(subscribe, update_subscription, sequence gaps, dropped connections, stale index feed).

Never commit API keys or recorded data. `.gitignore` excludes `*.pem`, `*.key`, `.env`
and `data/`.
