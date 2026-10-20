# System Design: Kalshi 15-Minute Crypto Bot (COAs 1 + 2 + 4)

*Version 0.1, 2026-10-20. Companion to [research and COAs](../research/01-kalshi-15m-research-and-coas.md).*
*Target: **small-size live trading by the end of week 6**, with COA 2's order-book features (v2) added after launch.*
*Items marked **[verify]** must be confirmed against docs.kalshi.com or your account before you rely on them.*

---

## 0. Summary

- **What it does:** it prices every open Kalshi 15-minute crypto "up/down" market and the hourly `KXBTCD`/`KXBTC` ladders from a Bayesian model of the **official CF Benchmarks settlement index**. It adjusts the price with Kalshi order-flow information, scans the ladders for no-arbitrage violations and relative-value gaps, and trades only when the *lower* credible bound of the edge beats fees.
- **Why 6 weeks is feasible:**
  1. Kalshi streams the official settlement index (`cfbenchmarks_value`, `cfbenchmarks_value_5hz`) and serves its history through a REST passthrough, so there is **no index rebuild or licence work**.
  2. About 9 months of settled 15-minute markets, trades and 1-minute candles can be **backfilled on day 1**.
  3. The one dataset that can't be backfilled (order-book history) feeds only COA 2 v2, which ships after launch.
- **Data size:** about **0.5–2 GB/day compressed** (15–60 GB/month) for the recommended capture set. That is "a ton of messages" but small on disk. The hard part is **reliability** (no silent gaps), not volume. See §3.

---

## 1. Scope

| In scope (v1, live by week 6) | After launch (weeks 7–10) | Out of scope |
|---|---|---|
| COA 1 structural model for BTC, ETH, SOL, XRP 15M markets | COA 2 v2 (order-book features) | Latency racing (COA 5) |
| COA 2 v1 (trade-tape/candle features as a correction to COA 1) | More assets (DOGE, HYPE, BNB, …) | Polymarket / cross-venue trading |
| COA 4 no-arbitrage scanner + 15M-vs-ladder + COA 1-vs-ladder signals | Full Bayesian implied-distribution fit used live | Market making as a strategy |
| Recorder, backfill, replay backtester, shadow trading, risk controls, monitoring | Cross-asset relative-value trading | |

Assets: start with **BTC, ETH, SOL and XRP**, which all have 15-minute series and the 5 Hz index feed.

---

## 2. Architecture

```
                          ┌────────────────────────────── Host A (primary, US-East) ──────────────────────────────┐
 Kalshi WebSocket ──────► │ kalshi-recorder ──┐                                                                     │
  ticker, trade,          │  (market data +   │   raw envelopes        ┌───────────────┐   hourly   ┌────────────┐  │
  orderbook_delta,        │   index feeds)    ├──────────────────────► │ raw log writer│──────────► │ uploader + │──┼──► Object storage
  market_lifecycle_v2,    │                   │                        │ (.ndjson.zst) │            │ compactor  │  │    (S3 / R2 / B2)
  cfbenchmarks_value(5hz) │ exchange-recorder ┤                        └───────────────┘            │ → Parquet  │  │
 Coinbase / Kraken WS ──► │  (lead signals)   │                                                     └────────────┘  │
                          │                   │  parsed events (ZeroMQ PUB, IPC)                                     │
 Market registry (REST) ─►│ registry ─────────┘          │                                                         │
  GET /markets polling    │                              ▼                                                         │
                          │                    ┌──────────────────┐   ┌────────────┐   ┌──────────┐   ┌──────────┐ │
                          │                    │ feature engine   │──►│ models     │──►│ decision │──►│ executor │─┼──► Kalshi REST orders
                          │                    │ (same code used  │   │ COA1/2/4   │   │ + risk   │   │          │ │
                          │                    │  in replay)      │   └────────────┘   └──────────┘   └────┬─────┘ │
                          │                    └──────────────────┘                                        │       │
                          │  private WS: fill, market_positions ◄─────────────────────────────────────────┘       │
                          │  metrics → Prometheus/Grafana, alerts → phone                                          │
                          └────────────────────────────────────────────────────────────────────────────────────────┘
                          Host B (secondary, different provider): recorders only, used to fill Host A's gaps
```

**Design rules**

1. **Recorders never depend on strategy code.** They are separate processes, so a strategy crash can't lose data.
2. **Raw first, parse later.** Every message is stored byte-for-byte with a local receive timestamp. Parsing bugs can then be fixed and replayed later.
3. **One feature engine, two inputs.** Live trading reads the ZeroMQ stream, and backtests replay Parquet through *the same code*. This prevents differences between training and live behaviour.
4. **Gaps are data.** Every disconnect, sequence gap and stale feed is written to a `gaps` table, and backtests exclude or flag the affected windows.

---

## 3. Data: what to capture, how much, and how to capture it live

### 3.1 Capture set and volume estimates

These are estimates. **Instrument byte and message counters on day 1 and replace them with measured values.** The sizes assume compact JSON of about 250–600 bytes per message and zstd compression of about 8–12× on JSON.

| Stream | Source | What to keep | Est. msgs/s (avg, peak) | Raw/day | Compressed/day |
|---|---|---|---|---|---|
| Official index, 1 Hz | Kalshi `cfbenchmarks_value` (4 assets) | everything | 4, 4 | ~0.2 GB | ~20 MB |
| Official index, 5 Hz | Kalshi `cfbenchmarks_value_5hz` (4 assets) | everything | 20, 20 | ~0.6 GB | ~60 MB |
| 15M markets | Kalshi `ticker`, `trade`, `orderbook_delta`, lifecycle (current + next window, 4 assets) | everything | 10–40, 300 | 0.3–1 GB | 30–100 MB |
| Hourly ladders | Kalshi same channels for `KXBTCD`/`KXBTC` (+ ETH equivalents) at the next 1–3 expiries | everything | 30–150, 1,000 | 1–4 GB | 0.1–0.4 GB |
| Exchange trades | Coinbase + Kraken (BTC/ETH/SOL/XRP-USD) | everything | 20–100, 1,000 | 0.5–2.5 GB | 50–250 MB |
| Exchange books | Same 8 books | **10-level snapshots at 100 ms**, not full deltas | 80, 80 | ~4 GB | ~0.4 GB |
| Perp/funding/liquidations (optional) | A venue you may legally access (see §3.5) | trades, top of book, funding, liquidations | 20–200 | 0.5–4 GB | 50–400 MB |
| **Total (recommended)** | | | **~200–600 avg** | **~7–17 GB** | **~0.7–1.7 GB** |

- **Per month:** about **20–50 GB** compressed, which costs **under $5/month** in object storage (R2/B2/S3).
- **If you instead store full L2 delta streams for the exchanges:** add 10–50 GB/day raw (1–5 GB compressed). This isn't needed for v1, because the official index comes from Kalshi and exchange books are only lead signals. Keep a **7-day rolling buffer** of full exchange L2 for BTC if you want to explore later.
- **Throughput:** a few hundred to a few thousand messages per second is light work. One Python asyncio process per venue using `orjson` handles well over 10k msg/s per core. A 2-vCPU/4 GB VM is enough for recording plus the strategy.

### 3.2 Backfill (day 1–3; this is what makes the 6-week schedule possible)

| Dataset | Endpoint | Est. size | Notes |
|---|---|---|---|
| Settled 15M markets (labels, `expiration_value`, strike) | `GET /markets?series_ticker=…&status=settled` + `/historical/...` | ~96 windows/day × 4 assets × ~270 days ≈ **100k windows** | Gives labels and the **official settlement values** |
| 15M trade tape | `GET /markets/trades?ticker=…` (cursor) | millions of rows; a few GB | Source for COA 2 v1 features |
| 15M 1-minute candles | Batch candlesticks / historical candlesticks | ~15 bars × 100k windows | Source for bid/ask/price, volume and open-interest paths |
| Hourly ladder candles + trades | Same endpoints for `KXBTCD`, `KXBTC` | Larger (many strikes) | Used to backtest COA 4; candles only show 1-minute bid/ask, so the arbitrage backtest is approximate |
| Official index history | CF Benchmarks REST passthrough `/trade-api/v2/cfbenchmarks/history/values` | 1 Hz × 4 assets × 270 days ≈ 93M points | Costs 50 read tokens per call (about 4 calls/s on the Basic tier). Points per call depend on `timespan`/`maxResolution` **[verify]**. Requires an **account entitlement [verify first; this is the #1 dependency]** |
| Exchange trade history (optional) | Coinbase/Kraken REST or a paid vendor (e.g. Tardis.dev) | GBs | Only needed for lead-lag studies; not needed for v1 |

**Rate-limit budget:** Basic tier is about 20 reads/s and 10 writes/s; Advanced is 30/30 (apply for it now). The backfiller is a rate-limited async job with resumable cursors that writes to the same Parquet layout as the live data.

**Fallback if the index passthrough isn't entitled:** you still have the official 60-second average at **every window open and close** from `expiration_value` of settled 15-minute markets (and from the hourly ladders). That is enough to fit the 15-minute volatility model (COA 1 v0) on window-to-window returns while the live 1 Hz index accumulates.

### 3.3 How live capture works (kalshi-recorder)

**Connection and authentication**
- Endpoint: `wss://external-api-ws.kalshi.com/trade-api/ws/v2` (demo: `wss://external-api-ws.demo.kalshi.co/...`).
- Authentication happens at the WebSocket handshake with the headers `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` (ms) and `KALSHI-ACCESS-SIGNATURE`. The signature is RSA-PSS/SHA-256 over `timestamp + "GET" + "/trade-api/ws/v2"` **[verify the exact signed string]**. Use one API key for recording and a **separate key for trading**, so the two can be revoked independently.
- Use two connections: **(1)** market data (`ticker`, `trade`, `orderbook_delta`, `market_lifecycle_v2`) and **(2)** index (`cfbenchmarks_value` with `index_ids: ["BRTI","ETHUSD_RTI","SOLUSD_RTI","XRPUSD_RTI"]` and `cfbenchmarks_value_5hz`). This keeps a busy book stream from delaying index ticks.

**Market discovery: markets roll every 15 minutes**
- The **registry** polls `GET /markets?series_ticker=<S>&status=open` (and `unopened`) every 30 s for each series in `{KXBTC15M, KXETH15M, KXSOL15M, KXXRP15M, KXBTCD, KXBTC, KXETHD, KXETH}` **[verify the ETH ladder series tickers]**. It also listens to `market_lifecycle_v2` for instant notice.
- On a change, it sends `update_subscription` with `add_markets` / `delete_markets` for the relevant `sid`s. It subscribes to the *next* window's market **before** it opens so the first quotes are captured, and unsubscribes about 2 minutes after settlement.
- There is a per-subscription market limit (error code 26). The recorder **shards** markets across several subscriptions or connections once a configurable cap is reached.

**Sequence integrity**
- Each `sid` carries a `seq`. The recorder tracks the last `seq` per `sid`. On a gap it sends `update_subscription` → `get_snapshot` for those markets, logs a `gaps` row, and marks the affected book "unclean" until a snapshot arrives.
- On disconnect it reconnects with jittered exponential backoff (0.5 s → 30 s), re-subscribes and re-seeds every book from snapshots. The outage is logged as a `gaps` row `[t_disconnect, t_resubscribed]`.
- Heartbeats: answer WebSocket pings and send your own ping every 10 s. If nothing arrives on the index connection for more than 3 s, the recorder treats it as dead and reconnects. During the final 60 seconds of a window, an index gap is a **trading kill condition** (§7).

**Writing**
- Envelope per message: `{"rt": <recv_ts_ns>, "h": <host>, "c": <conn_id>, "v": "kalshi", "raw": <original bytes as string>}`.
- Envelopes are appended to `raw/kalshi/<conn>/<YYYY-MM-DD>/<HH>.ndjson.zst` using a streaming zstd writer, flushed every 1 s and rotated hourly.
- An **uploader** pushes closed hours to object storage. A **compactor** parses them into typed Parquet tables partitioned by `date/asset/table`: `index_tick`, `kalshi_ticker`, `kalshi_trade`, `kalshi_book_snapshot`, `kalshi_book_delta`, `kalshi_lifecycle`, `market_meta`, `ex_trade`, `ex_book_snap`, `gaps`.
- Local disk keeps 7 days (about 15 GB), and object storage keeps everything.

**Redundancy**
- Host B runs the same recorders on a different provider. The compactor uses Host A's data and fills Host A's `gaps` intervals from Host B, deduplicating on `trade_id` for trades and re-seeding books from B's snapshots for book streams.

**Clocks and latency**
- `chrony` with good NTP servers on both hosts, alerting at more than 50 ms offset.
- Every Kalshi message carries `sending_ts_ms`. Logging `rt − sending_ts_ms` gives a per-message network-latency series, which is useful both for monitoring and as a feature-quality flag.

### 3.4 exchange-recorder (lead signals)

- Coinbase Advanced Trade WebSocket (`level2`, `market_trades`) and Kraken v2 (`book` depth 10, `trade`) for BTC/ETH/SOL/XRP-USD.
- Books are kept in memory. The recorder writes **every trade** plus a **10-level snapshot every 100 ms**. Venue sequence numbers are checked, and the book is re-snapshotted on a gap.
- Derived live features: a composite mid (median across venues), its lead over the official index (`lead_gap = log(composite_mid) − log(index)`), order-flow imbalance (OFI) and trade imbalance.

### 3.5 Venue access warning

Binance, Bybit and OKX restrict US persons, and some also block US IPs from their APIs. Check each venue's terms before using its data, and don't route around geoblocks. US-accessible options include Coinbase, Kraken, Gemini and Bitstamp (spot), Coinbase Derivatives and CME (futures, CME data is paid), and public on-chain perp APIs (check their terms). v1 doesn't need perps, so this can wait.

### 3.6 Hosting and cost

| Item | Choice | Est. $/month |
|---|---|---|
| Host A | 2–4 vCPU / 8 GB VM in **US-East** (Kalshi's location **[verify]**; AWS `us-east-1` is a reasonable default) | 30–80 |
| Host B | Small VM, different provider | 10–25 |
| Object storage | Cloudflare R2 / Backblaze B2 / S3, about 50 GB/month growth | 1–5 |
| Monitoring | Self-hosted Prometheus + Grafana on Host A, alerts via ntfy/Telegram | 0 |
| **Total** | | **~$45–110** |

---

## 4. Shared definitions (used by all models)

- **Index ticks:** `I_s` from `cfbenchmarks_value` (1 Hz) for the asset; `cfbenchmarks_value_5hz` is used only for intra-second features.
- **Settlement average:** `S = (1/60) Σ_{j=1..60} I_{T−60+j}`. That is the 60 ticks in `(T−60s, T]`, matching the feed's `last_60s_windowed_average_15min` semantics. **Write a unit test** that recomputes it from recorded ticks and compares it against both the feed's field and `expiration_value`.
- **Strike K:** the same average for the window *open*. Take it from the market (`floor_strike` or the rules text **[verify which field holds it for KXBTC15M]**) and cross-check against the feed at the opening quarter-hour close.
- **Decision times:** the models are evaluated continuously. Training snapshots are taken at fixed seconds-to-close, `d ∈ {840, 600, 420, 300, 180, 120, 90, 60, 45, 30, 15, 5}`.

---

## 5. Models

### 5.1 COA 1: structural Bayesian fair value

**Volatility prior (offline, refit weekly)**
- 1-second log returns: `r_s = log I_s − log I_{s−1}`.
- Realised variance per 15-minute window: `RV_w = Σ r_s²` (also bipower variation, to separate jumps).
- HAR model on log realised volatility, with intraday seasonality:
  `log RV_{w+1} = β0 + β1·log RV_w + β2·log RV_1h + β3·log RV_4h + β4·log RV_1d + β5·log DVOL² + seas(minute-of-week) + η`, where `η ~ N(0, τ²)`.
- Priors: `β ~ N(0, 1)` (with `β1` centred at 0.4), `τ ~ HalfNormal(0.5)`; seasonality is a cyclic random walk with a tight prior.
- Fit in **NumPyro (NUTS)** on the backfill. With about 100k windows per asset this takes minutes.
- Innovations: Student-t with `ν ~ Gamma(2, 0.1)`, fit on standardised 1-second returns per asset. This captures fat tails.

**Online update (each tick, microseconds)**
- Predicted window variance from HAR gives an **inverse-gamma prior** on the per-second σ² with `(α0, β0)` chosen to match the HAR posterior predictive mean and variance.
- After observing `n` returns in the current window: `α = α0 + n/2`, `β = β0 + Σr²/2`. The conjugate update absorbs a volatility spike within seconds.

**Predictive distribution of S at time t (m = seconds remaining)**
- Write `S` relative to the last tick, `log(S/I_t) ≈ μ_t + Σ_{k=1..m} w_k · r_{t+k}`, with weight `w_k = (number of averaging ticks at or after step k) / 60`, so `w_k = 1` for steps more than 60 before T, then falling linearly to 1/60.
- Inside the averaging window, the already-observed ticks enter as a known constant: `S = (Σ_known I + Σ_future I) / 60`.
- `Var[log S] ≈ σ² · Σ w_k²`. The continuous-time approximation is `σ²·[(m−60) + 20]` before the window and `σ²·m³/(3·60²)` inside it.
- **Drift term, the lead signal:** `μ_t = λ·lead_gap_t + κ·mom_t`, with priors `λ ~ N(0.5, 0.3)` truncated to [0, 1.2] and `κ ~ N(0, 0.05)`. λ is fit on recorded data (it needs about 2 weeks of exchange recording, so it is 0 until fitted).
- Computation: **Monte Carlo** (e.g. 4,000 paths × 64 posterior σ draws, vectorised in NumPy/JAX, about 1–5 ms) using Student-t innovations. A closed-form normal approximation serves as a fast path and a sanity check.

**Outputs per market per tick**
- `P_struct = P(S ≥ K)`: the posterior mean, plus **P_lo / P_hi**, the 10th/90th percentiles across σ draws. This is the uncertainty about the probability itself.
- A **credible interval for S** (5%, 25%, 50%, 75%, 95%) for the dashboard and for COA 4.
- `G_struct(x) = P(S ≥ x)` for any x. COA 4 uses this on the hourly ladder strikes at aligned expiries. For a 60-minute horizon the same formula applies with larger m.

**Validation:** PIT (probability integral transform) histogram of S, a reliability diagram of P_struct by `d` bucket, log loss and Brier score vs the Kalshi mid at the same timestamp, and coverage of the 50% and 90% intervals.

### 5.2 COA 2: order-flow correction to COA 1

**Structure:** a correction on COA 1's estimate, fit per decision-time bucket `d` and asset `a`:

```
logit p = γ_d · logit(P_struct) + δ_d · (logit(mid_kalshi) − logit(P_struct)) + Σ_j β_{d,a,j} · x_j
```

- Priors: `γ_d ~ N(1, 0.2)`, `δ_d ~ N(0, 0.3)`, and `β` from a **regularised horseshoe** with hierarchical pooling across assets (`β_{d,a,j} ~ N(β_{d,j}, τ_j)`). With priors centred on "COA 1 is right and flow adds nothing," the model has to earn every deviation.
- Fit in NumPyro. A **challenger** model is LightGBM with `init_score = logit(P_struct)`, the same correction idea, calibrated with isotonic regression.
- Rows: one per (window, d). Rows from the same window are correlated, so cross-validation uses **day-blocked walk-forward** splits with a 1-day purge.

**v1 features (backfillable from trades and candles)**

| Group | Features (computed over 1, 3 and 5 minutes) |
|---|---|
| Taker flow | (yes-taker − no-taker contracts)/total; signed dollar flow |
| Participation proxies | Trade count z-scored vs a minute-of-week baseline; mean/median/p90 trade size; share of trades ≥ 100 contracts; block-trade count |
| Open interest | ΔOI per minute (candles); ΔOI/volume (opening vs closing flow) |
| Price | Kalshi mid momentum; `mid − P_struct` gap; VWAP − mid |
| Context | Previous window's result and streak length; other assets' `P_struct − 0.5` (cross-asset agreement); seconds to close |

**v2 features (from recorded order books; week 8+)**
- Depth imbalance at the top 1, 3 and 5 levels, OFI, microprice − mid, spread, cancel-to-add ratio, quote lifetime, and the size of the largest resting order.

**Second target (for execution timing):** the change in Kalshi mid over the next 30 s. A Bayesian linear model on the same features, used only to decide between waiting with a resting order and crossing the spread now.

**Keep-or-drop test (before go-live, and again at v2):** COA 1 + COA 2 must beat COA 1 alone on out-of-sample log loss **and** on simulated after-fee P&L (bootstrap P(improvement > 0) ≥ 0.8). Otherwise COA 2 stays at γ = 1, δ = 0, β = 0 (switched off).

### 5.3 COA 4: ladder and relative value

**(a) No-arbitrage scanner.** It runs on every book update for the hourly ladders. All prices are executable (ask to buy YES, bid to sell YES), and every leg pays the taker fee `f(p) = ceil(0.07·p·(1−p)·100)/100` per contract **[verify the series fee multiplier]**.

| Rule | Condition (net of fees) | Trade | Payoff |
|---|---|---|---|
| Ladder monotonicity, "above X" strikes `X_lo < X_hi`, same expiry | `bid_YES(hi) − ask_YES(lo) > f_lo + f_hi` | Buy YES(lo) at ask, buy NO(hi) at `1 − bid_YES(hi)` | Pays ≥ $1 in every outcome, at a cost below $1 |
| Bracket set sums below 1 | `Σ ask_YES(i) + Σ f_i < 1` | Buy every bracket's YES | Exactly $1 |
| Bracket set sums above 1 | `Σ bid_YES(i) − Σ f_i > 1` | Buy every bracket's NO | `n−1` dollars at a cost of `n − Σbid` |
| Bracket vs ladder | `YES[lo,hi)` mispriced vs `YES(≥lo) − YES(≥hi)` beyond fees | Replicate with the cheaper side | Locked if the strikes align exactly |

- **Preconditions to check once per series:** the brackets cover all outcomes (open-ended tails included); the strike conventions (e.g. `T65499.99` = "above 65,499.99"); and the bracket boundaries vs ladder strikes **[verify all three]**.
- **Execution:** fill the thinner leg first with IOC, then the other. If leg 2 fails, either hold (if COA 1 says the single leg has positive expectation) or unwind. Size is capped at 50% of the thinner leg's displayed depth.

**(b) Market-implied distribution.**
- *Live:* a weighted isotonic (monotone) fit of the ladder mids, with weight `1/spread²`, then PCHIP interpolation to any x. A bootstrap over bid/ask gives an uncertainty band. This runs every 1 s and takes milliseconds.
- *Offline/diagnostic (v1.1):* a Bayesian fit with `log S ~` a 2-component Student-t mixture and likelihood `mid_i ~ N(G_θ(x_i), (spread_i/2)²)`. The posterior on `G` shows how uncertain the market's distribution itself is.

**(c) Signals produced**

1. **Hard arbitrage:** from (a). This is the only riskless signal.
2. **15M vs ladder on windows that close on the hour:** `gap = P_15M_quote − G_mkt(K)`. Act when |gap| > fees + band, and when the hourly expiry uses the identical settlement average **[verify that it settles on the same 60 seconds]**.
3. **COA 1 vs ladder:** for each hourly strike, compare `G_struct(x)` from COA 1 (with its P_lo/P_hi band) to that strike's quote. This **extends COA 1 to the hourly markets** and is likely the largest source of trades.
4. **Cross-asset staleness (v1.1):** residual `e_a = Φ⁻¹(P_mkt,a) − Φ⁻¹(P_struct,a)` per asset. After a common shock, an asset with a large residual while the others are near 0 has a stale quote. This feeds the decision layer as an edge boost/penalty, not as standalone trades.

The **cross-asset correlation** (a rolling ρ of 15-minute averaged returns, Student-t copula) is also used by the **risk layer**. Long-YES positions in BTC, ETH and SOL are mostly *one* bet.

---

## 6. Decision layer and sizing

For each market, side ∈ {YES, NO} and candidate price `c` (ask for taker, or our limit price for maker):

```
p_lo   = 10th percentile of the final P(YES)   (for buying YES; use 1 − 90th percentile for NO)
edge   = p_lo − c − fee(c)
trade if edge > θ_d          (θ_d ≥ 1¢, tuned in backtest; larger at long d where model error is larger)
```

- **Final P:** COA 1 → COA 2 correction → ± COA 4 cross-asset adjustment. For COA 4 arbitrage signals (1, 2), the arbitrage payoff replaces the edge calculation.
- **Maker vs taker:** if more than 120 s remain and COA 2's 30-second signal doesn't predict an adverse move, post a limit order at `p_mid − margin` (post-only **[verify the flag]**) and cancel/replace when fair value moves by more than 1¢. Otherwise cross the spread if the edge clears θ.
- **Size:** fractional Kelly on a binary contract, `f* = (p_lo − c)/(1 − c)` for YES, with `k = 0.1` at launch (0.25 maximum). Contracts = `min(k·f*·bankroll/c, 30% of displayed size at that price, per-market cap)`.
- **Position book:** one net position per market, shared across COAs. Each fill is tagged with the signal that caused it, for P&L attribution.

---

## 7. Risk controls and kill switches

| Control | Launch setting |
|---|---|
| Per-window max cost | $25 (scale ×2 every 2 weeks if live results stay within the backtest's 80% band) |
| Per-asset open exposure | $100 |
| Correlated exposure | Sum over assets of `exposure_a · ρ_a,BTC` ≤ $150 ("BTC-equivalent") |
| Daily loss limit | 3% of bankroll: flatten and halt until manual restart |
| Weekly loss limit | 8%: halt and review |
| Index feed stale | > 3 s anywhere, or any gap during a window's final 60 s → no new orders for that asset, cancel resting orders |
| Book unclean | Sequence gap not yet re-snapshotted → no orders in that market |
| Sanity | `|P_final − mid| > 0.40`, or σ posterior outside [0.2×, 5×] the HAR prediction → no orders, alert (probably a bug) |
| Settlement check | Our computed S ≠ `expiration_value` by > $0.01 → alert; 2 in a day → halt |
| Clock | NTP offset > 100 ms → halt |
| API errors | > 5 order rejects/min → halt |

All kill switches are **tested in shadow mode** by injecting faults (dropping the index feed, corrupting `seq`) before go-live.

---

## 8. Execution

- Kalshi REST order endpoints with a limit `price`, `count`, `side`, `action`, `client_order_id` (for idempotent retries) and time-in-force (IOC/GTC) **[verify field names and post-only support]**. Batch create/cancel endpoints are used for ladders. In batch cancels, each cancel counts as 0.2 against write limits.
- Private WebSocket channels: `fill` and `market_positions` keep the position book in sync. The executor reconciles with `GET /portfolio/positions` every 60 s and halts on mismatch.
- **Write budget:** 10/s on Basic is enough for v1 (a few orders per market per window). Advanced (30/s) is needed once cancel/replace maker quoting runs across ladders.

---

## 9. Backtesting, shadow trading and go/no-go

**Backtester (event-driven, replays Parquet through the live feature engine)**
- Taker fills at the recorded ask up to the displayed size, with injected latency (base 250 ms, plus a 500 ms stress test).
- Maker fills only when a recorded **trade prints at or through** our price after our estimated queue position. Before order-book recording exists, the backtest assumes taker-only.
- Fees per the formula, and settlement from `result`.
- Reported metrics: P&L with block-bootstrap confidence intervals (blocks of 1 day), hit rate, average edge at entry vs realised, maximum drawdown, and turnover. Everything is split by signal (COA 1, 1+2, 4-arb, 4-ladder) and by `d` bucket.

**Shadow mode (weeks 3–5):** the full live system runs against **production** data and logs intended orders with the fills the fill model *would* have given. The demo environment's liquidity isn't realistic, so it is used only to test order plumbing.

**Go/no-go for live (end of week 5 / week 6)**

| Gate | Pass criterion |
|---|---|
| Recorder | ≥ 99.5% uptime over the last 14 days; every gap logged; settlement check 100% |
| COA 1 calibration | Reliability slope within [0.9, 1.1] per `d` bucket; 90% interval coverage within [87%, 93%] |
| Backtest | Traded-subset after-fee P&L > 0 with bootstrap P(P&L > 0) ≥ 0.9 over ≥ 60 days of backfill |
| Shadow vs backtest | Shadow P&L per trade inside the backtest's 80% band; simulated fill rate within ±25% |
| Risk | Every kill switch exercised in a fault-injection drill |
| Ops | Alerting reaches your phone; one-command flatten-all script tested on demo |

**If a gate fails:** launch only the signals that pass (e.g. COA 4 hard arbitrage only, or COA 1 at `d ≤ 60 s` only) instead of delaying everything.

---

## 10. Six-week schedule (to small-size live trading)

| Week | Deliverables | Exit check |
|---|---|---|
| **1: Data** | API keys (separate recording and trading keys); confirm the CF passthrough entitlement; **recorder live by day 2** (Kalshi market data + index + Coinbase/Kraken); registry; raw writer + uploader; backfill jobs started (settled markets, trades, candles, index history); compactor → Parquet; Prometheus metrics + alerts | 48 h of clean recording; backfill at least 50% complete; settlement unit test passes |
| **2: COA 1** | Shared definitions (§4); realised-volatility and HAR features; NumPyro HAR fit; online inverse-gamma update; Monte Carlo predictive; replay backtester (taker fill model); calibration report | Calibration gates met on backfill |
| **3: Shadow + COA 4a** | COA 1 running live in shadow; decision layer + sizing; no-arbitrage scanner live (logging only); isotonic ladder fit; risk module skeleton | Shadow running 24/7; scanner logs violations with fee-adjusted P&L |
| **4: COA 4b + COA 2 v1** | 15M-vs-ladder and COA 1-vs-ladder signals; COA 2 v1 features from backfilled trades/candles; NumPyro correction model + LightGBM challenger; keep-or-drop test; lead-gap λ fit (2 weeks of exchange data now exist) | Keep-or-drop decision recorded; COA 4 signals in shadow |
| **5: Hardening** | Executor (orders, private WebSocket, reconciliation); all kill switches + fault drills; flatten-all script; end-to-end demo-environment test of order plumbing; Grafana P&L/attribution dashboard | All §9 gates evaluated |
| **6: Go live (small)** | Go/no-go review; live at launch limits (§7); daily review of live vs shadow vs backtest | First week live within risk limits |
| 7–10 (after launch) | COA 2 v2 (order-book features, about 4 weeks of book history by then); Bayesian implied distribution; cross-asset signal; add DOGE/HYPE/BNB; maker quoting on ladders; apply for Advanced API tier | Each addition must pass the keep-or-drop test |

**If you fall behind, cut in this order:** cross-asset signal → Bayesian ladder fit (keep isotonic) → COA 2 v1 (launch with COA 1 + COA 4 only) → hourly ladders except BTC. **Never cut:** the recorder, the settlement unit test, kill switches or shadow mode.

This schedule assumes **one full-time developer**. With two, split recorder/execution/ops (person A) from models/backtesting (person B), which buys about a week of slack.

---

## 11. Repository layout and stack

```
kalshi_trading/
  common/        config, time utilities, Kalshi auth (RSA-PSS signing), event schemas
  collectors/    kalshi_ws.py, registry.py, exchange_coinbase.py, exchange_kraken.py, backfill/
  storage/       raw_writer.py, uploader.py, compactor.py, parquet_schemas.py
  features/      index_features.py (RV, HAR inputs, lead_gap), kalshi_flow.py, ladder.py, books.py
  models/        coa1_structural.py, coa2_correction.py, coa4_ladder.py, calibration.py
  strategy/      decision.py, sizing.py, risk.py, position_book.py
  execution/     kalshi_orders.py, reconciler.py
  backtest/      replay.py, fill_model.py, reports.py
  monitoring/    metrics.py, alerts.py
configs/         series.yaml, risk.yaml, model.yaml
scripts/         flatten_all.py, run_recorder.py, run_strategy.py
tests/           test_settlement_avg.py, test_noarb_payoffs.py, test_seq_gaps.py, ...
docs/
```

**Stack:**
- Python 3.12; `asyncio` + `websockets`; `orjson`; `zstandard`; `pyarrow` + `polars`; DuckDB for ad-hoc queries.
- NumPyro/JAX for the Bayesian models; NumPy/SciPy; LightGBM (challenger).
- `cryptography` (RSA-PSS); `pyzmq` (live event bus); `prometheus-client`.
- systemd or Docker Compose on the VMs.

---

## 12. Open items to verify before writing code (week 1, day 1)

1. Whether the CF Benchmarks REST passthrough **entitlement** is on your account, and how many points `history/values` returns per call.
2. Which market field holds the 15M "price to beat" (`floor_strike`?), and whether it equals the feed's opening average to the cent.
3. Hourly `KXBTCD`/`KXBTC` expiry alignment and settlement window (the identical 60 s in `(T−60, T]`?); bracket coverage; strike boundary conventions; ETH ladder series tickers.
4. The fee multiplier for `KXBTC15M` and the ladder series; whether maker fees apply.
5. WebSocket per-subscription market limit; handshake signing string; ping/idle timeout.
6. Order API: post-only flag, time-in-force values, batch endpoints, order groups.
7. Your API tier and limits; apply for Advanced.
8. Terms of use for any exchange data feed you record (especially redistribution and US access).

---

## Sources

- CF Benchmarks value feed: https://docs.kalshi.com/websockets/cfbenchmarks-value · 5 Hz: https://docs.kalshi.com/websockets/cfbenchmarks-value-5hz
- CF Benchmarks REST passthrough: https://docs.kalshi.com/cfbenchmarks/rest-passthrough
- WebSocket connection: https://docs.kalshi.com/websockets/websocket-connection.md · Quick start: https://docs.kalshi.com/getting_started/quick_start_websockets
- Order-book updates: https://docs.kalshi.com/websockets/orderbook-updates.md · Market ticker: https://docs.kalshi.com/websockets/market-ticker.md
- Market fields: https://docs.kalshi.com/api-reference/market/get-market · API index: https://docs.kalshi.com/llms.txt
- Rate limits: https://docs.kalshi.com/getting_started/rate_limits
- Contract rules (KXBTC15M mirror): https://exchange.pmxt.dev/markets/kxbtc15m/bitcoin-price-abovebelow/kxbtc15m-26sep021900-00
- Fees: https://defirate.com/prediction-markets/fees/
