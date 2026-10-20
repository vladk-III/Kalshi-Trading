# Kalshi 15-Minute Crypto Markets: Data, Arbitrage, and Five Strategy Options (COAs)

*Research date: 2026-10-20. Kalshi changes contract terms, fees and API fields over time. Re-check every item marked **[verify]** against docs.kalshi.com and the series' own rules before you trade real money.*

---

## Part 1: What you are trading

### 1.1 Contract mechanics (KXBTC15M and siblings)

| Item | Detail |
|---|---|
| Series | `KXBTC15M` (BTC). Sibling series include `KXETH15M`, `KXSOL15M` and `KXXRP15M`. Third-party listings also mention DOGE, BNB and HYPE **[verify the current list with `GET /series`]** |
| Question | "BTC price up in next 15 mins?" There is one binary market per 15-minute window. Ticker format: `KXBTC15M-26SEP021900-00` |
| Strike ("price to beat") | The simple average of the 60 one-second CF Benchmarks **BRTI** prints taken just before the window **opens** |
| Settlement value | The simple average of the 60 BRTI prints taken just before the window **closes**, rounded to 2 decimal places |
| Resolves YES if | settlement average **≥** opening average (ties go to YES) |
| Payout | $1 per contract. Prices are quoted in $0.01 increments, and sub-penny pricing exists on some series **[verify]** |
| Fees | Taker fee ≈ `ceil(0.07 × C × P × (1−P))`, which is at most about 1.75¢ per contract at P = 0.50. Where maker fees apply they are about ¼ of that. Settlement is free. Each series carries a fee multiplier **[verify for KXBTC15M]** |

**Why this matters for modelling**

1. **You are predicting a 60-second average, not a spot price.** An average of a random walk has lower variance than the endpoint. For Brownian motion averaged over a window of length *w* ending at T, Var ≈ σ²(T − w) + σ²·w/3. Both the strike and the settlement value are averages.
2. **The final 60 seconds are partly observed.** Inside the averaging window, part of the settlement value is already locked in. With 30 seconds left, half the average is known and the remaining uncertainty is about σ²·(30s)/3·(¼). This is the most mechanical, model-friendly edge in the product.
3. **The strike is known once the window opens.** The question reduces to P(A_close ≥ K), where K is a known number and A_close is a 60-second average of a process you can watch live.
4. **The BRTI is not "the BTC price."** It is a CF Benchmarks index built from an order-book-weighted median over several constituent USD spot exchanges. **Kalshi streams the official index values itself.** The `cfbenchmarks_value` WebSocket channel (about 1 Hz, 21 coins including BRTI, ETHUSD_RTI, SOLUSD_RTI, XRPUSD_RTI) includes a trailing 60-second average and, in the final minute of each quarter hour, the **running settlement average** (`last_60s_windowed_average_15min`). The `cfbenchmarks_value_5hz` channel covers BTC/ETH/SOL/XRP/DOGE at 5 Hz. History comes through the **CF Benchmarks REST passthrough** (`/trade-api/v2/cfbenchmarks/...`, which requires an account entitlement **[verify that your account has it]**). Exchange feeds are still useful as *leading* signals, since the index is computed from them, but you do not need to rebuild the settlement index yourself.

### 1.2 Related Kalshi contracts (needed for the credible-interval and arbitrage ideas)

| Series | Structure | Relevance |
|---|---|---|
| `KXBTCD` (hourly/daily "above/below") | A ladder of strikes, each "BTC ≥ X at time T". Uses the same 60-second BRTI average before expiry | Gives the market's implied **CDF** of the settlement price at each hourly expiry |
| `KXBTC` (range brackets) | Mutually exclusive price ranges `[floor_strike, cap_strike)` at a fixed expiry | Gives the market's implied **PMF**. Bracket prices should sum to ≈ $1, and P(bracket) should ≈ P(>lo) − P(>hi) from the KXBTCD ladder |
| ETH/SOL/XRP… 15M | The same structure on correlated assets | Cross-asset relative value. The joint distribution is highly correlated |
| Previous and next 15M windows | Back-to-back windows | Momentum and mean-reversion features. Next window's strike = the current window's settlement value |

> **Key alignment:** a 15M window that closes on the hour (e.g. 18:45 to 19:00) settles on **the same 60-second BRTI average** as the hourly `KXBTCD`/`KXBTC` contracts expiring at 19:00 **[verify that the hourly expiry falls on that hour]**. In those windows the 15M "up" contract is economically a `KXBTCD` contract struck at K = the window's opening average. Interpolating the hourly ladder at K gives a second, independent market price for the same payoff.

---

## Part 2: What information is available (the feature inventory)

### 2.1 What Kalshi does **not** give you

Kalshi publishes **no number of traders, no unique-account counts, no per-user positions and no per-user bet sizes**. Trades are anonymous, and only the taker side is shown. You have to build proxies from trade prints and order-book activity, as in the table below.

| You asked for | Closest available proxy |
|---|---|
| Number of traders | Number of trades per interval, distinct trade-size "fingerprints", and the order add/cancel rate from `orderbook_delta` |
| Average bet size | Mean and median `count` per trade, the trade-size distribution (e.g. share of trades ≥ 100 contracts), and `is_block_trade` |
| Money flow / conviction | `dollar_volume`, `dollar_open_interest`, and taker-side imbalance (yes-taker vs no-taker contracts) |

### 2.2 Kalshi data sources

| Source | Fields | History available? |
|---|---|---|
| `GET /markets`, `GET /markets/{ticker}` | `yes_bid/ask_dollars`, `yes_bid/ask_size_fp`, `no_bid/ask_dollars`, `last_price_dollars`, `volume_fp`, `volume_24h_fp`, `open_interest_fp`, `notional_value_dollars`, `previous_*` (a day ago), `floor_strike`, `cap_strike`, `strike_type`, `functional_strike`, `custom_strike`, `expiration_value`, `result`, `settlement_value_dollars`, `settlement_ts`, `open/close/expiration_time`, `rules_primary`, `price_ranges`, `is_provisional` | Yes. Settled markets with `result` and `expiration_value` are your **labels** |
| `GET /markets/trades` (+ historical) | `trade_id`, `ticker`, `count`, `yes_price`/`no_price`, `taker_side`, `is_block_trade`, `created_time` | Yes, with a cursor. This gives you full trade tape history |
| `GET .../candlesticks` (1-minute, 1-hour, 1-day; batch endpoint; `/historical/...` for older markets) | OHLC of bid/ask/price, volume, open interest per bar | Yes, at 1-minute resolution |
| `GET /markets/{ticker}/orderbook` | Yes bids and no bids by level. A no bid at X equals a yes ask at 1−X | **Snapshot only. Kalshi keeps no order-book history, so you must record it yourself** |
| WebSocket `ticker` | `price_dollars`, `yes_bid/ask_dollars` + sizes, `volume_fp`, `open_interest_fp`, `dollar_volume`, `dollar_open_interest`, `last_trade_size_fp`, `ts_ms` | Live only |
| WebSocket `trade` | Every public print with taker side and size | Live only (the REST endpoint backfills this) |
| WebSocket `orderbook_delta` | Level-by-level book changes (adds and cancels) | Live only. Needs an authenticated connection |
| WebSocket `market_lifecycle_v2` | Market open, close, determination and settlement events | Live only |
| RFQ endpoints | Requests for quote, for larger size | Live |
| WebSocket `cfbenchmarks_value` / `cfbenchmarks_value_5hz` | **Official CF Benchmarks RTI values** (1 Hz for 21 coins, 5 Hz for BTC/ETH/SOL/XRP/DOGE), trailing 60-second average, and the running quarter-hour settlement average in the final minute | Live. History via the REST passthrough (entitlement required) |

**Kalshi-derived features** (computed per market, at several look-backs such as 10s, 30s, 1m and 5m):

- *Price:* mid, microprice `(bid·ask_sz + ask·bid_sz)/(bid_sz+ask_sz)`, spread, logit(mid), Δmid, and mid minus model fair value
- *Book:* depth at the top 1, 3 and 5 levels on each side, depth imbalance, order-flow imbalance (OFI), cancel-to-add ratio, quote lifetime, and price-level gaps
- *Flow:* trade count, volume, taker-yes − taker-no imbalance, mean/median/p90 trade size, block-trade flag, time since the last trade, VWAP vs mid
- *Participation:* ΔOI vs volume (opening vs closing flow), `dollar_open_interest`, and volume relative to the same time-of-day baseline
- *Lifecycle:* seconds to close, seconds into the averaging window, and the window index of the day
- *Cross-market (Kalshi):* the implied P from the hourly ladder at the same K, other assets' 15M mids, the previous window's result and closing price, and the realised "up" streak

### 2.3 Underlying-asset data (most of the predictive power for the *ending price*)

| Source | Features |
|---|---|
| Kalshi `cfbenchmarks_value(_5hz)` | **The official settlement index**: the running 60-second average, distance to strike in σ units, and 1-second/5 Hz returns for realised volatility |
| BRTI constituent spot exchanges (Coinbase, Kraken, Bitstamp, Gemini, …) via public WebSockets | *Leading* signals for the index (the index is computed from these books, so their moves arrive slightly earlier), spot order-book imbalance, trade flow, and a fallback index proxy if the Kalshi feed drops |
| Perpetual futures (Binance, Bybit, OKX, Coinbase International) | Perps lead spot by a few hundred ms to seconds: basis, funding rate, OI changes, **liquidation prints**, perp OFI |
| CF Benchmarks via the Kalshi REST passthrough | Official RTI history for backtesting COA 1 (`/cfbenchmarks/history/values`; each call costs 50 read tokens) |
| Options (Deribit, CME) | DVOL / ATM implied vol, which serves as a **prior for σ**, plus the short-dated skew |
| Realised volatility | Realised variance from 1-second returns, Parkinson/Garman-Klass estimators, HAR-RV (1-minute/15-minute/1-hour/1-day components), bipower variation (a jump-robust estimator), and jump flags |
| Calendar | Hour-of-day/day-of-week (volatility seasonality), US equity open/close, macro release times (CPI, FOMC, NFP), weekends, CME gap times |
| Cross-asset | ETH/SOL vs BTC beta and residual moves, NQ/ES futures, DXY (mostly matters in US hours) |

### 2.4 Cross-venue prediction markets

| Venue | Product | Basis vs Kalshi |
|---|---|---|
| Polymarket | 15-minute (and 5-minute) crypto Up/Down | Settles on **Chainlink Data Streams** at the close timestamp (a point price, not a 60-second BRTI average), and its strike is defined differently. That is real **basis risk**: the two contracts can resolve differently in near-tie windows. US access and regulatory status also need checking |
| Others (Coinbase/Robinhood prediction markets, etc.) | Some list Kalshi-sourced contracts **[verify]** | Often the same Kalshi book, which makes them useless for arbitrage |

---

## Part 3: Arbitrage and relative-value map

Ranked from "closest to riskless" to "statistical":

1. **Unified book (no free lunch):** Kalshi's yes and no sides are one book (`yes_ask = 1 − no_bid`), so the naive "yes + no < $1" arbitrage **cannot occur** inside one market.
2. **Ladder no-arbitrage (KXBTCD / KXBTC):** implied P(≥X) must fall as X rises; bracket prices must sum to about $1; and P(bracket[lo,hi)) ≈ P(≥lo) − P(≥hi). A violation larger than **all-in fees on every leg** is a near-arbitrage. Legging risk and thin depth are the practical problems.
3. **15M vs hourly ladder on aligned windows:** for windows closing on the hour, 15M-up ≡ `KXBTCD` at strike K. Interpolate the ladder (monotone spline on the CDF) and compare. Because no listed strike equals K exactly, the hedge is approximate: a bracket or adjacent-strike combination, with some residual risk.
4. **Cross-asset relative value:** BTC/ETH/SOL 15M windows share open and close times. With ρ ≈ 0.8–0.9 between 15-minute returns, the joint implied probabilities must be consistent with a bivariate distribution. Large divergences after a common shock (e.g. BTC repriced but SOL has not) are tradable. This is a statistical edge, not an arbitrage.
5. **Cross-venue (Kalshi vs Polymarket):** price differences on "the same" 15-minute question. The settlement mechanism differs (Chainlink point price vs BRTI 60-second average) and so does the strike, so this is **basis trading, not arbitrage**. Capital also sits on two venues, and moving it takes days.
6. **Latency / stale quotes:** Kalshi resting quotes lag sharp moves in perps and spot. Picking them off is a speed race against professional market makers, and the taker fee near 50¢ is about 1.75¢.

**Hurdle rule:** an edge is only real if `|model_P − execution_price| > taker_fee(P) + expected slippage + model-error buffer`. At P = 0.50, that means more than about 2–3¢ of edge per contract for a taker strategy.

---

## Part 4: Five candidate COAs (data collection → processing → model → prediction)

Every COA shares a **common base**: a recorder that logs Kalshi WebSocket data (`ticker`, `trade`, `orderbook_delta`, `market_lifecycle_v2`) for all 15M and hourly crypto series, plus 1-second spot data from the constituent exchanges. Kalshi has **no order-book history**, so start recording on day 1 whichever COA you pick. Labels come from settled markets (`result`, `expiration_value`).

---

### COA 1: Structural Bayesian fair value ("model the 60-second average directly")

| Stage | Design |
|---|---|
| **Collect** | The official index from Kalshi `cfbenchmarks_value(_5hz)` (history from the REST passthrough), constituent-exchange books as leading signals, Deribit DVOL, Kalshi 15M `ticker` plus settled labels |
| **Process** | Use the official index directly and check it against `expiration_value` on settled markets. Compute the running 60-second average, the observed fraction of the averaging window, and realised-volatility features (HAR-RV, bipower variation, jump flags) |
| **Model** | A Bayesian stochastic-volatility model of 1-second log returns with Student-t innovations (in PyMC, NumPyro or Stan), and a HAR prior on σ informed by DVOL. Fit offline, then update the σ posterior online (particle filter, or a conjugate inverse-gamma update on recent realised variance). Use an analytic or Monte Carlo posterior predictive for **A_close = known part + simulated remaining path** |
| **Predict** | Full posterior of A_close, giving a **credible interval** for the settlement price, and P(YES) = P(A_close ≥ K) with its own credible interval. Trade only when the *entire* credible interval of P sits beyond `market price ± fees` |

- **Strengths:** It needs little Kalshi history, since the physics of an averaged random walk does most of the work. It is interpretable and transfers across assets (BTC/ETH/SOL share the code). It handles the final-60-second partial-observation edge exactly. It gives honest uncertainty that you can use for Kelly sizing.
- **Weaknesses:** Market makers almost certainly run a similar model, so the edge comes mostly from better σ estimation, a better BRTI proxy and speed. Fat tails and jumps (liquidation cascades) break Gaussian assumptions. It ignores Kalshi order flow.
- **Assumptions:** Drift ≈ 0 over 15 minutes. Kalshi's index feed is reliable and timely, with the exchange-built proxy only as a fallback. Volatility is forecastable over a 1–15 minute horizon. Execution is fast enough to act before the market reprices.

---

### COA 2: Microstructure and order-flow direction model ("who is trading, and how")

| Stage | Design |
|---|---|
| **Collect** | Full Kalshi WebSocket: `orderbook_delta`, `trade`, `ticker` for 15M across all assets. Perp order flow and liquidations (Binance/Bybit), plus spot OFI. Requires **4–8+ weeks of recording** before training |
| **Process** | Rebuild the L2 book at every event. Compute features at 10s/30s/1m/5m (§2.2): OFI, depth imbalance, microprice, taker imbalance, trade-size distribution as a "bet size" proxy, trade count as a "trader activity" proxy, block trades, ΔOI/volume, cancel rate. Normalise by time-of-day baselines. Sample snapshots at fixed seconds-to-close (e.g. 600s, 300s, 120s, 60s, 30s) |
| **Model** | (a) A hierarchical Bayesian logistic regression with horseshoe priors (for sparse features) and partial pooling across assets and seconds-to-close buckets, for YES/NO. (b) A gradient-boosted challenger (LightGBM) with isotonic or Platt calibration. Validate with purged walk-forward splits by day |
| **Predict** | Calibrated P(YES) and a P(next-30s mid move) signal, used both for direction bets and for **timing entries** (e.g. waiting for a better price instead of crossing the spread) |

- **Strengths:** It uses information that pure price models ignore: informed Kalshi flow, large-size takers and book pressure. Hierarchical pooling helps low-volume assets borrow strength from BTC. The timing signal reduces execution cost even if direction alpha is weak.
- **Weaknesses:** It needs your own recorded data, with a long cold start. Microstructure features drift with participant mix and fee changes. There is a high risk of overfitting, since about 96 windows a day per asset is a small sample. Without real-time access to a feature, the model degrades.
- **Assumptions:** Kalshi order flow contains information not already in spot. Patterns persist across weeks. Proxies (trade count, size distribution) carry the signal you wanted from "number of traders" and "average bet size".

---

### COA 3: Market-as-prior Bayesian ensemble ("trust the market, then nudge it")

| Stage | Design |
|---|---|
| **Collect** | The Kalshi 15M mid/microprice, the COA 1 fair value, the COA 2 flow features, and the hourly ladder-implied P. Keep a historical record of all of them alongside settled outcomes |
| **Process** | Convert every source to logit probability. Measure each source's historical calibration and error correlation per seconds-to-close bucket |
| **Model** | A Bayesian **logit-pooling / stacking** model: `logit P = w₀·logit(market) + w₁·logit(structural) + w₂·flow + …`, with priors centred on w₀ = 1 and the rest at 0 (the market is efficient unless the data says otherwise). Weights vary with time-to-close and volatility regime (a dynamic linear model or Bayesian state-space model). Alternatively, a Beta prior from the market price updated with a model likelihood |
| **Predict** | A posterior P(YES) that only departs from the market when evidence is strong. The posterior on the weights tells you *which* source is adding value and when |

- **Strengths:** It is the most robust option, because the default behaviour is "don't trade," which guards against overconfidence. It produces small, well-calibrated deviations and folds in COA 1, COA 2 and COA 4 outputs. It is a built-in diagnostic for whether you have any edge.
- **Weaknesses:** Edges are smaller, so you need execution discipline and low fees (maker orders). It depends on the other models existing. Weights adapt slowly when the regime breaks.
- **Assumptions:** The market price is a strong but imperfect forecaster. Its errors are partly predictable by your features. Calibration is stable enough to estimate the weights.

---

### COA 4: Distribution-ladder relative value and cross-asset consistency ("credible intervals vs the market's implied distribution")

| Stage | Design |
|---|---|
| **Collect** | Snapshots of the full `KXBTCD` ladder and `KXBTC` brackets (all strikes, bid/ask/size), every 15M market across BTC/ETH/SOL/XRP, and spot for all assets. Collect at least every second near expiries |
| **Process** | Run no-arbitrage checks (monotonic CDF, brackets summing to 1, bracket = CDF difference). Fit a smooth implied distribution at each expiry using bid/ask as interval constraints (a monotone spline or mixture-of-lognormals CDF). Align 15M windows that close on the hour with the hourly expiry. Estimate cross-asset return correlations from 1-second spot data |
| **Model** | (a) A **Bayesian implied-distribution model**: fit the ladder's CDF with a Bayesian spline or Dirichlet process, so that its credible band shows how uncertain the market's distribution itself is. (b) A **joint model across assets**: a bivariate Student-t or copula over 15-minute averaged returns, so each asset's 15M price implies a range for the others. (c) Your own physical posterior from COA 1 to compare with the market's distribution |
| **Predict** | Mispricing scores: (i) hard violations of no-arbitrage rules, (ii) 15M vs interpolated-ladder gaps on aligned windows, (iii) cross-asset inconsistency, (iv) brackets where your credible-interval mass differs from bracket prices. Output multi-leg orders with a legging plan |

- **Strengths:** It is the closest to real arbitrage, and some trades have bounded or near-zero directional risk. It directly uses your "credible intervals for ranges" idea against bracket prices. Cross-asset consistency is a less crowded edge than BTC direction.
- **Weaknesses:** Hourly and bracket books are thin, so size is limited and fees are paid on every leg. Legging risk exists because there is no atomic multi-leg order **[verify combo/RFQ support]**. Interpolating to K leaves residual risk. Violations may be fleeting and contested by bots.
- **Assumptions:** The ladder and 15M settle on the identical BRTI average at aligned times. Liquidity is sufficient to execute both legs. Correlation estimates are stable within a window.

---

### COA 5: Cross-venue and lead-lag latency model ("Kalshi is the slow price")

| Stage | Design |
|---|---|
| **Collect** | Millisecond feeds from Binance/Bybit perps (trades, books, liquidations), constituent spot exchanges, Kalshi WebSocket (`ticker`, `orderbook_delta`), Polymarket 15M CLOB, and the Chainlink stream price used by Polymarket. Co-locate the bot near Kalshi's infrastructure (US East) and record latency (`ts_ms` vs `sending_ts_ms` vs local receive time) |
| **Process** | Synchronise clocks (NTP/PTP). Estimate lead-lag (perp → spot → BRTI proxy → Kalshi quotes) with cross-correlation and Hayashi-Yoshida estimators. Compute the Kalshi-implied vs perp-implied P(YES) gap in real time, and the Kalshi-vs-Polymarket gap net of the settlement basis (BRTI average vs Chainlink point price, and the strike difference) |
| **Model** | A **Bayesian dynamic linear model / Kalman filter** in which the latent "true" P(YES) is driven by perp and spot moves, and Kalshi and Polymarket quotes are noisy, lagged observations. A second model is a Bayesian regression of the Kalshi–Polymarket resolution-mismatch rate on how close the window is to its strike |
| **Predict** | Real-time P(YES), the expected Kalshi quote in Δt seconds, and stale-quote alerts. Act when a resting Kalshi quote is stale by more than fees plus adverse-selection cost; on cross-venue gaps, size by the basis-risk posterior |

- **Strengths:** It targets the most frequent edge source, short-lived mispricings after fast moves. It works for every asset. The state-space model doubles as an execution engine for COAs 1–4.
- **Weaknesses:** It is an infrastructure-heavy arms race against professional market makers. Edges decay as competition rises. Polymarket brings regulatory/access questions, a different oracle, a different strike and capital split across venues. Adverse selection on your own resting quotes is severe.
- **Assumptions:** Your end-to-end latency is competitive. Lead-lag relationships are stable. Kalshi API rate limits allow the needed order rate **[verify your tier's limits]**. Cross-venue settlement basis is small and measurable.

---

## Part 5: Side-by-side comparison

| | COA 1 Structural | COA 2 Microstructure | COA 3 Market-prior ensemble | COA 4 Ladder / cross-asset RV | COA 5 Latency / cross-venue |
|---|---|---|---|---|---|
| Primary output | Credible interval for the price, P(YES) | P(YES), short-term price move | Calibrated P(YES) | Mispricing / arbitrage scores | Real-time fair P, stale quotes |
| Data cold start | Low (days) | High (weeks to months) | Medium (needs the other COAs) | Medium | Medium |
| Infrastructure difficulty | Medium | Medium–High | Low (on top of the others) | Medium | **High** |
| Bayesian fit | Excellent | Good (hierarchical) | Excellent | Excellent | Good (state-space) |
| Edge crowding | High | Medium | n/a (meta-model) | **Low–Medium** | Very high |
| Directional risk | Full | Full | Full but sized down | Low–Medium | Low per trade, high adverse selection |
| Main failure mode | Tails, proxy error | Overfitting, drift | Too few trades | Thin liquidity, legging | Losing the speed race |

### Suggested sequencing (my recommendation)

1. **Week 0:** build the shared recorder (Kalshi WebSocket + spot + ladder snapshots). It feeds every COA, and the order-book history can't be bought later.
2. **Weeks 1–3:** build **COA 1**. It gives you the credible-interval engine and a fair-value baseline. Backtest it against settled markets using Brier score, log loss and a calibration curve, and estimate P&L only after fees.
3. **Weeks 3–6:** build **COA 4** on top of COA 1's distribution. It is the least crowded edge, and it uses the range-bracket/credible-interval framing directly.
4. **Weeks 6+:** use **COA 3** to combine sources and, once enough book data has accumulated, add **COA 2** features. Treat **COA 5** as an execution layer, not a standalone strategy, unless you are prepared to compete on latency.

> **Superseded:** the selected path is COAs 1 + 2 + 4 on a ~6-week schedule. See Part 6 and the [full design](../design/02-system-design-coa-1-2-4.md).

### Evaluation rules that apply to every COA

- Validate walk-forward by day, with no shuffling and a gap (purging) between train and test.
- Score probabilities with log loss/Brier and reliability diagrams, and size with fractional Kelly using the *lower* credible bound of the edge.
- Simulate fills realistically: taker at the ask, fee formula, partial fills and queue position for maker orders.
- Paper-trade on the Kalshi **demo** environment (`demo-api.kalshi.co`) before trading live.

---

## Part 6: Selected path, COAs 1 + 2 + 4 as one system

The three COAs are layers of one system, not three separate projects:

```
Shared recorder (Kalshi WebSocket + exchange spot/futures + hourly ladder snapshots)
        │
        ├── COA 1: structural model → price interval for the closing average, P_struct(YES)
        │        │
        │        ├── COA 4: compares COA 1's distribution with the hourly/range ladder
        │        │          and with the other assets → relative-value trades
        │        │
        │        └── COA 2: order-flow model fit on what COA 1 gets wrong
        │                   (COA 1's P is a fixed input; flow features only adjust it)
        ▼
   Final P(YES), edge after fees, position sizing
```

- **COA 1 is the base layer.** COA 4 needs a price distribution to compare against the ladder, and COA 1 produces it. COA 2 works best as a *correction* on COA 1, not as a standalone direction model. With only about 96 windows a day per asset, learning COA 1's mistakes needs far less data than learning direction from scratch.
- **COA 2's slow part is the order-book history,** which you have to record yourself. To fit the schedule, COA 2 ships in two versions. **v1** uses only features rebuilt from Kalshi's *historical* trade tape and 1-minute candles, which can be backfilled on day 1. **v2** adds order-book features once about 4 weeks of recorded book data exist. v2 arrives *after* go-live.
- **COA 4 diversifies the other two.** COAs 1 and 2 both make directional bets on the 15-minute markets. COA 4's ladder and cross-asset trades carry much less directional risk.
- **What we keep from COA 3:** feeding COA 1's P into COA 2 as a fixed input, with a prior centred on "flow adds nothing", does the combining.
- **What we keep from COA 5:** a basic execution layer (choosing maker vs taker, timing entries with COA 2's 30-second price-move signal). No speed race; we avoid strategies that depend on picking off stale quotes.

**New risks from running all three**
- **One common weak point:** all three depend on the index feed. Kalshi's `cfbenchmarks_value` stream is the primary source. Each window's value is checked against `expiration_value`, and trading stops automatically if the feed goes stale or disagrees with settlement.
- **Overlapping positions:** on windows that close on the hour, COA 1 and COA 4 can trade the same contract. One shared position-and-risk book nets them.
- **COA 2 has to earn its place:** it is compared against COA 1 alone on out-of-sample log loss and on P&L after fees. If flow doesn't improve things, it is dropped.

**Compressed schedule (about 6 weeks to small-size live trading):** week 1 recorder + backfill + index history, week 2 COA 1, week 3 COA 1 shadow trading + COA 4 no-arbitrage scanner, week 4 COA 4 models + COA 2 v1, week 5 combined shadow trading + risk controls, week 6 go/no-go and live at small size. COA 2 v2 comes in weeks 8–10. Details, data volumes and the live-capture architecture are in the [system design](../design/02-system-design-coa-1-2-4.md).

---

## Sources

- Kalshi API docs index: https://docs.kalshi.com/llms.txt
- Kalshi Get Market (Market object fields): https://docs.kalshi.com/api-reference/market/get-market
- Kalshi WebSocket market ticker channel: https://docs.kalshi.com/websockets/market-ticker.md
- Kalshi WebSocket connection / channels: https://docs.kalshi.com/getting_started/quick_start_websockets
- Kalshi CF Benchmarks value feed (official index over WebSocket): https://docs.kalshi.com/websockets/cfbenchmarks-value , 5 Hz: https://docs.kalshi.com/websockets/cfbenchmarks-value-5hz
- Kalshi CF Benchmarks REST passthrough (index history): https://docs.kalshi.com/cfbenchmarks/rest-passthrough
- Kalshi order-book updates channel: https://docs.kalshi.com/websockets/orderbook-updates.md
- Kalshi rate limits: https://docs.kalshi.com/getting_started/rate_limits
- Kalshi Help Center, crypto markets (60-second CF RTI average): https://help.kalshi.com/en/articles/13823838-crypto-markets
- CFTC filing, Kalshi bitcoin contract amendment to a 60-second average: https://cftc.gov/filings/orgrules/rules1112248605.pdf
- KXBTC15M rules text mirror: https://exchange.pmxt.dev/markets/kxbtc15m/bitcoin-price-abovebelow/kxbtc15m-26sep021900-00
- KXBTCD rules mirror: https://simplefunctions.dev/markets/KXBTCD-26JUL1717-T65499.99
- 15-minute markets overview (asset coverage): https://sailgp.com/prediction-markets/kalshi/15-minute-markets , https://www.thelines.com/prediction-markets/kalshi/15-minute-markets/
- Settlement source notes: https://defirate.com/prediction-markets/crypto/sources/
- Trade fields (third-party mirrors): https://docs.dune.com/data-catalog/curated/prediction-markets/kalshi/market_trades , https://docs.oddpool.com/kalshi/trades.md
- Fees: https://defirate.com/prediction-markets/fees/ , https://whirligigbear.substack.com/p/makertaker-math-on-kalshi
- Polymarket 15-minute markets and Chainlink settlement: https://www.mexc.com/news/94651 , https://dev.to/lkto1m/why-my-polymarket-bot-watches-chainlink-not-binance-the-oracle-gap-that-changes-every-close-1e96
- Cross-venue arbitrage caveats: https://pm.wiki/uk/learn/polymarket-kalshi-arbitrage , https://github.com/CarlosIbCu/polymarket-kalshi-btc-arbitrage-bot
