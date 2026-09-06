# Market Simulator

Approach and code structure for simulating realistic stock prices when no `MASSIVE_API_KEY` is configured — the default mode for FinAlly (`PLAN.md` §6). This document describes the simulator **as implemented** in `backend/app/market/simulator.py` and `backend/app/market/seed_prices.py`; it supersedes the pre-implementation sketch in `planning/archive/MARKET_SIMULATOR.md`. See `planning/MARKET_INTERFACE.md` for how `SimulatorDataSource` plugs into the shared `MarketDataSource`/`PriceCache` interface.

## 1. Overview

The simulator uses **Geometric Brownian Motion (GBM)** — the same stochastic process underlying Black-Scholes option pricing — to generate price paths that:

- never go negative (the update is multiplicative, via `exp()`),
- exhibit the lognormal-ish, fat-tailed-ish behavior real equity prices show,
- move together across related tickers (tech stocks correlate, so a portfolio heatmap doesn't look like independent noise), and
- occasionally jump 2–5% for visual drama, mimicking news-driven moves.

Updates run on a 500ms tick, driven by `SimulatorDataSource._run_loop()` (see `MARKET_INTERFACE.md` §7); each tick calls `GBMSimulator.step()` for every currently-tracked ticker.

## 2. GBM Math

At each time step, price evolves as:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

- `S(t)` — current price
- `mu` — annualized drift (expected return), e.g. `0.05` for 5%/year
- `sigma` — annualized volatility, e.g. `0.20` for 20%/year
- `dt` — this time step, expressed as a fraction of a trading year
- `Z` — a (correlated, see §3) standard normal draw

`dt` for a 500ms tick, assuming 252 trading days × 6.5 hours/day:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ≈ 8.48e-8
```

This tiny `dt` is what keeps individual ticks looking like realistic sub-cent jitter rather than a stock instantly becoming worthless or doubling: over one 500ms step the drift and diffusion terms are both very small, but compounded over thousands of ticks in a session they reproduce plausible intraday ranges (higher `sigma` tickers like TSLA visibly swing more than low-`sigma` tickers like JPM over the same session, without either exploding).

## 3. Correlated Moves

Real stocks don't move independently. The simulator builds correlated draws via **Cholesky decomposition** of a sector-based correlation matrix: given correlation matrix `C`, compute `L = cholesky(C)`, then for independent standard normals `Z_independent`, `Z_correlated = L @ Z_independent` has exactly the target correlation structure.

Correlation groups, from `seed_prices.py`:

```python
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6      # tech stocks move together
INTRA_FINANCE_CORR = 0.5   # finance stocks move together
CROSS_GROUP_CORR = 0.3     # different sectors, or an unknown ticker
TSLA_CORR = 0.3            # TSLA is checked first and always gets this, even though it's in the tech set
```

Pairwise lookup (`GBMSimulator._pairwise_correlation`), in the exact precedence order the code uses:

```python
@staticmethod
def _pairwise_correlation(t1: str, t2: str) -> float:
    tech = CORRELATION_GROUPS["tech"]
    finance = CORRELATION_GROUPS["finance"]

    if t1 == "TSLA" or t2 == "TSLA":      # checked BEFORE the tech-set check
        return TSLA_CORR
    if t1 in tech and t2 in tech:
        return INTRA_TECH_CORR
    if t1 in finance and t2 in finance:
        return INTRA_FINANCE_CORR
    return CROSS_GROUP_CORR
```

TSLA is a member of the `"tech"` set (for the sake of `TICKER_PARAMS` lookups elsewhere) but the TSLA check runs first, so any pair involving TSLA — including TSLA-vs-another-tech-stock — always gets `0.3`, not `0.6`. This is a deliberate choice ("TSLA does its own thing"), not an oversight: it keeps TSLA behaving like an idiosyncratic, high-volatility, low-correlation outlier rather than tracking the rest of tech.

The correlation matrix is rebuilt (`_rebuild_cholesky`) whenever a ticker is added or removed — O(n²), but n stays well under 50 in practice, so this is cheap even done synchronously on every watchlist edit.

## 4. Random Events

Every step, each ticker independently has a small chance of a sudden 2–5% move, in either direction:

```python
event_probability = 0.001   # ~0.1% chance per tick per ticker

if random.random() < event_probability:
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    price *= 1 + shock_magnitude * shock_sign
```

At 2 ticks/second, `0.001` works out to roughly one event every ~500 seconds *per ticker*; with 10 tickers on the default watchlist, expect a visible jump somewhere in the dashboard roughly every 50 seconds — frequent enough to keep the terminal feeling alive, rare enough not to look absurd.

## 5. Seed Prices & Per-Ticker Parameters

`seed_prices.py` holds only constants — no logic:

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00, "TSLA": 250.00,
    "NVDA": 800.00, "META": 500.00, "JPM": 195.00, "V": 280.00, "NFLX": 600.00,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # high volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # high volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}   # for dynamically added tickers
```

A ticker added at runtime that isn't in `SEED_PRICES`/`TICKER_PARAMS` (e.g., the user or the AI chat adds `"PYPL"` to the watchlist) starts at a random price in `[50, 300)` with `DEFAULT_PARAMS`, so the simulation never fails on an unknown symbol — it just behaves like a generic mid-cap.

## 6. Implementation — `GBMSimulator`

`app/market/simulator.py`. This is the pure-computation core; it holds no asyncio state and knows nothing about `PriceCache` — that separation is what makes it independently unit-testable (see `backend/tests/market/test_simulator.py`, 17 tests, 98% coverage).

```python
class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT, event_probability: float = 0.001) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for ticker in tickers:
            self._add_ticker_internal(ticker)   # no rebuild per-ticker during batch init
        self._rebuild_cholesky()                 # one rebuild after all initial tickers are in

    def step(self) -> dict[str, float]:
        """Advance every tracked ticker by one time step. Hot path — called every 500ms."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker mid-session. Rebuilds the correlation matrix (O(n^2))."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky — used for batch init in __init__."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None    # nothing to correlate with fewer than 2 tickers
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)
```

Notes on the shape of this code:

- **`step()` returns a plain `{ticker: price}` dict** — it doesn't know about `PriceUpdate` or the cache. `SimulatorDataSource._run_loop()` is the only thing that turns simulator output into cache writes (`MARKET_INTERFACE.md` §7).
- **`_add_ticker_internal` vs. `add_ticker`**: batch construction in `__init__` adds every starting ticker without rebuilding the correlation matrix each time, then rebuilds once at the end — O(n²) once instead of n times. `add_ticker()` (the public, one-at-a-time method used when the watchlist changes at runtime) always rebuilds immediately, since there's only ever one ticker being added.
- **`_cholesky = None` for n ≤ 1** is a real branch, not a placeholder — with zero or one ticker there's no pairwise correlation to compute, and `step()` falls back to using the independent draw directly.

## 7. File Structure

```
backend/
  app/
    market/
      simulator.py       # GBMSimulator (pure computation) + SimulatorDataSource (asyncio wrapper)
      seed_prices.py       # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, CORRELATION_GROUPS, *_CORR constants
  tests/
    market/
      test_simulator.py           # GBMSimulator unit tests (math, correlation, events, add/remove)
      test_simulator_source.py    # SimulatorDataSource integration tests (asyncio loop, cache writes)
```

`seed_prices.py` is deliberately just data — no functions, no classes — so tuning the simulator's "personality" (which tickers are volatile, which move together) never requires touching `simulator.py`'s logic.

## 8. Behavior Notes

- Prices can't go negative — every update is `price *= exp(...)`, and `exp()` is always positive.
- `sigma=0.50` (TSLA) vs. `sigma=0.17` (V) produces a visibly wider intraday range for TSLA over a session, without either ticker needing special-cased step logic — it all falls out of the same formula with different parameters.
- Random events fire ~0.1% of ticks per ticker; with the 10-ticker default watchlist at 2 ticks/sec, expect a visible jump roughly every 50 seconds somewhere on the dashboard.
- The correlation matrix must be positive semi-definite for `np.linalg.cholesky` to succeed — every pairwise value used here (0.3/0.5/0.6) keeps the matrix comfortably valid; this would only become a concern if someone hand-edited the correlation constants to something inconsistent.
- Rebuilding Cholesky on every watchlist add/remove is O(n²), acceptable for the tens-of-tickers scale this project targets — it would need revisiting only if the watchlist grew into the hundreds.
- The demo script `backend/market_data_demo.py` (`uv run market_data_demo.py`) renders a live terminal dashboard driven by exactly this simulator — useful for eyeballing that the correlation/volatility/event tuning still "feels right" after any parameter change, without needing the frontend running.
