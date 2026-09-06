# Market Data Backend — Detailed Design

Implementation-ready design for the FinAlly market data subsystem: a unified data-source interface with two implementations (GBM simulator, Massive/Polygon.io REST poller) behind a shared thread-safe price cache, feeding a Server-Sent Events stream to the frontend.

**Status note:** This subsystem is already built, tested (73 tests, 84% coverage), and reviewed — see `planning/MARKET_DATA_SUMMARY.md` for the build summary and `planning/archive/MARKET_DATA_REVIEW.md` for the code review that shaped it. This document is the consolidated, as-implemented design reference: every code sample below is verified against the real source in `backend/app/market/`, not a pre-implementation sketch (the earlier sketch that predates the build lives at `planning/archive/MARKET_DATA_DESIGN.md` for history). Sections 10–11 (FastAPI lifecycle wiring, watchlist coordination) describe integration points that the rest of the backend (not yet built) needs to implement against this subsystem.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model)
4. [Price Cache — `cache.py`](#4-price-cache)
5. [Abstract Interface — `interface.py`](#5-abstract-interface)
6. [Seed Prices & Ticker Parameters — `seed_prices.py`](#6-seed-prices--ticker-parameters)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client)
9. [Factory — `factory.py`](#9-factory)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint)
11. [FastAPI Lifecycle Integration](#11-fastapi-lifecycle-integration)
12. [Watchlist Coordination](#12-watchlist-coordination)
13. [Testing Strategy](#13-testing-strategy)
14. [Error Handling & Edge Cases](#14-error-handling--edge-cases)
15. [Configuration Summary](#15-configuration-summary)

---

## 1. Architecture Overview

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io/Massive REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory, single source of truth)
        │
        ├──→ SSE stream endpoint (GET /api/stream/prices)
        ├──→ Portfolio valuation (reads cache.get_price(ticker))
        └──→ Trade execution (reads cache.get_price(ticker) at fill time)
```

Design goals that shape every decision below:

- **One data model everywhere.** Whether a price came from GBM simulation or a live Massive poll, every downstream consumer sees the same `PriceUpdate` shape. Neither the simulator's internals nor Massive's `TickerSnapshot`/`Agg`/`LastTrade` SDK objects ever leave `app/market/`.
- **Producers push, they don't return.** `MarketDataSource.start()` doesn't hand back prices synchronously — it kicks off a background task that writes into a shared cache on its own schedule (500ms for the simulator, 15s+ for Massive). Callers always read from the cache, never from the source directly.
- **Swappable via one env var.** `MASSIVE_API_KEY` set and non-empty → real data; otherwise → simulator. Nothing else in the app (SSE stream, trade execution, portfolio valuation) needs to know or care which one is active — this is the strategy pattern's whole point.
- **Thread-safe cache, decoupled cadences.** The cache is written from a background asyncio task (or an `asyncio.to_thread` worker, for Massive's blocking SDK call) and read from request-handling coroutines at a different, fixed cadence. A monotonic version counter lets readers cheaply detect "nothing changed" without diffing payloads.

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py             # Re-exports: PriceUpdate, PriceCache, MarketDataSource,
                               #   create_market_data_source, create_stream_router
      models.py                # PriceUpdate dataclass
      cache.py                  # PriceCache (thread-safe in-memory store)
      interface.py               # MarketDataSource ABC
      seed_prices.py               # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, correlation constants
      simulator.py                  # GBMSimulator + SimulatorDataSource
      massive_client.py              # MassiveDataSource
      factory.py                      # create_market_data_source()
      stream.py                        # create_stream_router() — SSE endpoint factory
  tests/
    market/                            # 73 tests across 6 modules (see §13)
  market_data_demo.py                   # Rich terminal demo — `uv run market_data_demo.py`
```

Each file has a single responsibility. `__init__.py` re-exports the public surface so the rest of the backend imports from `app.market` without reaching into submodules:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source, create_stream_router
```

---

## 3. Data Model

**File: `backend/app/market/models.py`**

`PriceUpdate` is the only structure that leaves the market data layer.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True, slots=True`** — price updates are immutable value objects, safe to share across async tasks without copying, and slotted to avoid per-instance `__dict__` overhead since many are created per second.
- **`change`, `change_percent`, `direction` are computed properties, not stored fields.** There's exactly one source of truth (`price`, `previous_price`); a derived field can never drift out of sync with the values it's derived from.
- **`to_dict()` is the single serialization point** used by both the SSE endpoint and any future REST responses that need to embed a price.

---

## 4. Price Cache

**File: `backend/app/market/cache.py`**

The central hub: exactly one data source writes to it at a time; the SSE endpoint, trade execution, and portfolio valuation all read from it.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why `update()` recomputes `previous_price` itself

The cache — not the data source — decides what "previous" means: `previous_price` is always "whatever this cache last held for this ticker," not whatever the source's own upstream data claims. This matters most for Massive: `TickerSnapshot.prev_day.close` reflects the *prior trading session's* close, not the prior *poll*. If `massive_client.py` used that value directly, `direction`/`change` would answer "vs. yesterday" instead of "vs. the last poll" — the wrong signal for a live price-flash animation. Recomputing from the cache's own last-seen value keeps the semantics identical regardless of which source is active.

**First update for a ticker is always `direction="flat"`** — there's nothing to compare against yet, so newly-added tickers don't spuriously flash green/red on their first render.

### Why a version counter, and why a plain `threading.Lock`

The SSE loop polls the cache every ~500ms. Without a way to detect "nothing changed," it would re-serialize and re-send every ticker's price every tick even when the active source is Massive, which only updates every 15s+. A single monotonic integer, bumped on every `update()` call, turns that check into an `int !=` comparison instead of a payload diff:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        send(price_cache.get_all())
    await asyncio.sleep(0.5)
```

`threading.Lock` (not `asyncio.Lock`) is deliberate: the Massive client's synchronous `get_snapshot_all()` call runs via `asyncio.to_thread`, which executes in a real OS thread pool — an `asyncio.Lock` would not serialize access against that thread. `threading.Lock` works correctly from both a plain thread and the event loop, and the critical sections here (dict lookup/assignment) are small enough that contention is a non-issue at this project's scale (tens of tickers, low-frequency reads).

---

## 5. Abstract Interface

**File: `backend/app/market/interface.py`**

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

Both `SimulatorDataSource` and `MassiveDataSource` implement this exactly. None of `start`/`stop`/`add_ticker`/`remove_ticker` returns price data — every one of them mutates the shared cache as a side effect. This push model is what decouples timing: the simulator ticks at 500ms, Massive polls at 15s+, but the SSE layer always reads the cache at its own fixed 500ms cadence and doesn't need to know which source (or update interval) is currently active.

---

## 6. Seed Prices & Ticker Parameters

**File: `backend/app/market/seed_prices.py`**

Constants only — no logic, no imports beyond type annotations. Shared by the simulator for initial prices, GBM parameters, and correlation structure.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},   # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},   # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},    # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},      # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6      # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR = 0.3     # Between sectors / unknown tickers
TSLA_CORR = 0.3            # TSLA does its own thing
```

A ticker added at runtime that isn't in `SEED_PRICES`/`TICKER_PARAMS` (e.g. the user or the AI chat adds `"PYPL"` to the watchlist) starts at a random price in `[50, 300)` with `DEFAULT_PARAMS` — the simulation never fails on an unknown symbol, it just behaves like a generic mid-cap.

`seed_prices.py` is deliberately just data. Tuning the simulator's "personality" (which tickers are volatile, which move together) never requires touching `simulator.py`'s logic.

---

## 7. GBM Simulator

**File: `backend/app/market/simulator.py`**

Two classes live here: `GBMSimulator` (pure math engine, no asyncio, no cache dependency — independently unit-testable) and `SimulatorDataSource` (the `MarketDataSource` implementation that wraps it in an async loop and writes to the cache).

### 7.1 The math

Geometric Brownian Motion — the same stochastic process underlying Black-Scholes — advances each price as:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

- `S(t)` — current price
- `mu` — annualized drift (expected return), e.g. `0.05` for 5%/year
- `sigma` — annualized volatility, e.g. `0.20` for 20%/year
- `dt` — this time step, as a fraction of a trading year
- `Z` — a (correlated) standard normal draw

This form guarantees prices never go negative (the update is multiplicative via `exp()`), reproduces the fat-tailed, lognormal-ish behavior real equity prices show, and — with per-ticker `sigma` — makes high-volatility tickers (TSLA at 0.50) visibly swing more than low-volatility ones (V at 0.17) without any special-cased step logic.

`dt` for a 500ms tick, assuming 252 trading days × 6.5 hours/day:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ≈ 8.48e-8
```

This tiny `dt` keeps each individual tick a realistic sub-cent jitter — a stock doesn't visibly jump or crater on any single 500ms step — while thousands of ticks compounded over a session still reproduce plausible intraday ranges.

### 7.2 Correlated moves via Cholesky decomposition

Real stocks don't move independently. Given a target correlation matrix `C`, compute `L = cholesky(C)`; then for independent standard normals `Z_independent`, `Z_correlated = L @ Z_independent` has exactly the target correlation structure. Pairwise correlation is looked up by sector group, with TSLA special-cased to always behave as an idiosyncratic outlier — checked *before* the tech-set membership check, so a TSLA/AAPL pair gets `0.3`, not `0.6`, even though TSLA is a member of the `"tech"` set for `TICKER_PARAMS` lookup purposes.

The correlation matrix (and its Cholesky factor) is rebuilt whenever a ticker is added or removed — O(n²), acceptable given the watchlist stays in the tens of tickers.

### 7.3 Random shock events

Every step, each ticker independently has a small chance of a sudden 2–5% move, in either direction — mimicking a news-driven jump:

```python
event_probability = 0.001   # ~0.1% chance per tick per ticker

if random.random() < event_probability:
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    price *= 1 + shock_magnitude * shock_sign
```

At 2 ticks/second with the 10-ticker default watchlist, expect a visible jump somewhere in the dashboard roughly every ~50 seconds — frequent enough to keep the terminal feeling alive, rare enough not to look absurd.

### 7.4 `GBMSimulator` — full implementation

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    The tiny dt (~8.5e-8 for 500ms ticks over 252 trading days * 6.5h/day)
    produces sub-cent moves per tick that accumulate naturally over time.
    """

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)   # no rebuild per ticker during batch init
        self._rebuild_cholesky()                 # one rebuild after all initial tickers are in

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        Hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker, shock_magnitude * 100, "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n stays well
        under 50 in practice.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None   # nothing to correlate with fewer than 2 tickers
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping.

        Precedence matters: the TSLA check runs BEFORE the tech-set check, so
        any pair involving TSLA gets 0.3, never the 0.6 intra-tech rate.
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

Notes on the shape of this code:

- **`step()` returns a plain `{ticker: price}` dict** — it has no knowledge of `PriceUpdate` or the cache. Only `SimulatorDataSource._run_loop()` turns simulator output into cache writes.
- **`_add_ticker_internal` vs. `add_ticker`**: batch construction in `__init__` adds every starting ticker without rebuilding the correlation matrix each time, then rebuilds once at the end — O(n²) once instead of n times. The public `add_ticker()` (used when the watchlist changes at runtime) always rebuilds immediately, since only one ticker is ever being added at a time.
- **`_cholesky = None` for n ≤ 1 is a real branch**, not a placeholder — with zero or one ticker there's no pairwise correlation to compute, and `step()` falls back to the independent draw directly.
- **Prices can never go negative** — every update is `price *= exp(...)`, and `exp()` is always positive.

### 7.5 `SimulatorDataSource` — async wrapper

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Key behaviors: the cache is seeded **before** the loop starts, so an SSE client connecting immediately after startup never sees an empty payload; `stop()` cancels the task and awaits it, catching `CancelledError`, so shutdown is clean; and `_run_loop` catches exceptions per-step so one bad tick can't silently kill the whole data feed.

---

## 8. Massive API Client

**File: `backend/app/market/massive_client.py`**

Polls the Massive (formerly Polygon.io) REST snapshot endpoint on a configurable interval. See `planning/MASSIVE_API.md` for the full wire-level API reference this wraps.

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        await self._poll_once()   # immediate poll so cache isn't empty at t=0

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval", len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop will retry on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Design notes

- **The blocking SDK call runs via `asyncio.to_thread`.** `massive.RESTClient` is synchronous; without offloading it, every poll would stall the event loop — and therefore the SSE stream and every other request — for the duration of the HTTP round trip.
- **A malformed snapshot for one ticker doesn't take down the poll.** Each snapshot is processed in its own `try`/`except`; one bad entry is logged and skipped, the rest still update.
- **A failed poll (network error, 429, bad key) doesn't crash the background task.** It's logged and the loop retries on the next `poll_interval` — the whole subsystem's "log and continue" posture, since a transient data-provider outage shouldn't take the app down.
- **`massive` is a top-level import, not lazy.** It's declared as a core dependency in `pyproject.toml` (`massive>=1.0.0`), so it's always available — no conditional import gymnastics needed at the cost of requiring the package even for simulator-only runs (a deliberate, already-made tradeoff; see `planning/archive/MARKET_DATA_REVIEW.md` §3.1–3.2 for the history of why an earlier lazy-import draft was reworked).

### ⚠️ Known gotcha — timestamp field bug

`snap.last_trade.timestamp` **does not exist on the real Massive SDK model.** `LastTrade` only exposes `sip_timestamp` / `participant_timestamp` / `trf_timestamp` (see `planning/MASSIVE_API.md` §5.1, §9). Against a real API key, every snapshot currently raises `AttributeError` inside the per-snapshot `try`/`except`, gets logged as "Skipping snapshot," and **the cache never updates when running against the real API.** The existing unit tests don't catch this because they mock the snapshot with `MagicMock()`, which happily fabricates a `.timestamp` attribute that doesn't exist on the real class.

**Fix, when this path is next touched:** use `snap.last_trade.sip_timestamp / 1_000_000_000` (nanoseconds → seconds), or simply omit the `timestamp=` argument to `cache.update()` and let it default to `time.time()` (arrival time) — which is in fact the more defensible choice anyway, since all of Massive's `*_timestamp` fields are documented inconsistently across providers and shouldn't be trusted blindly (`planning/MASSIVE_API.md` §5.1).

### Error handling summary

| Error | Behavior |
|-------|----------|
| **401 Unauthorized** | Logged as error; poller keeps running (user might fix `.env` and restart). |
| **429 Rate Limited** | Logged as error; next poll retries after `poll_interval` seconds — no dedicated backoff beyond the fixed interval. |
| **Network timeout** | Logged as error; retries automatically on next cycle. |
| **Malformed snapshot** (incl. the timestamp bug above) | That ticker is skipped with a warning; other tickers in the same poll still process. |
| **All tickers fail** | Cache retains last-known prices; SSE keeps streaming stale data (better than no data). |

---

## 9. Factory

**File: `backend/app/market/factory.py`**

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

This is the entire selection mechanism described in `PLAN.md` §5/§6 — one env var, checked once, at startup. `.strip()` guards against an `.env` file with `MASSIVE_API_KEY=` (present but empty) being mistaken for "configured."

```python
# Usage at app startup
price_cache = PriceCache()
source = create_market_data_source(price_cache)
await source.start(initial_tickers)  # e.g., ["AAPL", "GOOGL", ...]
```

---

## 10. SSE Streaming Endpoint

**File: `backend/app/market/stream.py`**

The only consumer that pushes to the frontend; completely unaware of which `MarketDataSource` is active.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    yield "retry: 1000\n\n"   # browser retries after 1s if the connection drops

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{"ticker":"GOOGL","price":175.12,...}}

```

Frontend consumption:

```javascript
const eventSource = new EventSource('/api/stream/prices');
eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices is { "AAPL": { ticker, price, previous_price, ... }, ... }
};
```

### Why poll-and-push instead of event-driven

The endpoint polls the cache on a fixed 500ms interval rather than being notified by the data source. This is simpler, and produces predictable, evenly-spaced updates — important because the frontend accumulates SSE events client-side into sparkline charts, where regular spacing matters for a clean visualization. Polling `price_cache.version` rather than diffing dict contents is what keeps this cheap even at 500ms: it's an integer comparison. It also decouples the SSE loop's own cadence from whichever source is currently updating the cache — if Massive hasn't polled again yet, `version` is unchanged and the loop simply sends nothing that tick, rather than resending stale data.

---

## 11. FastAPI Lifecycle Integration

**Not yet built** — this section specifies how the (not-yet-existing) `backend/app/main.py` should wire up the market data subsystem via FastAPI's `lifespan` context manager, so whoever builds the app entrypoint has a concrete contract to implement against.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, MarketDataSource, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown of background services."""

    # --- STARTUP ---

    # 1. Create the shared price cache
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    # 2. Create and start the market data source
    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # 3. Load initial tickers from the database watchlist (lazy-init on first run;
    #    see PLAN.md §7 for the seed watchlist: AAPL, GOOGL, MSFT, AMZN, TSLA,
    #    NVDA, META, JPM, V, NFLX)
    initial_tickers = await load_watchlist_tickers()  # reads from SQLite
    await source.start(initial_tickers)

    # 4. Register the SSE streaming router
    app.include_router(create_stream_router(price_cache))

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# Dependencies for injecting market-data state into route handlers
def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### Accessing market data from other routes

Trade execution, portfolio valuation, and watchlist management should reach the cache and the active source through FastAPI dependency injection, never by importing `app.market` internals directly:

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(404, f"No price available for {trade.ticker}")
    # ... execute trade at current_price ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... insert into watchlist table ...
    await source.add_ticker(payload.ticker)
    # ... return ticker + current price if available ...


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... delete from watchlist table ...
    await source.remove_ticker(ticker)
```

---

## 12. Watchlist Coordination

When the watchlist changes — via the REST API or the LLM chat's `watchlist_changes` (`PLAN.md` §9) — the active `MarketDataSource` must be told, so it tracks the right ticker set. This is prescriptive design for the watchlist/portfolio routes, not yet implemented.

### Flow: adding a ticker

```
User (or LLM) → POST /api/watchlist {ticker: "PYPL"}
  → Insert into watchlist table (SQLite)
  → await source.add_ticker("PYPL")
      Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
      Massive:   appends to ticker list, appears on next poll (up to poll_interval later)
  → Return success (ticker + current price, if already cached)
```

### Flow: removing a ticker

```
User (or LLM) → DELETE /api/watchlist/PYPL
  → Delete from watchlist table (SQLite)
  → await source.remove_ticker("PYPL")
      Simulator: removes from GBMSimulator, rebuilds Cholesky, removes from cache
      Massive:   removes from ticker list, removes from cache
  → Return success
```

### Edge case: ticker still has an open position

If the user removes a ticker from the watchlist while still holding shares, the data source must keep tracking it so portfolio valuation stays accurate — the watchlist route, not the market data layer, is responsible for this check:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)

    # Only stop tracking if there's no open position
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok"}
```

This means the market data layer's ticker set is not always identical to the watchlist table — it's the *union* of the watchlist and any ticker with a nonzero position. Whoever builds portfolio/watchlist routes should call `source.add_ticker()` for a ticker with a new position even if it isn't (or is no longer) on the watchlist.

---

## 13. Testing Strategy

**As built:** 73 tests across 6 modules in `backend/tests/market/`, all passing, 84% overall coverage.

| Module | Tests | Coverage | What it covers |
|--------|-------|----------|-----------------|
| `test_models.py` | 11 | 100% | `PriceUpdate` computed properties, `to_dict()`, edge cases (zero previous price) |
| `test_cache.py` | 13 | 100% | `update`/`get`/`get_all`/`remove`, first-update-is-flat, direction up/down, version increments |
| `test_simulator.py` | 17 | 98% | GBM math, prices always positive, correlation/Cholesky rebuild on add/remove, unknown-ticker fallback, random events |
| `test_simulator_source.py` | 10 | (integration) | `SimulatorDataSource` async lifecycle: start seeds cache, prices evolve over time, stop is idempotent, add/remove ticker |
| `test_factory.py` | 7 | 100% | Env var selection (Massive vs. simulator), `.strip()` on empty-string key |
| `test_massive.py` | 13 | 56% (expected) | `_poll_once` cache updates, malformed-snapshot skip, API-error resilience — all against mocked `RESTClient`/snapshots, since real API calls aren't exercised in CI |

Representative examples:

```python
# test_simulator.py
def test_prices_are_positive():
    """GBM prices can never go negative (exp() is always positive)."""
    sim = GBMSimulator(tickers=["AAPL"])
    for _ in range(10_000):
        prices = sim.step()
        assert prices["AAPL"] > 0


def test_cholesky_rebuilds_on_add():
    sim = GBMSimulator(tickers=["AAPL"])
    assert sim._cholesky is None      # only 1 ticker, no correlation matrix
    sim.add_ticker("GOOGL")
    assert sim._cholesky is not None  # now 2 tickers, matrix exists
```

```python
# test_cache.py
def test_direction_up():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    update = cache.update("AAPL", 191.00)
    assert update.direction == "up"
    assert update.change == 1.00


def test_version_increments():
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.00)
    assert cache.version == v0 + 1
```

```python
# test_massive.py — mocking the SDK's snapshot shape
def _make_snapshot(ticker: str, price: float, timestamp_ms: int) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap


async def test_malformed_snapshot_skipped():
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
    source._tickers = ["AAPL", "BAD"]

    good_snap = _make_snapshot("AAPL", 190.50, 1707580800000)
    bad_snap = MagicMock()
    bad_snap.ticker = "BAD"
    bad_snap.last_trade = None  # triggers AttributeError

    with patch.object(source, "_fetch_snapshots", return_value=[good_snap, bad_snap]):
        await source._poll_once()

    assert cache.get_price("AAPL") == 190.50
    assert cache.get_price("BAD") is None
```

Run the suite:

```bash
cd backend
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app
```

### Gaps worth closing later

- **`stream.py` sits at 31% coverage** — the SSE generator needs a running ASGI test client (e.g. `httpx.AsyncClient(app=app, ...)`) to exercise properly; it has no dedicated test today.
- **No concurrent-writer test for `PriceCache`** — the lock usage looks correct by inspection, but a test with multiple threads writing simultaneously would verify it empirically.
- **No 10-ticker Cholesky test** — existing simulator tests use 1–2 tickers; a test building the full default 10-ticker correlation matrix would catch any positive-semi-definiteness regression if the correlation constants are ever hand-edited.
- **The `massive` timestamp bug (§8)** has no test that would catch it against the real SDK model, since mocks use `MagicMock()` which fabricates the nonexistent `.timestamp` attribute. A test built against `massive`'s actual `LastTrade` dataclass (or a `spec=`'d mock) would catch this class of drift.

---

## 14. Error Handling & Edge Cases

### 14.1 Startup with an empty watchlist

If the database has no watchlist entries, `start([])` is called. Both sources handle this gracefully: the simulator's `step()` returns `{}` for zero tickers, and the Massive poller's `_poll_once()` returns immediately (`if not self._tickers: return`). The SSE endpoint sends nothing until a ticker is added, at which point the relevant `add_ticker()` starts tracking it immediately.

### 14.2 Cache miss during trade execution

If a user tries to trade a ticker with no cached price yet (just added, Massive hasn't polled), the trade route should surface a clear error rather than executing at a stale/missing price:

```python
price = price_cache.get_price(ticker)
if price is None:
    raise HTTPException(
        status_code=400,
        detail=f"Price not yet available for {ticker}. Please wait a moment and try again.",
    )
```

The simulator avoids this entirely by seeding the cache synchronously in both `start()` and `add_ticker()`. Massive has a real (if brief) gap between `add_ticker()` and the next poll cycle — the 400 with a clear message is the correct response there.

### 14.3 Invalid Massive API key

A bad key surfaces as a 401 on the very first poll. The poller logs the error and keeps retrying every `poll_interval` — it does not crash or fall back to the simulator. The SSE endpoint keeps streaming (connection status shows "connected"), but with no data, since the cache never gets populated. The fix is operational: correct `MASSIVE_API_KEY` in `.env` and restart the container.

### 14.4 Thread safety under load

`PriceCache`'s `threading.Lock` serializes all reads and writes. At this project's scale (tens of tickers, a handful of readers) contention is negligible — the critical section is a dict lookup plus assignment. This would only need revisiting (e.g., a read-write lock) if the watchlist grew into the hundreds with many concurrent SSE readers, which is outside this project's target scale.

### 14.5 Simulator numerical precision

GBM with the tiny `dt` here produces very small per-tick moves; this isn't a precision concern because prices are rounded to 2 decimal places in `GBMSimulator.step()`, the `exp(drift + diffusion)` formulation is numerically stable, and the multiplicative update guarantees positivity regardless of how small the increment gets.

---

## 15. Configuration Summary

| Parameter | Location | Default | Description |
|-----------|----------|---------|--------------|
| `MASSIVE_API_KEY` | Environment variable | `""` (empty) | If set and non-empty, use Massive API; otherwise use the simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` (seconds) | Time between simulator ticks |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` (seconds) | Time between Massive API polls (free tier: don't go below ~12–15s) |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Chance of a random shock event per ticker per tick |
| `dt` | `GBMSimulator.__init__` | `~8.5e-8` | GBM time step, as a fraction of a trading year |
| SSE push interval | `_generate_events()` | `0.5` (seconds) | Time between cache polls / potential pushes to the client |
| SSE retry directive | `_generate_events()` | `1000` (ms) | Browser `EventSource` reconnection delay after a dropped connection |

### `__init__.py` — public surface

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```
