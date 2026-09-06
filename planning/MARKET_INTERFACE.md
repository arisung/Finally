# Market Data Interface

Unified Python interface for market data in FinAlly: one abstract data-source contract with two implementations (simulator and Massive API) behind it, and a shared in-memory cache that decouples producers (the data sources) from consumers (SSE streaming, portfolio valuation, trade execution). All downstream code is source-agnostic — it only ever talks to `PriceCache`.

This document describes the interface **as implemented** in `backend/app/market/` (see `planning/MARKET_DATA_SUMMARY.md` for the build/test summary). It supersedes the earlier pre-implementation sketch in `planning/archive/MARKET_INTERFACE.md` — a few field names and behaviors changed during implementation and review. For the Massive-specific wire details referenced here, see `planning/MASSIVE_API.md`; for the simulator's math, see `planning/MARKET_SIMULATOR.md`.

## 1. Design Goals

- **One data model everywhere.** Whether a price came from GBM simulation or a live Massive poll, downstream code sees the same `PriceUpdate` shape.
- **Producers don't return data, they push it.** `MarketDataSource.start()` doesn't hand back prices synchronously — it kicks off a background task that writes into a shared cache on its own schedule (500ms for the simulator, 15s+ for Massive). Callers read from the cache whenever they need a price, not from the source.
- **Swappable via one env var.** `MASSIVE_API_KEY` set → real data; unset → simulator. Nothing else in the app (SSE stream, trade execution, portfolio valuation) needs to know or care which one is active.
- **Thread-safe cache, single writer at a time.** Only one `MarketDataSource` is ever running per process, but the cache is written from a background asyncio task and read from request-handling coroutines, so it still guards its state with a lock.

## 2. Core Data Model — `PriceUpdate`

`app/market/models.py`

```python
from dataclasses import dataclass, field
import time

@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        ...
```

Differences from the original design sketch worth calling out: `change`, `change_percent`, and `direction` are **computed properties**, not stored fields — there's exactly one source of truth (`price` and `previous_price`) and no risk of the derived fields drifting out of sync. `to_dict()` centralizes JSON serialization for the SSE payload.

This is the **only** structure that leaves the market data layer. Neither the simulator's GBM internals nor Massive's `TickerSnapshot`/`Agg`/`LastTrade` objects ever escape `app/market/`.

## 3. Abstract Interface — `MarketDataSource`

`app/market/interface.py`

```python
from abc import ABC, abstractmethod

class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers. Call exactly once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. Also removes it from the PriceCache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

Both `SimulatorDataSource` and `MassiveDataSource` implement this. Neither `start`/`stop`/`add_ticker`/`remove_ticker` returns price data — they only ever mutate the shared cache as a side effect.

## 4. Shared Price Cache — `PriceCache`

`app/market/cache.py`

The single point of truth both data sources write to and everything else reads from.

```python
import time
from threading import Lock

class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker."""

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # bumped on every update — cheap SSE change detection

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price. Computes previous_price from whatever was cached before."""
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price   # first update: flat, no fake jump
            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None: ...
    def get_price(self, ticker: str) -> float | None: ...   # convenience: just the float
    def get_all(self) -> dict[str, PriceUpdate]: ...          # shallow copy, safe to iterate
    def remove(self, ticker: str) -> None: ...

    @property
    def version(self) -> int: ...   # monotonic counter for SSE change detection

    def __len__(self) -> int: ...
    def __contains__(self, ticker: str) -> bool: ...
```

Key points:

- **`update()` decides "up/down/flat" implicitly** by diffing against whatever was cached before — the cache is where `previous_price` actually comes from for the simulator (which only ever produces a new absolute price, not a delta) and where it's discarded and recomputed for Massive (whose own `prev_day.close` reflects the prior trading *session*, not the prior poll — using it directly would make `direction` reflect "vs. yesterday" instead of "vs. last poll", which is the wrong signal for a live-price flash animation).
- **First update for a ticker is always `direction="flat"`** — there's nothing to compare against yet, so it doesn't spuriously flash green/red on initial load.
- **`version` exists purely so the SSE endpoint can skip re-sending unchanged data.** It's not a vector clock or per-ticker version — one global counter, incremented on every single `update()` call, is enough for "has anything changed since I last checked."
- **All writes and reads take the same lock** — simple and correct given there's only ever one writer (the active data source's background task) and a handful of readers (SSE loop, trade execution, portfolio valuation) at low frequency.

## 5. Factory — `create_market_data_source`

`app/market/factory.py`

```python
import os

def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty -> MassiveDataSource. Otherwise -> SimulatorDataSource.

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        return SimulatorDataSource(price_cache=price_cache)
```

This is the entire selection mechanism described in `PLAN.md` §5/§6 — one env var, checked once, at startup. `.strip()` guards against an `.env` file with `MASSIVE_API_KEY=` (present but empty) being mistaken for "configured."

## 6. Implementation: Massive (real data)

`app/market/massive_client.py` — see `planning/MASSIVE_API.md` for the full API reference this wraps.

```python
class MassiveDataSource(MarketDataSource):
    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0):
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._client: RESTClient | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()          # immediate poll so cache isn't empty at t=0
        self._task = asyncio.create_task(self._poll_loop())

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # Synchronous SDK call -> run off the event loop
            snapshots = await asyncio.to_thread(
                self._client.get_snapshot_all,
                market_type=SnapshotMarketType.STOCKS,
                tickers=self._tickers,
            )
            for snap in snapshots:
                try:
                    self._cache.update(ticker=snap.ticker, price=snap.last_trade.price)
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
        except Exception as e:
            logger.error("Massive poll failed: %s", e)   # retried next interval, never re-raised
```

Design notes:

- **The blocking SDK call runs via `asyncio.to_thread`.** `massive.RESTClient` is synchronous; without offloading it, every poll would stall the event loop (and therefore the SSE stream, and every other request) for the duration of the HTTP round trip.
- **A malformed snapshot for one ticker doesn't take down the poll.** Each snapshot is processed in its own `try`/`except`; one bad entry is logged and skipped.
- **A failed poll (network error, 429, bad key) doesn't crash the background task.** It's logged and the loop tries again on the next `poll_interval` — same "log and continue" posture the whole subsystem uses, since a transient data-provider outage shouldn't take the app down.
- **See `planning/MASSIVE_API.md` §9** for a real correctness gap in this code as written: `snap.last_trade.timestamp` doesn't exist on the actual SDK model (it's `sip_timestamp`), so against a real API key every snapshot currently hits the `except (AttributeError, TypeError)` branch and gets skipped — worth fixing before this path is exercised with a real key.

## 7. Implementation: Simulator (default)

`app/market/simulator.py` — full math and correlation model in `planning/MARKET_SIMULATOR.md`.

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5, event_probability: float = 0.001):
        self._cache = price_cache
        self._interval = update_interval
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        for ticker in tickers:                      # seed cache immediately, don't wait for tick 1
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop())

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Both implementations share the same shape deliberately: seed the cache synchronously in `start()` before the first tick so a client connecting to the SSE stream immediately after startup never sees an empty payload, then run a loop that writes on an interval and never lets an internal exception kill the background task.

## 8. Integration with SSE

`app/market/stream.py` reads `PriceCache` and is the only consumer that pushes to the frontend; it is completely unaware of which `MarketDataSource` is active.

```python
async def _generate_events(price_cache: PriceCache, request: Request, interval: float = 0.5):
    yield "retry: 1000\n\n"          # tells EventSource to reconnect after 1s if dropped
    last_version = -1
    while True:
        if await request.is_disconnected():
            break
        if price_cache.version != last_version:
            last_version = price_cache.version
            prices = price_cache.get_all()
            if prices:
                yield f"data: {json.dumps({t: u.to_dict() for t, u in prices.items()})}\n\n"
        await asyncio.sleep(interval)
```

Polling `price_cache.version` rather than diffing the dict contents is why this is cheap even at 500ms — it's an integer comparison, not a payload diff. It also means the SSE loop's own poll interval (500ms) is decoupled from whichever source is currently updating the cache (500ms for the simulator, 15s+ for Massive): if Massive hasn't polled again yet, `version` is unchanged and the SSE loop simply sends nothing that tick, rather than resending stale data.

## 9. File Structure (as built)

```
backend/
  app/
    market/
      __init__.py            # re-exports: PriceCache, PriceUpdate, MarketDataSource,
                              #   create_market_data_source, create_stream_router
      models.py               # PriceUpdate
      interface.py             # MarketDataSource ABC
      cache.py                 # PriceCache
      factory.py                # create_market_data_source()
      simulator.py               # GBMSimulator + SimulatorDataSource
      seed_prices.py              # SEED_PRICES, TICKER_PARAMS, correlation constants
      massive_client.py            # MassiveDataSource
      stream.py                     # create_stream_router() — SSE endpoint factory
  tests/market/                    # 73 tests covering every module above
```

## 10. Lifecycle

1. **App startup**: create one `PriceCache`, call `create_market_data_source(cache)`, then `await source.start(initial_tickers)` (the seeded watchlist from `PLAN.md` §7).
2. **Watchlist changes** (manual or via chat): `await source.add_ticker(t)` / `await source.remove_ticker(t)`.
3. **SSE streaming**: `create_stream_router(cache)` reads `cache.get_all()` on its own 500ms loop, independent of the active source's own update cadence.
4. **Trade execution / portfolio valuation**: read the current price synchronously via `cache.get_price(ticker)` — never touch the data source directly.
5. **App shutdown**: `await source.stop()`.

## 11. Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)     # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", ...])

update = cache.get("AAPL")          # PriceUpdate | None
price = cache.get_price("AAPL")     # float | None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

await source.stop()
```
