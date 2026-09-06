# Massive API Reference (formerly Polygon.io)

Research reference for the Massive REST API, as used by `backend/app/market/massive_client.py` to retrieve real-time and end-of-day stock prices for FinAlly's watchlist. Verified against the official `massive-com/client-python` SDK source (v2.2.0, the version pinned in `backend/pyproject.toml`) and the current [massive.com/docs](https://massive.com/docs) REST reference in September 2026.

## 1. Background

Polygon.io rebranded to **Massive** on 2025-10-30. Practically, for this project:

- The PyPI package renamed from `polygon-api-client` to **`massive`**.
- The SDK's default base URL changed from `api.polygon.io` to `api.massive.com`; the old host is still supported.
- Existing Polygon.io API keys continue to work unchanged.
- Import paths changed from `polygon.*` to `massive.*`. Everything else (client shape, method names, response models) is materially the same as the old Polygon.io client — the earlier ecosystem's docs/blog posts/Stack Overflow answers are still a useful reference if you mentally rename `polygon` → `massive`.

## 2. Installation & Setup

```bash
# uv (this project)
uv add massive

# pip
pip install -U massive
```

- Requires Python 3.9+.
- Single dependency; no extras needed for the REST client used here (`massive` also ships a WebSocket client, which FinAlly does not use — see [§8](#8-why-rest-polling-not-websockets)).

## 3. Authentication

```python
from massive import RESTClient

# Reads the MASSIVE_API_KEY environment variable automatically
client = RESTClient()

# Or pass the key explicitly
client = RESTClient(api_key="your_key_here")
```

`RESTClient()` with no arguments looks for `MASSIVE_API_KEY` in the environment — this is exactly the env var name FinAlly already uses (see `PLAN.md` §5), so no adapter layer is needed between the project's config and the SDK's expectations.

## 4. Rate Limits

| Tier | Limit |
|------|-------|
| Free | 5 requests/minute |
| Paid (all tiers) | Unlimited request *rate* (fair-use applies) |

Source: [Massive REST FAQs](https://massive.com/knowledge-base/categories/rest). For FinAlly's polling design: free tier → poll every 15s; paid tiers can safely poll every 2–5s (`PLAN.md` §6 already specifies this).

Free/Starter/Developer snapshot data is also **~15-minutes delayed**; only Advanced/Business plans get real-time snapshots. This matters for user expectations if someone runs FinAlly with a free-tier key: prices will look "live" (updating every poll) but reflect quotes from 15 minutes ago.

## 5. Endpoints Used by FinAlly

### 5.1 Snapshot — All Tickers (primary polling endpoint)

Returns current-session data for one call covering **multiple tickers at once** — the reason this is the endpoint the poller uses instead of per-ticker calls.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT`

**Python client**:
```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType, TickerSnapshot

client = RESTClient()

# Positional form, straight from the official SDK examples:
snapshots = client.get_snapshot_all("stocks", ["AAPL", "GOOGL", "MSFT"])

# Equivalent keyword form (what massive_client.py uses):
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,   # or the plain string "stocks"
    tickers=["AAPL", "GOOGL", "MSFT"],
)

for snap in snapshots:
    if isinstance(snap, TickerSnapshot):
        print(f"{snap.ticker}: last trade ${snap.last_trade.price}")
        print(f"  today's change: {snap.todays_change_percent:.2f}%")
        print(f"  prev close: ${snap.prev_day.close}")
```

**Signature** (from `massive/rest/snapshot.py`):
```python
def get_snapshot_all(
    self,
    market_type: Union[str, SnapshotMarketType],
    tickers: Optional[Union[str, List[str]]] = None,
    include_otc: Optional[bool] = False,
    ...
) -> Union[List[TickerSnapshot], HTTPResponse]
```
- `market_type` accepts either the bare string `"stocks"` or the `SnapshotMarketType.STOCKS` enum member — both work.
- `tickers` omitted or empty → snapshots for **every** ticker in that market (thousands of rows) — always pass an explicit list.
- Snapshot data resets at 12am ET and repopulates as exchanges send data (as early as 4am ET); outside that window a snapshot can be `None`/stale.

**`TickerSnapshot` fields actually present on the model** (`massive/rest/models/snapshot.py`) — this is the part earlier drafts of this document got wrong, so it's worth being explicit:

| Field | Type | Notes |
|---|---|---|
| `ticker` | `str` | e.g. `"AAPL"` |
| `day` | `Agg \| None` | **current** session's OHLCV bar |
| `prev_day` | `Agg \| None` | **previous** session's OHLCV bar — there is no `day.previous_close`; previous close lives here, as `prev_day.close` |
| `last_trade` | `LastTrade \| None` | most recent trade |
| `last_quote` | `LastQuote \| None` | most recent NBBO quote |
| `min` | `MinuteSnapshot \| None` | most recent minute bar |
| `todays_change` | `float \| None` | dollar change since prior close |
| `todays_change_percent` | `float \| None` | percent change since prior close — use this directly instead of computing it from `day`/`prev_day` |
| `updated` | `int \| None` | last update, Unix **nanoseconds** |

`Agg` (used for both `day` and `prev_day`): `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp`, `transactions`, `otc`.

`LastTrade`: `ticker`, `price`, `size`, `exchange`, `conditions`, `sip_timestamp`, `participant_timestamp`, `trf_timestamp`, `sequence_number`, `id`, `correction`, `trf_id`, `fractional_size`. **There is no plain `.timestamp` attribute** — see [§9](#9-known-gotchas).

`LastQuote`: `ticker`, `bid_price`, `bid_size`, `bid_exchange`, `ask_price`, `ask_size`, `ask_exchange`, `sip_timestamp`, `participant_timestamp`, `trf_timestamp`, `sequence_number`, `conditions`, `indicators`, `tape`. Field names are `bid_price`/`ask_price`, not `bid`/`ask`.

All `*_timestamp` fields on trades/quotes are **Unix nanoseconds** (SIP timestamps), not milliseconds. `Agg.timestamp` and `TickerSnapshot.updated` are documented inconsistently across Polygon/Massive's own docs (some say ms) — treat any timestamp field as untrusted and prefer to stamp `PriceUpdate.timestamp` with `time.time()` at receipt if the API's own value isn't confirmed for the field you're reading (this is in fact what `massive_client.py`'s poll loop's own wall-clock arrival time would give you instead of trusting `last_trade`'s timestamp — see §9).

### 5.2 Single-Ticker Snapshot

Useful for a detail view on one ticker (e.g., the "click a ticker for the big chart" interaction in `PLAN.md` §2), though FinAlly's poller currently always uses the all-tickers form since it needs the whole watchlist anyway.

```python
snapshot = client.get_snapshot_ticker(
    market_type=SnapshotMarketType.STOCKS,
    ticker="AAPL",
)
print(f"Price: ${snapshot.last_trade.price}")
print(f"Bid/Ask: ${snapshot.last_quote.bid_price} / ${snapshot.last_quote.ask_price}")
print(f"Day range: ${snapshot.day.low} - ${snapshot.day.high}")
```

Same `TickerSnapshot` model as §5.1, just for one ticker. **REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}`.

### 5.3 Previous Close (end-of-day)

Gets the prior trading day's OHLCV for one ticker. Useful for seeding realistic simulator starting prices, or for a "vs. yesterday's close" comparison independent of the snapshot's own `prev_day` field.

**REST**: `GET /v2/aggs/ticker/{ticker}/prev`

```python
prev = client.get_previous_close_agg("AAPL")   # returns ONE object, not a list

print(f"Previous close: ${prev.close}")
print(f"OHLC: O={prev.open} H={prev.high} L={prev.low} C={prev.close}")
print(f"Volume: {prev.volume}")
```

`get_previous_close_agg(ticker, adjusted=None, ...)` returns a single `PreviousCloseAgg` (`ticker`, `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp`) — **not** an iterable of results, despite the raw JSON response wrapping it in a `results: [...]` array. The SDK unwraps this for you.

### 5.4 Aggregates (Bars) — historical OHLCV

Not needed for live polling, but the natural endpoint if/when FinAlly adds historical charting beyond what accumulates client-side from the SSE stream.

**REST**: `GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`

```python
aggs = []
for a in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",          # "minute", "hour", "day", "week", "month", "quarter", "year"
    from_="2024-01-01",       # note the trailing underscore — "from" is a Python keyword
    to="2024-01-31",
    limit=50000,
):
    aggs.append(a)

for a in aggs:
    print(f"t={a.timestamp} O={a.open} H={a.high} L={a.low} C={a.close} V={a.volume}")
```

`list_aggs(...)` returns an `Iterator[Agg]` and paginates automatically.

### 5.5 Last Trade / Last Quote (standalone)

Individual endpoints if you only need the single most-recent trade or quote rather than a full snapshot.

```python
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} x {trade.size}")

quote = client.get_last_quote(ticker="AAPL")
print(f"Bid: ${quote.bid_price} x {quote.bid_size}")
print(f"Ask: ${quote.ask_price} x {quote.ask_size}")
```

Not used by FinAlly today — the all-tickers snapshot already returns `last_trade`/`last_quote` per ticker in one call, which is strictly better for the polling use case.

## 6. How FinAlly Uses This API

`MassiveDataSource` (`backend/app/market/massive_client.py`) runs as a background `asyncio` task:

1. On `start(tickers)`, builds a synchronous `RESTClient(api_key=...)`, does one immediate poll so the price cache isn't empty on first render, then schedules a poll loop.
2. Each poll: calls `get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=self._tickers)` for the full current watchlist in a single request (via `asyncio.to_thread`, since the SDK call is blocking).
3. For each returned snapshot, extracts `last_trade.price` and writes it into the shared `PriceCache` (see `MARKET_INTERFACE.md`).
4. Sleeps for `poll_interval` seconds (default 15s), then repeats.
5. A malformed/missing snapshot for one ticker is logged and skipped rather than crashing the whole poll cycle; an API-level exception (network error, 429, etc.) is caught, logged, and retried on the next interval rather than propagated.

```python
# Simplified shape of the real implementation
async def _poll_once(self) -> None:
    if not self._tickers or not self._client:
        return
    try:
        snapshots = await asyncio.to_thread(
            self._client.get_snapshot_all,
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
        for snap in snapshots:
            self._cache.update(ticker=snap.ticker, price=snap.last_trade.price)
    except Exception as e:
        logger.error("Massive poll failed: %s", e)  # retried next interval, not re-raised
```

## 7. Error Handling

The client raises exceptions for HTTP errors:

| Status | Meaning |
|---|---|
| 401 | Invalid API key |
| 403 | Plan doesn't include this endpoint |
| 429 | Rate limit exceeded (free tier: 5 req/min) |
| 5xx | Server error — the SDK retries a few times internally before raising |

FinAlly's poller treats every exception the same way: log and let the next scheduled poll try again. There's no dedicated backoff for 429s beyond the fixed `poll_interval` — on the free tier, setting `poll_interval` below ~12–15s risks tripping the rate limit repeatedly.

## 8. Why REST Polling, Not WebSockets

`massive` also ships a `WebSocketClient` for streaming trades/quotes. FinAlly deliberately doesn't use it (`PLAN.md` §6):

- REST polling works on every plan tier, including free; the WebSocket feed for stocks generally requires a paid real-time plan.
- A single `get_snapshot_all()` call already covers the entire watchlist, so polling doesn't multiply API usage per ticker.
- FinAlly's own SSE endpoint (`/api/stream/prices`) is the thing the frontend subscribes to — Massive's transport (REST vs. WebSocket) is fully hidden behind `PriceCache`, so switching to the WebSocket client later would be a change contained entirely inside `massive_client.py`.

## 9. Known Gotchas

These are worth flagging explicitly since they affect correctness of a real (non-simulated) run:

- **`last_trade.timestamp` does not exist on the real SDK model.** `LastTrade` only has `sip_timestamp` / `participant_timestamp` / `trf_timestamp` (see §5.1). The current `massive_client.py` reads `snap.last_trade.timestamp`, which will raise `AttributeError` against a real `massive` response — caught by its existing `except (AttributeError, TypeError)` guard, so the poller won't crash, but it means **every snapshot is silently skipped** and the cache never updates when running against the real API. The unit tests don't catch this because they mock the snapshot object with `MagicMock()`, which happily fabricates a `.timestamp` attribute that doesn't exist on the real class. Fix is to use `snap.last_trade.sip_timestamp / 1_000_000_000` (nanoseconds → seconds) or simply omit the timestamp argument and let `PriceCache.update()` default to `time.time()`.
- **`prev_day`, not `day.previous_close`.** Earlier design notes for this project referenced `day.previous_close` and `day.change_percent`; neither exists. Previous close is `snap.prev_day.close`; day change is `snap.todays_change_percent`.
- **`bid_price`/`ask_price`, not `bid`/`ask`**, on `LastQuote`.
- **`get_previous_close_agg` returns a single object**, not an iterable — don't loop over it.
- **Snapshot values can be delayed 15 minutes** on Free/Starter/Developer plans; don't assume "polled just now" means "real-time."

## Sources

- [massive-com/client-python — README](https://github.com/massive-com/client-python/blob/master/README.md)
- [massive-com/client-python — repository](https://github.com/massive-com/client-python)
- [massive-com/client-python — source: `massive/rest/snapshot.py`, `massive/rest/aggs.py`, `massive/rest/models/{snapshot,aggs,trades,quotes,common}.py`](https://github.com/massive-com/client-python)
- [Full Market Snapshot | Stocks REST API — Massive](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot)
- [Previous Day Bar (OHLC) — Massive](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar)
- [What is the request limit for Massive's RESTful APIs? — Massive Knowledge Base](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis)
- [Polygon.io is Now Massive — Massive blog](https://massive.com/blog/polygon-is-now-massive)
- [Massive + Python: Unlocking Real-Time and Historical Stock Market Data — Massive blog](https://massive.com/blog/polygon-io-with-python-for-stock-market-data)
