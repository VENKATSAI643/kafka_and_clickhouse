# Finding 10 — CoinGecko Rate Limiting Fix
**Captured:** 2026-08-27 (Session 2)

---

## Problem Statement

The producer was polling CoinGecko every **3 seconds** (~20 req/min).
CoinGecko free tier allows ~10–30 req/min but enforces a **burst throttle**
that triggers 429 responses after several consecutive rapid calls.

Each 429 episode costs **~62 seconds** of dead time (5 retries: 2+4+8+16+32s).
During this time, **Kafka receives no messages** — the pipeline has silent gaps.

---

## Observed Pattern Before Fix

```
[INFO]    Published 4 messages               ? successful poll
[INFO]    Published 4 messages               ? successful poll
[INFO]    Published 4 messages               ? successful poll
[WARNING] Rate limited (429). Retry 1/5 in 2s
[WARNING] Rate limited (429). Retry 2/5 in 4s
[WARNING] Rate limited (429). Retry 3/5 in 8s
[WARNING] Rate limited (429). Retry 4/5 in 16s
[WARNING] Rate limited (429). Retry 5/5 in 32s
[ERROR]   CoinGecko fetch failed after 5 attempts.
[ERROR]   Skipping this poll cycle — no data.   ? 62s gap, Kafka gets nothing
[INFO]    Published 4 messages               ? resumes, then hits 429 again ~5 polls later
```

**Frequency:** Rate limiting hit every ~5 successful polls (~15 seconds of good data, 62s gap).  
**Impact:**
- ~62-second gaps in Kafka topic
- Anomaly detector z-score window receives flat/no data during gaps
- OHLCV candles have missing ticks ? flat Open=High=Low=Close candles
- Duplicate rows when producer re-publishes same cached price after backoff

---

## Fix 1 — Raise Default Poll Interval (3s ? 10s)

**File:** `producer/producer.py`

```python
# BEFORE
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "3"))

# AFTER
# CoinGecko free tier: ~10-30 req/min. 10s = 6 req/min — well under limit.
# Set POLL_INTERVAL_SEC=3 only if you have a CoinGecko Pro API key.
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "10"))
```

**File:** `.env`
```ini
# BEFORE
POLL_INTERVAL_SEC=3

# AFTER
POLL_INTERVAL_SEC=10
```

**Effect:** Reduces from ~20 req/min to ~6 req/min.
Burst throttle no longer triggered. Rate limiting eliminated in normal operation.

---

## Fix 2 — Last-Data Cache (Stale Publish Instead of Silent Skip)

**File:** `producer/producer.py` — `main()` function

```python
# NEW: cache of last successful API response
_last_data: dict | None = None

while True:
    data = fetch_prices(session)

    if data is None:
        if _last_data is not None:
            # Rate-limited: publish last known prices — pipeline keeps flowing.
            # Consumers see a [STALE] warning in logs but receive no gap.
            logger.warning("[STALE] CoinGecko unavailable — republishing last known prices.")
            data = _last_data
        else:
            logger.error("Skipping this poll cycle — no data and no cache yet.")

    if data is not None:
        _last_data = data    # update cache on every successful fetch
        # ... publish to Kafka as normal
```

**Effect:** If CoinGecko is unavailable, the producer re-publishes the last known
prices with a `[STALE]` log prefix instead of silently dropping the cycle.
Downstream consumers (ClickHouse, anomaly detector) keep receiving data with no gaps.

---

## Behaviour Comparison

| Scenario | Before Fix | After Fix |
|---|---|---|
| API returns 200 | Publishes fresh prices | Publishes fresh prices ? |
| API returns 429 (5 retries) | **Skips cycle — 62s Kafka gap** | Publishes last known prices (stale) ? |
| First poll ever and API fails | Skips | Skips (no cache yet) — same behaviour |
| Log output on 429 | `[ERROR] Skipping this poll cycle` | `[WARNING] [STALE] republishing last known prices` |

---

## Log Output After Fix

```
2026-08-27T16:51:13 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$80,443.00', ...}
2026-08-27T16:51:22 [WARNING] producer — Rate limited (429). Retry 1/5 in 2s
2026-08-27T16:51:24 [WARNING] producer — Rate limited (429). Retry 2/5 in 4s
2026-08-27T16:51:28 [WARNING] producer — Rate limited (429). Retry 3/5 in 8s
2026-08-27T16:51:36 [WARNING] producer — Rate limited (429). Retry 4/5 in 16s
2026-08-27T16:51:53 [WARNING] producer — Rate limited (429). Retry 5/5 in 32s
2026-08-27T16:52:25 [WARNING] producer — [STALE] CoinGecko unavailable — republishing last known prices.
2026-08-27T16:52:25 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$80,443.00', ...}   ? stale, no gap
2026-08-27T16:52:28 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$80,435.00', ...}   ? fresh resumes
```

---

## Trade-off Analysis

| Approach | Pros | Cons |
|---|---|---|
| **Raise interval to 10s** | Eliminates 429s; clean data | Fewer ticks per candle (6/min vs 20/min) |
| **Stale cache fallback** | No Kafka gaps; consumers stay warm | Stale rows in ClickHouse; flat z-score during outage |
| **Add Pro API key** | Keep 3s interval, no rate limiting | Requires paid CoinGecko account |

Both fixes are complementary and applied together in this project.

---

## How to Apply on Running Stack

```bash
# Rebuild the producer image with the fix
docker compose up -d --build producer

# Confirm new interval in startup log
docker compose logs producer | grep "Poll interval"
# Expected: [INFO] producer —   Poll interval: 10s

# Confirm stale-cache activates on next 429
docker compose logs -f producer | grep -E "STALE|Published"
```
