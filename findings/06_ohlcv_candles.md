# Finding 06 — OHLCV Candles (prices_1min and prices_5min)
**Captured:** 2026-08-25 22:29 IST

---

## Command 1 — 1-Minute OHLCV candles

```bash
docker exec clickhouse clickhouse-client -q '
SELECT coin, window_start,
       round(open,2)      AS open,
       round(high,2)      AS high,
       round(low,2)       AS low,
       round(close,2)     AS close,
       round(avg_price,2) AS avg_price,
       tick_count
FROM prices_1min
ORDER BY window_start DESC, coin
LIMIT 12'
```

## Result

```
coin           window_start           open      high      low       close     avg_price  tick_count
binancecoin    2026-08-25 16:57:00    698.83    698.83    698.83    698.83    698.83     2
bitcoin        2026-08-25 16:57:00    79108     79108     79108     79108     79108      2
ethereum       2026-08-25 16:57:00    2471.71   2471.71   2471.71   2471.71   2471.71    2
solana         2026-08-25 16:57:00    98.21     98.21     98.21     98.21     98.21      2
binancecoin    2026-08-25 16:56:00    698.76    698.76    698.76    698.76    698.76     1 (+ 3 unmerged)
bitcoin        2026-08-25 16:56:00    79114     79114     79114     79114     79114      1 (+ 3 unmerged)
```

## Notes on 1-min candles

- tick_count = 2 per window because only 2 polls succeeded per minute (rate limiting)
- Open = High = Low = Close (flat candle) — price didn't change within the window
- Unmerged parts show duplicate rows with same window — use FINAL to deduplicate:
  SELECT ... FROM prices_1min FINAL

---

## Command 2 — 5-Minute OHLCV candles

```bash
docker exec clickhouse clickhouse-client -q '
SELECT coin, window_start,
       round(open,2)      AS open,
       round(high,2)      AS high,
       round(low,2)       AS low,
       round(close,2)     AS close,
       round(avg_price,2) AS avg_price,
       tick_count
FROM prices_5min
ORDER BY window_start DESC, coin
LIMIT 8'
```

## Result

```
coin           window_start           open      high      low       close     avg_price  tick_count
binancecoin    2026-08-25 16:55:00    698.83    698.83    698.83    698.83    698.83     2
bitcoin        2026-08-25 16:55:00    79108     79108     79108     79108     79108      2
bitcoin        2026-08-25 16:55:00    79140     79140     79140     79140     79140      2
bitcoin        2026-08-25 16:55:00    79108     79108     79108     79108     79108      2
bitcoin        2026-08-25 16:55:00    79114     79114     79114     79114     79114      1
```

## Notes on 5-min candles

- Multiple unmerged parts visible in same 16:55:00 window (AggregatingMergeTree merges in background)
- BTC shows price variation: $79,108 → $79,114 → $79,140 across parts — this is real price movement
- Query with FINAL to get deduplicated/merged result:
  SELECT ... FROM prices_5min FINAL ORDER BY window_start DESC LIMIT 8
