# Finding 05 — Live Price Data (crypto_prices_raw)
**Captured:** 2026-08-25 22:29 IST

---

## Command 1 — Row count and price stats per coin

```bash
docker exec clickhouse clickhouse-client -q '
SELECT coin,
       count()             AS rows,
       round(min(price_usd),2) AS min_price,
       round(max(price_usd),2) AS max_price,
       round(avg(price_usd),2) AS avg_price,
       max(event_ts)           AS last_seen
FROM crypto_prices_raw
GROUP BY coin
ORDER BY coin'
```

## Result

```
binancecoin    15    698.76    698.83    698.8      2026-08-25 16:57:40
bitcoin        15    79108     79140     79121.07   2026-08-25 16:57:40
ethereum       15    2471.48   2472.41   2471.85    2026-08-25 16:57:40
solana         15    98.21     98.3      98.27      2026-08-25 16:57:40
```

---

## Command 2 — Data freshness lag

```bash
docker exec clickhouse clickhouse-client -q '
SELECT coin, now()-max(event_ts) AS lag_sec
FROM crypto_prices_raw
GROUP BY coin
ORDER BY coin'
```

## Result

```
binancecoin    122
bitcoin        122
ethereum       122
solana         122
```

---

## Command 3 — Latest 8 raw rows

```bash
docker exec clickhouse clickhouse-client -q '
SELECT * FROM crypto_prices_raw ORDER BY event_ts DESC LIMIT 8'
```

## Result

```
coin          price_usd   change_24h                   event_ts               ingested_at
binancecoin   698.83      -0.9516438818986136           2026-08-25 16:57:40    2026-08-25 16:59:13.606
binancecoin   698.83      -0.9516438818986136           2026-08-25 16:57:40    2026-08-25 16:59:17.618
bitcoin       79108       0.0052886678374937995         2026-08-25 16:57:40    2026-08-25 16:59:13.606
bitcoin       79108       0.0052886678374937995         2026-08-25 16:57:40    2026-08-25 16:59:17.618
ethereum      2471.71     -0.2788419650085574           2026-08-25 16:57:40    2026-08-25 16:59:13.606
ethereum      2471.71     -0.2788419650085574           2026-08-25 16:57:40    2026-08-25 16:59:17.618
solana        98.21       2.146608065196787             2026-08-25 16:57:40    2026-08-25 16:59:13.606
solana        98.21       2.146608065196787             2026-08-25 16:57:40    2026-08-25 16:59:17.618
```

## Notes

- ~122 seconds of data lag due to CoinGecko rate limiting (HTTP 429)
- Duplicate rows (same event_ts, different ingested_at) are expected:
  the producer re-sends the same snapshot after rate-limit backoff
- Solana 24h change +2.15% is the highest mover in this window
- Bitcoin is up just +0.005% — essentially flat
