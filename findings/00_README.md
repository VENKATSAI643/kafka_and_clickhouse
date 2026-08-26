# Pipeline Findings — Kafka to ClickHouse
**Session:** 2026-08-25 → 2026-08-26, last updated 05:37 IST

This folder contains the commands run and their live output during a full pipeline health-check session.

---

## Files

| File | What it covers |
|---|---|
| 01_container_status.md  | docker compose ps — which containers are up |
| 02_kafka_topic.md       | kafka-topics list and describe |
| 03_consumer_group_lag.md| kafka-consumer-groups lag check |
| 04_clickhouse_tables.md | SHOW TABLES + row counts and sizes |
| 05_live_prices.md       | crypto_prices_raw row stats, latest rows, lag |
| 06_ohlcv_candles.md     | prices_1min and prices_5min OHLCV data |
| 07_producer_logs.md     | Producer container logs (last 10 lines) |
| 08_bugs_and_fixes.md    | 5 bugs found, root causes, fixes applied, remaining issues |

---

## Pipeline Status at Capture Time

```
LIVE — data flowing end to end
  BTC  $79,108  |  ETH  $2,471.71  |  SOL  $98.21  |  BNB  $698.83
  crypto_prices_raw: 60 rows  |  prices_1min: 20 rows  |  prices_5min: 16 rows
  Kafka consumer LAG: 0 on all partitions
```

---

## Key Bug Found

ClickHouse was silently dropping every message because:
- Producer emits ISO 8601: "2026-08-25T16:50:10Z"
- ClickHouse DateTime (JSONEachRow) expects: "2026-08-25 16:50:10"
- kafka_skip_broken_messages=10 hid the error completely

Fix: Store as String in Kafka engine table, cast via parseDateTime64BestEffort() in the MV.
See 08_bugs_and_fixes.md for full details.
