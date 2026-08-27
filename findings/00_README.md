# Pipeline Findings — Kafka to ClickHouse
**Session 1:** 2026-08-25 ? 2026-08-26 (initial run, bug discovery)  
**Session 2:** 2026-08-27 (anomaly consumer, rate-limit fix, README overhaul)  
**Last updated:** 2026-08-27 22:30 IST

This folder contains commands run and their live output across both pipeline sessions.

---

## Files

| File | What it covers |
|---|---|
| 01_container_status.md   | docker compose ps — which containers are up |
| 02_kafka_topic.md        | kafka-topics list and describe |
| 03_consumer_group_lag.md | kafka-consumer-groups lag check |
| 04_clickhouse_tables.md  | SHOW TABLES + row counts and sizes |
| 05_live_prices.md        | crypto_prices_raw row stats, latest rows, lag |
| 06_ohlcv_candles.md      | prices_1min and prices_5min OHLCV data |
| 07_producer_logs.md      | Producer container logs (Session 1 + Session 2) |
| 08_bugs_and_fixes.md     | All bugs found, root causes, fixes applied |
| 09_anomaly_consumer.md   | Anomaly consumer startup, alerts, ClickHouse inserts |
| 10_rate_limiting_fix.md  | CoinGecko 429 fix — stale-cache + poll interval change |

---

## Pipeline Status — Session 2 (2026-08-27)

```
LIVE — all 5 services running
  BTC  $80,521  |  ETH  $2,528.70  |  SOL  $108.77  |  BNB  $712.86
  Poll #40+ — 40+ messages per coin published
  anomaly_consumer: RUNNING — alerts detected and persisting to ClickHouse
  Kafka consumer LAG: 0 on all partitions
```

---

## Pipeline Status — Session 1 (2026-08-26 05:37 IST)

```
LIVE — data flowing end to end
  BTC  $79,108  |  ETH  $2,471.71  |  SOL  $98.21  |  BNB  $698.83
  crypto_prices_raw: 60 rows  |  prices_1min: 20 rows  |  prices_5min: 16 rows
  Kafka consumer LAG: 0 on all partitions
```

---

## All Bugs Found (chronological)

| # | Bug | Session | Status |
|---|---|---|---|
| 1 | kafka-init never ran — topic missing | Session 1 | ? Fixed |
| 2 | ClickHouse silently dropping all messages (DateTime Z suffix) | Session 1 | ? Fixed |
| 3 | IF NOT EXISTS migrations no-op on schema changes | Session 1 | ? Documented |
| 4 | Producer exit 127 — WSL bind-mount breaks after Docker Desktop restart | Session 1 | ? Fixed |
| 5 | Pipeline does not auto-recover after host restart | Session 1 | ? Fixed |
| 6 | anomaly_consumer ClickHouse insert fails — DateTime Z suffix (HTTP insert) | Session 2 | ? Fixed |
| 7 | CoinGecko 429 rate limiting — 62s gaps every ~5 polls | Session 2 | ? Fixed |
