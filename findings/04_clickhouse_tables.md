# Finding 04 — ClickHouse Tables
**Captured:** 2026-08-25 22:29 IST

---

## Command 1 — Show tables

```bash
docker exec clickhouse clickhouse-client -q 'SHOW TABLES'
```

## Result

```
crypto_prices_raw
kafka_raw_prices
mv_1min
mv_5min
mv_kafka_to_raw
prices_1min
prices_5min
```

---

## Command 2 — Table sizes and row counts

```bash
docker exec clickhouse clickhouse-client -q \
  'SELECT name, engine, total_rows, formatReadableSize(total_bytes) AS size
   FROM system.tables WHERE database=currentDatabase() ORDER BY name'
```

## Result

```
crypto_prices_raw    MergeTree              60    2.58 KiB
kafka_raw_prices     Kafka                  \N    \N
mv_1min              MaterializedView       \N    \N
mv_5min              MaterializedView       \N    \N
mv_kafka_to_raw      MaterializedView       \N    \N
prices_1min          AggregatingMergeTree   20    4.62 KiB
prices_5min          AggregatingMergeTree   16    4.45 KiB
```

## Notes

- \N for Kafka engine and MVs is expected — they have no physical storage
- crypto_prices_raw growing in real time (was 20 rows at first check, now 60)
- prices_1min and prices_5min both active with OHLCV data
- price_anomalies table does NOT exist — anomaly_consumer was never started
