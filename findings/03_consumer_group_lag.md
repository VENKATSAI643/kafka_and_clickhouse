# Finding 03 — Kafka Consumer Group Lag
**Captured:** 2026-08-25 22:29 IST

---

## Command

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group clickhouse-consumer --describe
```

## Result

```
GROUP               TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
clickhouse-consumer crypto_prices   0          32              32              0
clickhouse-consumer crypto_prices   1          -               0               -
clickhouse-consumer crypto_prices   2          64              64              0
clickhouse-consumer crypto_prices   3          32              32              0
```

## Notes

- LAG = 0 on all active partitions — ClickHouse is fully caught up in real time
- Partition 1 shows "-" offset because bitcoin messages go to partition 2 via key hashing;
  partition 1 has received no messages (all 4 coins hash to partitions 0, 2, 3)
- Total messages consumed across partitions: 32 + 64 + 32 = 128
- 2 ClickHouse background consumer threads (kafka_num_consumers=2), each handling 2 partitions
