# Finding 02 — Kafka Topic
**Captured:** 2026-08-25 22:29 IST

---

## Command 1 — List topics

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

## Result

```
__consumer_offsets
crypto_prices
```

---

## Command 2 — Describe topic

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic crypto_prices
```

## Result

```
Topic: crypto_prices    TopicId: RM66_VyeSGGRXv_qWXYPsQ    PartitionCount: 4    ReplicationFactor: 1
Configs: cleanup.policy=delete, segment.bytes=1073741824, retention.ms=86400000

  Topic: crypto_prices    Partition: 0    Leader: 1    Replicas: 1    Isr: 1
  Topic: crypto_prices    Partition: 1    Leader: 1    Replicas: 1    Isr: 1
  Topic: crypto_prices    Partition: 2    Leader: 1    Replicas: 1    Isr: 1
  Topic: crypto_prices    Partition: 3    Leader: 1    Replicas: 1    Isr: 1
```

## Notes

- 4 partitions, replication factor 1 (single broker, dev setup)
- Retention: 86400000 ms = 24 hours
- Cleanup policy: delete (not compacted — correct for time-series)
- Topic was created manually after kafka-init container failed to run
