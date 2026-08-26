# Finding 01 — Container Status
**Captured:** 2026-08-25 22:29 IST

---

## Command

```bash
docker compose ps
```

## Result

```
NAME         IMAGE                               STATUS                    PORTS
clickhouse   clickhouse/clickhouse-server:23.8   Up 12 minutes (healthy)   0.0.0.0:8123->8123/tcp, 0.0.0.0:9000->9000/tcp
kafka        confluentinc/cp-kafka:7.6.0         Up 12 minutes (healthy)   0.0.0.0:29092->29092/tcp
producer     kafkaandclickhouse-producer         Up 8 minutes
zookeeper    confluentinc/cp-zookeeper:7.6.0     Up 12 minutes (healthy)   2181/tcp, 2888/tcp, 3888/tcp
```

## Notes

- clickhouse, kafka, zookeeper — all healthy
- producer — running (no health check defined)
- anomaly_consumer — NOT started (not in compose output)
- kafka-init — one-shot container; never ran on this boot (topic had to be created manually)
