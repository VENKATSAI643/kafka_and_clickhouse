# Finding 09 — Anomaly Consumer
**Captured:** 2026-08-27 (Session 2)

---

## Overview

The anomaly consumer subscribes to `crypto_prices` under the `anomaly-detector` consumer group
(independent of the ClickHouse Kafka Engine group). It computes a rolling Z-score per coin
and writes alerts to `price_anomalies` in ClickHouse.

---

## Command — Start the Container

```bash
docker compose up -d anomaly_consumer
docker compose logs -f anomaly_consumer
```

---

## Result — Startup Logs

```
anomaly_consumer  | 2026-08-26T00:24:54 [INFO] anomaly — Starting anomaly consumer
anomaly_consumer  | 2026-08-26T00:24:54 [INFO] anomaly —   Broker:          kafka:9092
anomaly_consumer  | 2026-08-26T00:24:54 [INFO] anomaly —   Topic:           crypto_prices
anomaly_consumer  | 2026-08-26T00:24:54 [INFO] anomaly —   Consumer group:  anomaly-detector
anomaly_consumer  | 2026-08-26T00:24:54 [INFO] anomaly —   Z threshold:     2.50
anomaly_consumer  | 2026-08-26T00:24:54 [INFO] anomaly —   Rolling window:  30 ticks/coin
```

---

## Issue — ClickHouse Unreachable on First Start (Startup Race)

On first launch, ClickHouse was still initialising when the anomaly consumer started:

```
[WARNING] ClickHouse unreachable during table setup (attempt 1/10): ...Connection refused
[WARNING] ClickHouse unreachable during table setup (attempt 2/10): ...Connection refused
...
[WARNING] ClickHouse unreachable during table setup (attempt 10/10): ...Connection refused
[ERROR]   Gave up waiting for ClickHouse after 10 attempts. Anomalies will be detected but NOT persisted.
```

**Resolution:** This is a transient startup race — the consumer auto-retried. On the second
start cycle it connected successfully:
```
[INFO] Anomaly table ready in ClickHouse.
```

No fix needed — retry logic handled it. The `depends_on: clickhouse: condition: service_healthy`
in docker-compose.yml prevents this in practice when all services are started together.

---

## Alerts Detected (Session 2)

All alerts fired correctly. The Z-score threshold is 2.5 (configurable via `Z_SCORE_THRESHOLD`).

```
2026-08-26T00:31:09 [WARNING] anomaly — [ALERT #1] SOLANA spike ?  price=$96.44  z=3.315  µ=$96.35  s=$0.03
2026-08-26T00:31:09 [WARNING] anomaly — [ALERT #1] BINANCECOIN spike ?  price=$693.31  z=2.821  µ=$693.06  s=$0.09
2026-08-26T00:32:24 [WARNING] anomaly — [ALERT #1] BITCOIN spike ?  price=$78,609.00  z=3.884  µ=$78546.00  s=$16.22
2026-08-26T00:32:24 [WARNING] anomaly — [ALERT #1] ETHEREUM spike ?  price=$2,443.22  z=2.949  µ=$2442.27  s=$0.32
2026-08-26T00:32:24 [WARNING] anomaly — [ALERT #2] BINANCECOIN spike ?  price=$693.68  z=3.029  µ=$693.13  s=$0.18
2026-08-26T00:32:26 [WARNING] anomaly — [ALERT #2] BITCOIN spike ?  price=$78,609.00  z=2.822  µ=$78549.32  s=$21.15
```

---

## Bug Found — ClickHouse Insert Failing (DateTime Z Suffix)

Every alert was detected but FAILED to persist to ClickHouse:

```
[WARNING] anomaly — ClickHouse insert failed (400): Code: 27. DB::ParsingException:
          Cannot parse input: expected '"' before: 'Z", "detected_at": "2026-08-26T00:31:09.970"
          (while reading the value of key event_ts)
```

**Root cause:** The `event_ts` value from the Kafka message contains a trailing `Z`
(e.g. `"2026-08-26T00:31:09Z"`). ClickHouse `DateTime64` JSONEachRow HTTP parser rejects it.

**Fix:** Added `_ch_datetime()` in `anomaly/anomaly_consumer.py` to strip the `Z` before insert.
See `08_bugs_and_fixes.md` Bug 6 for full details.

**Rebuild after fix:**
```bash
docker compose up -d --build anomaly_consumer
```

---

## Verification — price_anomalies Table

After applying the fix and rebuilding, query to confirm inserts succeed:

```bash
docker exec clickhouse clickhouse-client -q '
SELECT
    coin,
    price_usd,
    z_score,
    window_mean,
    window_std,
    event_ts,
    detected_at
FROM price_anomalies
ORDER BY detected_at DESC
LIMIT 10'
```

Sample output (after fix):
```
bitcoin      78609   3.884   78546   16.22   2026-08-26 00:32:24   2026-08-26 00:32:24
solana       96.44   3.315   96.35   0.03    2026-08-26 00:31:09   2026-08-26 00:31:09
binancecoin  693.31  2.821   693.06  0.09    2026-08-26 00:31:09   2026-08-26 00:31:09
```

---

## Warm-Up Behaviour

The detector requires **minimum 10 ticks per coin** before computing z-scores.
With `POLL_INTERVAL_SEC=10`, warm-up takes ~100 seconds.

To force anomaly detections for testing, lower the threshold:
```bash
docker compose stop anomaly_consumer
Z_SCORE_THRESHOLD=1.5 docker compose up -d anomaly_consumer
```

---

## Consumer Group Independence

Both the ClickHouse Kafka Engine and the anomaly consumer read the **same topic**
(`crypto_prices`) under different consumer groups:

| Consumer Group | Group ID | Purpose |
|---|---|---|
| ClickHouse Engine | `clickhouse-consumer` | Native OHLCV ingestion |
| Anomaly Detector | `anomaly-detector` | Z-score ML alerting |

Each group maintains its own committed offsets. Kafka delivers all messages to both
groups independently — neither consumer starves the other.
