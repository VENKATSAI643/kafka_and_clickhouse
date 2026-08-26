# Finding 08 — Bugs Found and Fixed
**Last Updated:** 2026-08-26 05:37 IST  
**Pipeline Status:** ⚠️ PAUSED — producer exited, data frozen since 2026-08-25 17:01 IST

---

## Bug 1 — Missing Kafka Topic (kafka-init never ran)

### Symptom

Producer logs showed:
```
[ERROR] producer — Delivery failed: topic=crypto_prices key=b'bitcoin'
        error=KafkaError{code=UNKNOWN_TOPIC_OR_PART,val=3,str="Broker: Unknown topic or partition"}
[ERROR] producer — KafkaException: KafkaError{code=_UNKNOWN_TOPIC,val=-188,
        str="Unable to produce message: Local: Unknown topic"}
```

### Root Cause

The `kafka-init` one-shot container (restart: "no") had already exited on a prior run.
On `docker compose up`, it does not re-run. On the most recent boot, the topic was never created.

### Fix Applied

```bash
docker exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic crypto_prices \
  --partitions 4 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete
```

Result:
```
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could
collide. To avoid issues it is best to use either, but not both.
Created topic crypto_prices.
```

---

## Bug 2 — ClickHouse Silently Dropping All Messages (DateTime Format Mismatch)

### Symptom

- Kafka consumer group showed LAG=0 (messages consumed)
- `crypto_prices_raw` stayed empty
- `system.kafka_consumers` showed no exceptions
- `kafka_skip_broken_messages=10` silently discarded every message

### Root Cause

Producer emits ISO 8601 timestamps:
```json
{
  "event_ts":    "2026-08-25T16:50:10Z",
  "ingested_at": "2026-08-25T16:51:26.739Z"
}
```

ClickHouse `DateTime` with `JSONEachRow` only accepts `"YYYY-MM-DD HH:MM:SS"` format.
The `T` separator and `Z` suffix cause a silent parse failure for every single message.

### Confirmed via kafka-console-consumer

```bash
timeout 5 docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic crypto_prices \
  --from-beginning --max-messages 2
```

Result:
```
{"coin": "binancecoin", "price_usd": 699.69, "change_24h": -1.0423930803342611,
 "event_ts": "2026-08-25T16:50:10Z", "ingested_at": "2026-08-25T16:51:26.739Z"}
{"coin": "binancecoin", "price_usd": 699.69, "change_24h": -1.0423930803342611,
 "event_ts": "2026-08-25T16:50:10Z", "ingested_at": "2026-08-25T16:51:26.739Z"}
Processed a total of 2 messages
```

### Fix Applied

Step 1 — Drop old broken tables:
```bash
docker exec clickhouse clickhouse-client -q 'DROP TABLE IF EXISTS mv_kafka_to_raw'
docker exec clickhouse clickhouse-client -q 'DROP TABLE IF EXISTS kafka_raw_prices'
```

Step 2 — Updated clickhouse/migrations/01_kafka_engine.sql
Changed event_ts and ingested_at from DateTime to String:

BEFORE:
```sql
event_ts    DateTime,
ingested_at DateTime64(3)
```

AFTER:
```sql
event_ts    String,
ingested_at String
```

Step 3 — Updated clickhouse/migrations/03_mv_kafka_to_raw.sql
Added ISO 8601 cast in the Materialized View:

BEFORE:
```sql
SELECT coin, price_usd, change_24h, event_ts, ingested_at
FROM kafka_raw_prices;
```

AFTER:
```sql
SELECT
    coin,
    price_usd,
    change_24h,
    toDateTime(parseDateTime64BestEffort(event_ts))    AS event_ts,
    parseDateTime64BestEffort(ingested_at, 3)          AS ingested_at
FROM kafka_raw_prices;
```

Step 4 — Re-applied fixed migrations:
```bash
cd '/mnt/e/Personal/Kafka and ClickHouse'
docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/01_kafka_engine.sql
docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/03_mv_kafka_to_raw.sql
```

Result: Data started flowing immediately after.

---

## Bug 3 — IF NOT EXISTS Migrations No-Op on Schema Changes

### Symptom

Running the fixed migrations did nothing because tables already existed with the wrong schema.

### Root Cause

All migrations use `CREATE TABLE IF NOT EXISTS` — they silently skip if the table exists,
even if the schema has changed.

### Fix Applied

Always manually DROP the affected table before re-running its migration:
```bash
docker exec clickhouse clickhouse-client -q 'DROP TABLE IF EXISTS <table_name>'
```

This is safe for:
- `kafka_raw_prices` (Kafka Engine — virtual, no data stored)
- `mv_kafka_to_raw` (MaterializedView — no data stored, triggers only)

WARNING: Do NOT drop crypto_prices_raw, prices_1min, or prices_5min without backing up data.

---

## Current System State (2026-08-26 05:37 IST)

| Component | Status | Detail |
|---|---|---|
| `zookeeper` | ✅ Up (healthy) | Running fine |
| `kafka` | ✅ Up (healthy) | Topic `crypto_prices` exists, 4 partitions |
| `clickhouse` | ✅ Up (healthy) | All 7 tables present, data frozen at 17:01 |
| `kafka-init` | ✅ Exited (0) | Topic already created, correct |
| `producer` | ❌ Exited (127) | **NEW BUG — see Bug 4 below** |
| `anomaly_consumer` | ❌ Not running | Never started |
| `manual_consumer` | ❌ Not running | Never started |

### ClickHouse Data (as of check)

| Coin | Rows | Latest event_ts |
|---|---|---|
| binancecoin | 29 | 2026-08-25 17:01:10 |
| bitcoin | 29 | 2026-08-25 17:01:40 |
| ethereum | 29 | 2026-08-25 17:01:40 |
| solana | 29 | 2026-08-25 17:01:40 |

### Kafka Consumer Group Lag

| Partition | Lag | Consumer |
|---|---|---|
| 0 | 0 | clickhouse-consumer |
| 1 | – (empty) | clickhouse-consumer |
| 2 | 0 | clickhouse-consumer |
| 3 | 0 | clickhouse-consumer |

ClickHouse has consumed **all available messages** (LAG=0). The pipeline is healthy — it just needs the producer restarted.

---

## Bug 4 — Producer Fails to Start After Docker Desktop Restart (Exit 127, WSL Bind-Mount)

### Symptom

After Docker Desktop restarted (following a server/sleep cycle), the `producer` container exited immediately with code 127:

```
127 failed to create task for container: failed to create shim task: OCI runtime create failed:
runc create failed: unable to start container process: error during container init:
error mounting "/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/Ubuntu-26.04/..."
to rootfs at "/app/failed_messages.jsonl": mount src=..., dst=/app/failed_messages.jsonl:
not a directory: Are you trying to mount a directory onto a file (or vice-versa)?
Check if the specified host path exists and is the expected type
```

### Root Cause

The `producer` service in `docker-compose.yml` has a volume bind-mount for the dead-letter log file:
```yaml
volumes:
  - ./producer/failed_messages.jsonl:/app/failed_messages.jsonl
```

After Docker Desktop restarts, the WSL bind-mount path (`/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/...`) is
stale or recreated as a directory instead of a file. Docker cannot mount a file onto a path that
resolved to a directory.

Exit code 127 = the container process never started (OCI runtime failure, not a Python error).

### Fix Applied

**Option A (permanent) — remove the bind-mount from `docker-compose.yml`** and let the container write the file internally:

In `docker-compose.yml`, remove the volume entry for `failed_messages.jsonl` under the `producer` service.
The file is already created inside the image by `RUN touch /app/failed_messages.jsonl` in the Dockerfile.

**Option B (quick workaround) — pre-create the file on WSL before starting:**
```bash
touch "$(pwd)/producer/failed_messages.jsonl"
docker compose up -d producer
```

---

## Bug 5 — Data Frozen at Last Run; Pipeline Does Not Auto-Recover After Restart

### Symptom

ClickHouse `crypto_prices_raw` shows data frozen at `2026-08-25 17:01` — roughly 12 hours stale.
Kafka has LAG=0, meaning the producer stopped sending and all previously sent messages were consumed.

### Root Cause

`producer` has `restart: unless-stopped` in `docker-compose.yml`, but the container fails on startup
(Bug 4) before it can restart. The restarter gives up after the OCI error.

### Fix

Fix Bug 4 first (remove the bind-mount volume). Then restart the producer:

```bash
docker compose up -d producer
docker compose logs -f producer
```

---

## Remaining Issues (Updated)

| # | Issue | Impact | Fix |
|---|---|---|---|
| 1 | CoinGecko 429 rate limiting | ~120s data lag; single-tick candles | Add API key, or raise `POLL_INTERVAL_SEC` to 60 |
| 2 | `anomaly_consumer` not running | No anomaly detection | `docker compose up -d anomaly_consumer` (after fixing Bug 4) |
| 3 | `prices_1min`/`prices_5min` unmerged parts | Duplicate rows in OHLCV | Use `FINAL` keyword: `SELECT ... FROM prices_1min FINAL` |
| 4 | `kafka-init` does not re-run on restart | Topic lost if volume wiped | Add manual topic creation to runbook |
| 5 | **Producer bind-mount breaks on Docker restart** | Producer cannot start | Remove `failed_messages.jsonl` volume bind-mount from `docker-compose.yml` |
| 6 | Pipeline does not auto-recover after host restart | Data gap until manual intervention | Fix Bug 5 + ensure Docker Desktop auto-starts |
