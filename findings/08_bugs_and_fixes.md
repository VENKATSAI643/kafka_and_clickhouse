# Finding 08 — Bugs Found and Fixed
**Session 1 Last Updated:** 2026-08-26 05:37 IST  
**Session 2 Last Updated:** 2026-08-27 22:30 IST  
**Pipeline Status:** ? LIVE — all services running, data flowing

---

## Bug 1 — Missing Kafka Topic (kafka-init never ran)

### Symptom
Producer logs showed:
```
[ERROR] producer — Delivery failed: topic=crypto_prices key=b'bitcoin'
        error=KafkaError{code=UNKNOWN_TOPIC_OR_PART,val=3,str="Broker: Unknown topic or partition"}
```

### Root Cause
The `kafka-init` one-shot container (restart: "no") had already exited on a prior run.
On `docker compose up`, it does not re-run. Topic was never created on this boot.

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
Created topic crypto_prices.
```

**Status:** ? Fixed (Session 1)

---

## Bug 2 — ClickHouse Silently Dropping All Messages (DateTime Z Suffix — Kafka Engine)

### Symptom
- Kafka consumer group showed LAG=0 (messages consumed)
- `crypto_prices_raw` stayed empty
- `kafka_skip_broken_messages=10` silently discarded every message

### Root Cause
Producer emits ISO 8601 timestamps:
```json
{
  "event_ts":    "2026-08-25T16:50:10Z",
  "ingested_at": "2026-08-25T16:51:26.739Z"
}
```
ClickHouse `DateTime` with `JSONEachRow` only accepts `"YYYY-MM-DD HH:MM:SS"`.
The `T` separator and `Z` suffix cause a silent parse failure for every single message.

### Confirmed via kafka-console-consumer
```bash
timeout 5 docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic crypto_prices \
  --from-beginning --max-messages 2
```

Output confirmed the Z suffix was present in all messages.

### Fix Applied

Step 1 — Drop broken tables:
```bash
docker exec clickhouse clickhouse-client -q 'DROP TABLE IF EXISTS mv_kafka_to_raw'
docker exec clickhouse clickhouse-client -q 'DROP TABLE IF EXISTS kafka_raw_prices'
```

Step 2 — Updated `clickhouse/migrations/01_kafka_engine.sql`
Changed `event_ts` and `ingested_at` from `DateTime` to `String`:
```sql
-- BEFORE
event_ts    DateTime,
ingested_at DateTime64(3)

-- AFTER
event_ts    String,
ingested_at String
```

Step 3 — Updated `clickhouse/migrations/03_mv_kafka_to_raw.sql`
Added ISO 8601 cast in the Materialized View:
```sql
-- AFTER
SELECT
    coin,
    price_usd,
    change_24h,
    toDateTime(parseDateTime64BestEffort(event_ts))    AS event_ts,
    parseDateTime64BestEffort(ingested_at, 3)          AS ingested_at
FROM kafka_raw_prices;
```

Step 4 — Re-applied fixed migrations. Data started flowing immediately.

**Status:** ? Fixed (Session 1)

---

## Bug 3 — IF NOT EXISTS Migrations No-Op on Schema Changes

### Symptom
Running the fixed migrations did nothing — tables already existed with wrong schema.

### Root Cause
All migrations use `CREATE TABLE IF NOT EXISTS` — they silently skip if table exists,
even if the schema has changed.

### Fix Applied
Always manually DROP the affected table before re-running its migration:
```bash
docker exec clickhouse clickhouse-client -q 'DROP TABLE IF EXISTS <table_name>'
```

Safe to drop (no data stored):
- `kafka_raw_prices` — Kafka Engine virtual table
- `mv_kafka_to_raw` — MaterializedView (trigger only)

**WARNING:** Do NOT drop `crypto_prices_raw`, `prices_1min`, or `prices_5min` without backing up data.

**Status:** ? Documented (Session 1)

---

## Bug 4 — Producer Fails to Start After Docker Desktop Restart (Exit 127, WSL Bind-Mount)

### Symptom
After Docker Desktop restart, producer container exited immediately with code 127:
```
127 failed to create task for container: failed to create shim task: OCI runtime create failed:
runc create failed: unable to start container process: error during container init:
error mounting "/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/Ubuntu-26.04/..."
to rootfs at "/app/failed_messages.jsonl": not a directory
```

### Root Cause
Volume bind-mount for `failed_messages.jsonl` becomes stale after Docker Desktop restarts.
The WSL bind-mount path is recreated as a directory instead of a file.

### Fix Applied
Removed the volume bind-mount from `docker-compose.yml`.
File is created inside the image via `RUN touch /app/failed_messages.jsonl` in the Dockerfile.

**Status:** ? Fixed (Session 1)

---

## Bug 5 — Pipeline Does Not Auto-Recover After Host Restart

### Symptom
ClickHouse `crypto_prices_raw` frozen at `2026-08-25 17:01` (~12 hours stale).

### Root Cause
Producer has `restart: unless-stopped` but fails on startup (Bug 4) before the
restart policy can kick in. OCI error causes the restarter to give up.

### Fix
Fix Bug 4 first (remove bind-mount), then:
```bash
docker compose up -d producer
docker compose logs -f producer
```

**Status:** ? Fixed (Session 1)

---

## Bug 6 — anomaly_consumer ClickHouse Insert Fails (DateTime Z Suffix — HTTP Insert)

**Discovered:** Session 2, 2026-08-27  
**File affected:** `anomaly/anomaly_consumer.py`

### Symptom
Anomaly detection was working (alerts printing to logs) but NOT persisting to ClickHouse:
```
[WARNING] anomaly — [ALERT #1] SOLANA spike ?  price=$96.44  z=3.315
[WARNING] anomaly — ClickHouse insert failed (400): Code: 27. DB::ParsingException:
          Cannot parse input: expected '"' before: 'Z", "detected_at": "2026-08-26T00:31:09.970"
```

### Root Cause
The `event_ts` field from Kafka messages contains a trailing `Z` suffix
(e.g. `"2026-08-26T00:31:09Z"`).

ClickHouse's `DateTime64` **JSONEachRow HTTP insert parser** rejects this suffix —
it accepts `YYYY-MM-DDTHH:MM:SS[.fff]` but not the `Z` terminator.

This is the **same class of bug as Bug 2**, but in a different layer:
- Bug 2 = Kafka Engine DDL (server-side SQL)
- Bug 6 = Python HTTP insert via `requests` (client-side)

### Fix Applied
Added `_ch_datetime()` helper in `anomaly/anomaly_consumer.py`:

```python
def _ch_datetime(ts: str) -> str:
    """Strip the trailing 'Z' UTC suffix before inserting into ClickHouse."""
    return ts.rstrip("Z")
```

Called before building the insert payload:
```python
"event_ts": _ch_datetime(event_ts),   # "2026-08-26T00:31:09" — no Z
```

Rebuild to apply:
```bash
docker compose up -d --build anomaly_consumer
```

**Status:** ? Fixed (Session 2)

---

## Bug 7 — CoinGecko 429 Rate Limiting Causes 62s Data Gaps Every ~5 Polls

**Discovered:** Session 2, 2026-08-27  
**File affected:** `producer/producer.py`, `.env`

### Symptom
Rate limiting occurring roughly every 5 successful polls:
```
[WARNING] Rate limited (429). Retry 1/5 in 2s
[WARNING] Rate limited (429). Retry 2/5 in 4s
[WARNING] Rate limited (429). Retry 3/5 in 8s
[WARNING] Rate limited (429). Retry 4/5 in 16s
[WARNING] Rate limited (429). Retry 5/5 in 32s
[ERROR]   CoinGecko fetch failed after 5 attempts.
[ERROR]   Skipping this poll cycle — no data.   ? 62-second gap in Kafka
```

### Root Cause
- `POLL_INTERVAL_SEC=3` ? ~20 req/min
- CoinGecko free tier: ~10–30 req/min but enforces burst throttle
- On rate limit, all 5 retries fire (2+4+8+16+32 = 62s wasted)
- After all retries fail: `Skipping this poll cycle` — Kafka gets NO messages for 62s
- Anomaly detector z-score window gets corrupted with gaps

### Fix Applied
Two changes in `producer/producer.py`:

**1. Raised default poll interval: `3s ? 10s`**
```python
# BEFORE
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "3"))

# AFTER (6 req/min — well under free-tier limit)
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "10"))
```

**2. Added last-data cache — stale publish instead of silent skip:**
```python
_last_data: dict | None = None   # cache of last successful CoinGecko response

data = fetch_prices(session)
if data is None:
    if _last_data is not None:
        logger.warning("[STALE] CoinGecko unavailable — republishing last known prices.")
        data = _last_data
    else:
        logger.error("Skipping this poll cycle — no data and no cache yet.")

if data is not None:
    _last_data = data   # update cache on every successful fetch
    # ... publish to Kafka as normal
```

**Also updated `.env`:**
```ini
POLL_INTERVAL_SEC=10
```

### New Behaviour After Fix
```
[INFO]    Published 4 messages | prices: {'bitcoin': '$80,521.00', ...}  ? fresh
[WARNING] [STALE] CoinGecko unavailable — republishing last known prices.
[INFO]    Published 4 messages | prices: {'bitcoin': '$80,521.00', ...}  ? stale but no gap
[INFO]    Published 4 messages | prices: {'bitcoin': '$80,535.00', ...}  ? fresh resumes
```

**Status:** ? Fixed (Session 2)

---

## System State — Session 2 (2026-08-27 22:30 IST)

| Component | Status | Detail |
|---|---|---|
| `zookeeper` | ? Up (healthy) | Running fine |
| `kafka` | ? Up (healthy) | Topic `crypto_prices`, 4 partitions |
| `clickhouse` | ? Up (healthy) | All 7 tables + price_anomalies |
| `kafka-init` | ? Exited (0) | Topic created correctly |
| `producer` | ? Running | Poll #40–50+, rate-limit fix applied |
| `anomaly_consumer` | ? Running | Alerts firing, persisting to ClickHouse |
| `manual_consumer` | ? Not started | Optional — comparison study only |

### Live Prices at Capture (2026-08-27 ~16:48 IST)
| Coin | Price | 24h Change |
|---|---|---|
| Bitcoin | $80,521.00 | — |
| Ethereum | $2,528.70 | — |
| Solana | $108.77 | — |
| BNB | $712.86 | — |

### Anomaly Alerts Detected (Session 2)
| Alert | Coin | Price | Z-Score | Direction |
|---|---|---|---|---|
| #1 | SOLANA | $96.44 | 3.315 | ? spike |
| #1 | BINANCECOIN | $693.31 | 2.821 | ? spike |
| #1 | BITCOIN | $78,609.00 | 3.884 | ? spike |
| #1 | ETHEREUM | $2,443.22 | 2.949 | ? spike |
| #2 | BINANCECOIN | $693.68 | 3.029 | ? spike |
| #2 | BITCOIN | $78,609.00 | 2.822 | ? spike |

---

## Remaining Issues

| # | Issue | Impact | Recommended Fix |
|---|---|---|---|
| 1 | `prices_1min`/`prices_5min` unmerged parts | Duplicate rows in OHLCV until background merge | Use `FINAL` keyword: `SELECT ... FROM prices_1min FINAL` |
| 2 | `kafka-init` does not re-run on restart | Topic lost if volume wiped | Add manual topic creation to runbook |
| 3 | Stale prices in Kafka during API outages | Anomaly detector z-score receives flat data | Expected + documented; use `[STALE]` log to filter if needed |
| 4 | No CoinGecko API key | Free tier burst throttle | Set `COINGECKO_API_KEY` in `.env` to use Pro tier (3s interval safe) |
