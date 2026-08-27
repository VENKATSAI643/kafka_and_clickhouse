# Kafka ? ClickHouse Real-Time Pipeline

A **production-grade real-time streaming analytics pipeline** that ingests live
cryptocurrency prices from CoinGecko, streams them through Apache Kafka, and
materialises OHLCV candles inside ClickHouse — all within a single
`docker compose up` command.

---

## Architecture

```
+---------------------------------------------------------------------+
¦                         Docker Compose Network                       ¦
¦                                                                      ¦
¦  +--------------+     +------------------------------------------+  ¦
¦  ¦  Python      ¦     ¦           Apache Kafka                    ¦  ¦
¦  ¦  Producer    ¦----?¦  Topic: crypto_prices                    ¦  ¦
¦  ¦  (CoinGecko  ¦     ¦  Partitions: 4  |  Retention: 24 h       ¦  ¦
¦  ¦   every 3 s) ¦     +------------------------------------------+  ¦
¦  +--------------+              ¦                  ¦                  ¦
¦                                ¦                  ¦                  ¦
¦                    +-----------?------+  +--------?-------------+  ¦
¦                    ¦ ClickHouse       ¦  ¦ Python Anomaly        ¦  ¦
¦                    ¦ Kafka Engine     ¦  ¦ Consumer              ¦  ¦
¦                    ¦ (native ingest)  ¦  ¦ (z-score detector)    ¦  ¦
¦                    +------------------+  +-----------------------+  ¦
¦                                ¦                                      ¦
¦                    +-----------?----------------------+             ¦
¦                    ¦  ClickHouse Tables                ¦             ¦
¦                    ¦  kafka_raw_prices   (Kafka Eng.)  ¦             ¦
¦                    ¦  crypto_prices_raw  (MergeTree)   ¦             ¦
¦                    ¦  prices_1min        (Agg. MV)     ¦             ¦
¦                    ¦  prices_5min        (Agg. MV)     ¦             ¦
¦                    ¦  price_anomalies    (MergeTree)   ¦             ¦
¦                    +-----------------------------------+             ¦
+----------------------------------------------------------------------+
```

---

## Project Structure

```
kafka-clickhouse-pipeline/
+-- docker-compose.yml
+-- .env
+-- .env.example
+-- README.md
+-- requirements.txt           # ? single shared venv for ALL Python components
+-- .venv/                     # ? you create this (see Step 2)
¦
+-- producer/
¦   +-- Dockerfile
¦   +-- producer.py            # CoinGecko poller + Kafka publisher
¦
+-- anomaly/
¦   +-- Dockerfile
¦   +-- anomaly_consumer.py    # Rolling z-score anomaly detector
¦
+-- manual_consumer/
¦   +-- Dockerfile
¦   +-- manual_consumer.py     # Phase 6 comparison: manual batch insert
¦
+-- clickhouse/
    +-- config/
    ¦   +-- users.xml           # ClickHouse user / access config
    +-- migrations/
        +-- 01_kafka_engine.sql       # Kafka Engine virtual table
        +-- 02_raw_table.sql          # MergeTree raw storage
        +-- 03_mv_kafka_to_raw.sql    # MV: Kafka ? raw
        +-- 04_1min_aggregation.sql   # 1-min OHLCV + MV
        +-- 05_5min_aggregation.sql   # 5-min OHLCV + MV
```

---

## Prerequisites

| Requirement | Minimum Version | How to check |
|---|---|---|
| Docker Desktop | 4.x | `docker --version` |
| WSL2 integration | Enabled | Docker Desktop ? Settings ? Resources ? WSL Integration |
| Python | 3.10+ | `python3 --version` |
| Free RAM | ~1.5 GB | Stop Spark / Airflow containers first |
| Internet | — | CoinGecko free-tier API (no key required) |

Install Python + build tools if missing (run inside WSL):
```bash
sudo apt update && sudo apt install -y python3.11 python3.11-venv librdkafka-dev python3-dev gcc
```

---

## Quick Start (WSL)

> All commands are run inside your **WSL terminal**.
> Your Windows path `e:\Personal\Kafka and ClickHouse` maps to
> `/mnt/e/Personal/Kafka and ClickHouse` in WSL.

---

### Step 1 — Navigate to the Project

```bash
cd "/mnt/e/Personal/Kafka and ClickHouse"
```

Confirm you are in the right directory:
```bash
ls
```

Expected output:
```
README.md  anomaly  clickhouse  docker-compose.yml  manual_consumer  producer  requirements.txt
```

---

### Step 2 — Create the Python Virtual Environment

This project uses **one single `.venv`** at the project root shared by all Python components.

```bash
# Create the virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate
```

Your prompt will change to show the venv name:
```
(.venv) venky@Venky:/mnt/e/Personal/Kafka and ClickHouse$
```

```bash
# Install system build dependencies (confluent-kafka is a C extension)
sudo apt update && sudo apt install -y librdkafka-dev python3-dev gcc

# Upgrade pip and install all project dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

Verify the install:
```bash
pip list | grep -E "confluent|requests|python-dotenv"
```

Expected output:
```
confluent-kafka   2.3.0
python-dotenv     1.0.0
requests          2.31.0
```

> **Keep the venv active** for all Python commands in the steps below.
> To reactivate later: `source .venv/bin/activate`
> To deactivate: `deactivate`

---

### Step 3 — Configure Environment Variables

```bash
# The .env file is already present. Inspect it:
cat .env
```

Expected contents (defaults work out of the box):
```ini
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=crypto_prices
POLL_INTERVAL_SEC=3
COINS=bitcoin,ethereum,solana,binancecoin
COINGECKO_API_KEY=
ANOMALY_CONSUMER_GROUP=anomaly-detector
Z_SCORE_THRESHOLD=2.5
ROLLING_WINDOW=30
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_NATIVE_PORT=9000
CLICKHOUSE_DB=default
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

If `.env` is missing, create it from the example:
```bash
cp .env.example .env
```

> No changes are needed for a local run. All defaults are correct.

---

### Step 4 — Start the Infrastructure Stack

```bash
docker compose up -d zookeeper kafka clickhouse kafka-init
```

Sample output:
```
[+] Running 4/4
 ? Container zookeeper   Healthy   3.2s
 ? Container kafka       Healthy   18.6s
 ? Container kafka-init  Exited    21.0s
 ? Container clickhouse  Healthy   25.4s
```

Check service health (~30 seconds after startup):
```bash
docker compose ps
```

Expected output:
```
NAME          IMAGE                               STATUS
zookeeper     confluentinc/cp-zookeeper:7.6.0    Up (healthy)
kafka         confluentinc/cp-kafka:7.6.0         Up (healthy)
clickhouse    clickhouse/clickhouse-server:23.8   Up (healthy)
kafka-init    confluentinc/cp-kafka:7.6.0         Exited (0)
```

> `kafka-init` must exit with code **0**. Exit code 1 means topic creation failed.
> Debug: `docker compose logs kafka-init`

Verify the Kafka topic was created:
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Expected:
```
crypto_prices
```

---

### Step 5 — Apply ClickHouse Migrations

Run each SQL migration **in order** inside the ClickHouse container:

```bash
docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/01_kafka_engine.sql
echo "? 01 done — Kafka Engine virtual table"

docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/02_raw_table.sql
echo "? 02 done — MergeTree raw storage"

docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/03_mv_kafka_to_raw.sql
echo "? 03 done — Materialized View: Kafka ? raw"

docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/04_1min_aggregation.sql
echo "? 04 done — 1-minute OHLCV aggregation"

docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/05_5min_aggregation.sql
echo "? 05 done — 5-minute OHLCV aggregation"
```

Verify all tables were created:
```bash
docker exec clickhouse clickhouse-client -q 'SHOW TABLES'
```

Expected output:
```
crypto_prices_raw
kafka_raw_prices
mv_1min
mv_5min
mv_kafka_to_raw
prices_1min
prices_5min
```

> **Order matters** — run migrations `01` ? `05` in sequence.
> `03_mv_kafka_to_raw` depends on both `01` and `02` existing first.

---

### Step 6 — Start the Producer

The producer polls CoinGecko every 3 seconds and publishes one JSON message per coin to Kafka.

#### Option A — Inside Docker (recommended for long-running sessions)

```bash
docker compose up -d producer
docker compose logs -f producer
```

#### Option B — Natively in WSL (for debugging / interactive use)

```bash
source .venv/bin/activate

KAFKA_BROKER=localhost:29092 \
POLL_INTERVAL_SEC=3 \
COINS=bitcoin,ethereum,solana,binancecoin \
python producer/producer.py
```

Expected log output (every 3 seconds):
```
2026-08-26T00:20:01 [INFO] producer — Starting CoinGecko ? Kafka producer
2026-08-26T00:20:01 [INFO] producer —   Broker:        kafka:9092
2026-08-26T00:20:01 [INFO] producer —   Topic:         crypto_prices
2026-08-26T00:20:01 [INFO] producer —   Coins:         ['bitcoin', 'ethereum', 'solana', 'binancecoin']
2026-08-26T00:20:01 [INFO] producer —   Poll interval: 3s
2026-08-26T00:20:01 [INFO] producer — Connected to Kafka broker: kafka:9092
2026-08-26T00:20:02 [INFO] producer — Published 4 messages | prices: {'bitcoin': '$78,532.00', 'ethereum': '$2,441.50', 'solana': '$96.21', 'binancecoin': '$692.80'}
2026-08-26T00:20:05 [INFO] producer — Published 4 messages | prices: {'bitcoin': '$78,535.00', 'ethereum': '$2,441.80', 'solana': '$96.23', 'binancecoin': '$692.85'}
```

Each Kafka message payload looks like this (one per coin):
```json
{
  "coin":         "bitcoin",
  "price_usd":    78532.00,
  "change_24h":   1.23,
  "event_ts":     "2026-08-26T00:20:02Z",
  "ingested_at":  "2026-08-26T00:20:02.341Z"
}
```

---

### Step 7 — Verify Data in ClickHouse

Open in your **Windows browser**: **http://localhost:8123/play**

Or use `clickhouse-client` from your WSL terminal:

```bash
# Row count per coin — should grow every 3 seconds
docker exec clickhouse clickhouse-client -q '
SELECT coin, count() AS rows, max(event_ts) AS latest
FROM crypto_prices_raw
GROUP BY coin ORDER BY coin'
```

Sample output (after ~1 minute):
```
binancecoin   20   2026-08-26 00:21:02.000
bitcoin       20   2026-08-26 00:21:02.000
ethereum      20   2026-08-26 00:21:02.000
solana        20   2026-08-26 00:21:02.000
```

```bash
# 1-min OHLCV candles for Bitcoin (available after ~60 seconds of data)
docker exec clickhouse clickhouse-client -q "
SELECT
    window_start,
    open,
    high,
    low,
    close,
    tick_count
FROM prices_1min
WHERE coin = 'bitcoin'
ORDER BY window_start DESC
LIMIT 5"
```

Sample output:
```
2026-08-26 00:21:00   78532.00   78609.00   78501.00   78572.00   20
2026-08-26 00:20:00   78490.00   78545.00   78480.00   78532.00   20
2026-08-26 00:19:00   78450.00   78510.00   78440.00   78490.00   20
```

```bash
# Data lag check (how far behind ClickHouse is from real-time)
docker exec clickhouse clickhouse-client -q '
SELECT coin, now()-max(event_ts) AS lag_seconds
FROM crypto_prices_raw GROUP BY coin ORDER BY coin'
```

Sample output:
```
binancecoin   1
bitcoin       1
ethereum      2
solana        1
```

---

### Step 8 — Start the Anomaly Consumer

The anomaly consumer subscribes to the same Kafka topic under an independent consumer
group and runs a rolling Z-score detector (default window: 30 ticks, threshold: 2.5s).

#### Option A — Inside Docker

```bash
docker compose up -d --build anomaly_consumer
docker compose logs -f anomaly_consumer
```

#### Option B — Natively in WSL

```bash
source .venv/bin/activate

KAFKA_BROKER=localhost:29092 \
CLICKHOUSE_HOST=localhost \
CLICKHOUSE_PORT=8123 \
Z_SCORE_THRESHOLD=2.5 \
ROLLING_WINDOW=30 \
python anomaly/anomaly_consumer.py
```

Expected startup logs:
```
2026-08-26T00:24:54 [INFO] anomaly — Starting anomaly consumer
2026-08-26T00:24:54 [INFO] anomaly —   Broker:          kafka:9092
2026-08-26T00:24:54 [INFO] anomaly —   Topic:           crypto_prices
2026-08-26T00:24:54 [INFO] anomaly —   Consumer group:  anomaly-detector
2026-08-26T00:24:54 [INFO] anomaly —   Z threshold:     2.50
2026-08-26T00:24:54 [INFO] anomaly —   Rolling window:  30 ticks/coin
2026-08-26T00:24:54 [INFO] anomaly — Anomaly table ready in ClickHouse.
2026-08-26T00:24:55 [INFO] anomaly — Connected to Kafka broker: kafka:9092
2026-08-26T00:24:55 [INFO] anomaly — Consumer group: anomaly-detector
```

When a price spike is detected:
```
2026-08-26T00:31:09 [WARNING] anomaly — [ALERT #1] SOLANA spike ?  price=$96.44  z=3.315  µ=$96.35  s=$0.03
2026-08-26T00:32:24 [WARNING] anomaly — [ALERT #1] BITCOIN spike ?  price=$78,609.00  z=3.884  µ=$78546.00  s=$16.22
```

> **Warm-up period**: The detector needs a minimum of **10 price ticks per coin**
> before computing z-scores. With the default 3-second poll, expect first alerts after ~30–60 seconds.

Query detected anomalies from ClickHouse:
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

Sample output:
```
bitcoin   78609.00   3.884   78546.00   16.22   2026-08-26 00:32:24   2026-08-26 00:32:24
solana    96.44      3.315   96.35      0.03    2026-08-26 00:31:09   2026-08-26 00:31:09
```

---

### Step 9 — (Optional) Manual Consumer — Comparison Study

Runs a Python-based batch consumer that inserts into ClickHouse via HTTP instead
of the native Kafka Engine. Used to benchmark latency against the built-in engine.

#### Option A — Inside Docker

```bash
docker compose up -d manual_consumer
docker compose logs -f manual_consumer
```

#### Option B — Natively in WSL

```bash
source .venv/bin/activate

KAFKA_BROKER=localhost:29092 \
CLICKHOUSE_HOST=localhost \
BATCH_SIZE=50 \
FLUSH_INTERVAL_SEC=5 \
python manual_consumer/manual_consumer.py
```

Every 30 seconds it logs batch insert latency:
```
Stats (30s snapshot) | messages=120 | {'total_inserted': 118, 'avg_latency_ms': 12.4, 'total_errors': 0}
```

---

### Step 10 — Check Consumer Group Lag

Verify both consumer groups are keeping up with the Kafka topic:

```bash
# ClickHouse Kafka Engine consumer group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group clickhouse-consumer \
  --describe
```

Sample output:
```
GROUP                TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
clickhouse-consumer  crypto_prices  0          1240            1240            0
clickhouse-consumer  crypto_prices  1          1238            1238            0
clickhouse-consumer  crypto_prices  2          1241            1241            0
clickhouse-consumer  crypto_prices  3          1239            1239            0
```

```bash
# Anomaly detector consumer group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group anomaly-detector \
  --describe
```

A **LAG of 0** on all partitions means real-time consumption.

---

### Step 11 — Teardown

```bash
# Stop all containers, KEEP ClickHouse data volume (safe for resume)
docker compose down

# Full teardown — WARNING: permanently deletes all ClickHouse data
docker compose down -v

# Deactivate the Python venv
deactivate
```

---

## Verification Queries (Full Reference)

Open http://localhost:8123/play in your browser, or prefix with
`docker exec clickhouse clickhouse-client -q '...'`.

```sql
-- All tables in the database
SHOW TABLES;

-- Raw data: row count and freshness per coin
SELECT coin, count() AS rows, max(event_ts) AS last_seen
FROM crypto_prices_raw
GROUP BY coin
ORDER BY coin;

-- Latest 10 raw ticks for Bitcoin
SELECT coin, price_usd, change_24h, event_ts, ingested_at
FROM crypto_prices_raw
WHERE coin = 'bitcoin'
ORDER BY event_ts DESC
LIMIT 10;

-- 1-minute OHLCV candles for Bitcoin
SELECT window_start, open, high, low, close, tick_count
FROM prices_1min
WHERE coin = 'bitcoin'
ORDER BY window_start DESC
LIMIT 10;

-- 5-minute OHLCV candles (all coins)
SELECT coin, window_start, open, high, low, close, tick_count
FROM prices_5min
ORDER BY window_start DESC
LIMIT 20;

-- Anomalies detected (requires anomaly_consumer running)
SELECT coin, price_usd, z_score, window_mean, window_std, detected_at
FROM price_anomalies
ORDER BY detected_at DESC
LIMIT 10;

-- Price range volatility by coin (last 5 minutes)
SELECT
    coin,
    min(price_usd)  AS low,
    max(price_usd)  AS high,
    max(price_usd) - min(price_usd) AS range_usd
FROM crypto_prices_raw
WHERE event_ts >= now() - INTERVAL 5 MINUTE
GROUP BY coin
ORDER BY range_usd DESC;
```

---

## Known Issues & Fixes

### Fix: ClickHouse DateTime Parsing Error (`Z` suffix)

**Symptom** (in `docker compose logs anomaly_consumer`):
```
ClickHouse insert failed (400): Code: 27. DB::ParsingException:
Cannot parse input: expected '"' before: 'Z", "detected_at": ...
```

**Root cause**: The producer stamps timestamps with an ISO 8601 UTC `Z` suffix
(e.g. `"2026-08-26T00:31:09Z"`). ClickHouse's `DateTime64` JSONEachRow parser
accepts `YYYY-MM-DDTHH:MM:SS[.fff]` but **rejects** the trailing `Z`.

**Fix applied** in `anomaly/anomaly_consumer.py`:
```python
def _ch_datetime(ts: str) -> str:
    """Strip the trailing 'Z' UTC suffix before inserting into ClickHouse."""
    return ts.rstrip("Z")
```

The `event_ts` field is sanitised before every ClickHouse HTTP insert.

**After applying the fix, rebuild the container:**
```bash
docker compose up -d --build anomaly_consumer
```

---

## Troubleshooting

### `kafka-init` exits with non-zero code
```bash
docker compose logs kafka-init
```
Kafka may not have been fully ready. Re-run:
```bash
docker compose up kafka-init
```

### Migrations fail with "Table already exists"
Safe to ignore — all SQL files use `CREATE TABLE IF NOT EXISTS`.
For a schema mismatch, do a full reset:
```bash
docker compose down -v
docker compose up -d zookeeper kafka clickhouse kafka-init
# Then re-apply all five migrations
```

### `crypto_prices_raw` stays at 0 rows
- Wait 10–15 seconds — ClickHouse polls Kafka on its own schedule
- Confirm tables exist: `docker exec clickhouse clickhouse-client -q 'SHOW TABLES'`
- Check producer is publishing: `docker compose logs producer --tail=5`

### `price_anomalies` table is empty

**Step 1 — Confirm the fix was applied and container rebuilt:**
```bash
docker compose up -d --build anomaly_consumer
docker compose logs anomaly_consumer --tail=20
```
Verify no `ClickHouse insert failed` lines appear after `[ALERT]` lines.

**Step 2 — Check raw data volume (need 10+ rows per coin):**
```bash
docker exec clickhouse clickhouse-client -q '
SELECT coin, count() FROM crypto_prices_raw GROUP BY coin'
```

**Step 3 — Insert a manual test row to confirm the table works:**
```bash
docker exec clickhouse clickhouse-client -q "
INSERT INTO price_anomalies VALUES (
  'bitcoin', 99999.99, 5.0, 60000.0, 500.0, now(), now()
)"
docker exec clickhouse clickhouse-client -q 'SELECT * FROM price_anomalies'
```

**Step 4 — Lower the Z-score threshold to force detections:**
```bash
docker compose stop anomaly_consumer
Z_SCORE_THRESHOLD=1.5 docker compose up -d anomaly_consumer
docker compose logs -f anomaly_consumer
```
A threshold of `1.5` flags normal price fluctuations — confirms the end-to-end pipeline.

### `confluent-kafka` fails to install
```bash
sudo apt install -y librdkafka-dev python3-dev gcc
pip install "confluent-kafka>=2.3.0,<3.0.0"
```

### Anomaly consumer logs "ClickHouse unreachable" on startup
This is a normal startup race condition. The consumer retries automatically:
```
[WARNING] ClickHouse unreachable during table setup (attempt 1/10): ...
[WARNING] ClickHouse unreachable during table setup (attempt 2/10): ...
[INFO]    Anomaly table ready in ClickHouse.    ? appears once connected
```
No action needed — wait for ClickHouse to become healthy.

---

## Approach Comparison

| Dimension | ClickHouse Kafka Engine | Manual Python Consumer |
|---|---|---|
| **External process** | None — pure SQL | Separate container |
| **Throughput** | Very high (C++ internals) | Moderate — tunable |
| **Insert latency** | ~1–2 s (ClickHouse polls) | Configurable — sub-second |
| **Error handling** | Skip broken msgs only | Full retry, DLQ, alerting |
| **Offset visibility** | Opaque (ClickHouse manages) | Fully visible in Kafka |
| **Schema changes** | DDL + consumer restart | Flexible Python dict |
| **Best for** | Bulk ingestion, simple ETL | Complex transforms, multi-step |

> **This project implements both** — native engine for primary ingestion,
> Python consumer for anomaly detection and manual comparison.

---

## Key Concepts

| Concept | Why It Matters |
|---|---|
| **Kafka Consumer Groups** | Two groups (ClickHouse + anomaly) consume the same topic independently |
| **ClickHouse MergeTree** | Foundation of all ClickHouse storage |
| **AggregatingMergeTree** | Needed for correct incremental aggregations |
| **Materialized Views** | In ClickHouse, MVs are triggers, not snapshots — they fire on every INSERT |
| **Kafka Engine internals** | ClickHouse polls Kafka in background C++ threads; `kafka_num_consumers` = thread count |
| **At-least-once delivery** | Kafka Engine gives at-least-once; handle duplicates at query time |
| **argMin / argMax** | ClickHouse functions to get first/last value in a group — essential for OHLCV open/close |
| **Rolling Z-Score** | `z = (x - µ) / s` over a sliding window; flags statistically unusual prices |

---

## Gotchas

> **Offset reset on DROP**: If you `DROP TABLE kafka_raw_prices` and recreate it,
> ClickHouse resets to `latest` offset — messages produced during downtime are lost.

> **MVs do not backfill**: Aggregation tables only contain data from after the MV
> was created. No historical replay.

> **Never SELECT from the Kafka Engine table in production** — each SELECT consumes
> and commits offsets, starving the Materialized View.

> **DateTime `Z` suffix**: ClickHouse's JSONEachRow parser rejects the ISO 8601 `Z`
> suffix on `DateTime64` columns. Always strip it before inserting (see Known Issues).

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `crypto_prices` | Topic name |
| `POLL_INTERVAL_SEC` | `3` | Seconds between CoinGecko polls |
| `COINS` | `bitcoin,ethereum,solana,binancecoin` | Comma-separated coin IDs |
| `COINGECKO_API_KEY` | _(empty)_ | CoinGecko Pro key (optional) |
| `Z_SCORE_THRESHOLD` | `2.5` | Anomaly alert trigger threshold |
| `ROLLING_WINDOW` | `30` | Number of ticks in z-score window |
| `BATCH_SIZE` | `50` | Messages per ClickHouse HTTP insert (manual consumer) |
| `FLUSH_INTERVAL_SEC` | `5` | Max seconds between flushes (manual consumer) |
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse hostname |
| `CLICKHOUSE_HTTP_PORT` | `8123` | ClickHouse HTTP interface port |
| `CLICKHOUSE_DB` | `default` | ClickHouse database name |

---

## Portfolio Differentiators

1. **ClickHouse native Kafka Engine** — used at Cloudflare, Contentsquare, and high-throughput analytics shops; rarely covered in tutorials
2. **Real-time OHLCV materialisation** — demonstrates streaming aggregation, not just ingestion
3. **Dual-consumer architecture** — analytics + ML anomaly detection on the same Kafka stream
4. **Documented trade-off analysis** — native vs. manual consumer with measurable latency comparison
5. **Production-grade error handling** — dead-letter queue, exponential backoff, startup retry loops
