# Kafka → ClickHouse Real-Time Pipeline

A **production-grade real-time streaming analytics pipeline** that ingests live
cryptocurrency prices from CoinGecko, streams them through Apache Kafka, and
materialises OHLCV candles inside ClickHouse — all within a single
`docker compose up` command.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Compose Network                       │
│                                                                      │
│  ┌──────────────┐     ┌──────────────────────────────────────────┐  │
│  │  Python      │     │           Apache Kafka                    │  │
│  │  Producer    │────▶│  Topic: crypto_prices                    │  │
│  │  (CoinGecko  │     │  Partitions: 4  |  Retention: 24 h       │  │
│  │   every 3 s) │     └──────────────────────────────────────────┘  │
│  └──────────────┘              │                  │                  │
│                                │                  │                  │
│                    ┌───────────▼──────┐  ┌────────▼─────────────┐  │
│                    │ ClickHouse       │  │ Python Anomaly        │  │
│                    │ Kafka Engine     │  │ Consumer              │  │
│                    │ (native ingest)  │  │ (z-score detector)    │  │
│                    └───────────┬──────┘  └───────────────────────┘  │
│                                │                                      │
│                    ┌───────────▼──────────────────────┐             │
│                    │  ClickHouse Tables                │             │
│                    │  kafka_raw_prices   (Kafka Eng.)  │             │
│                    │  crypto_prices_raw  (MergeTree)   │             │
│                    │  prices_1min        (Agg. MV)     │             │
│                    │  prices_5min        (Agg. MV)     │             │
│                    │  price_anomalies    (MergeTree)   │             │
│                    └───────────────────────────────────┘             │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
kafka-clickhouse-pipeline/
├── docker-compose.yml
├── .env
├── README.md
├── requirements.txt           # ← single shared venv for ALL Python components
├── .venv/                     # ← you create this (see Quick Start step 1)
│
├── producer/
│   ├── Dockerfile
│   └── producer.py            # CoinGecko poller + Kafka publisher
│
├── anomaly/
│   ├── Dockerfile
│   └── anomaly_consumer.py    # Rolling z-score anomaly detector
│
├── manual_consumer/
│   ├── Dockerfile
│   └── manual_consumer.py     # Phase 6 comparison: manual batch insert
│
└── clickhouse/
    ├── config/
    │   └── users.xml           # ClickHouse user / access config
    └── migrations/
        ├── 01_kafka_engine.sql       # Kafka Engine virtual table
        ├── 02_raw_table.sql          # MergeTree raw storage
        ├── 03_mv_kafka_to_raw.sql    # MV: Kafka → raw
        ├── 04_1min_aggregation.sql   # 1-min OHLCV + MV
        └── 05_5min_aggregation.sql   # 5-min OHLCV + MV
```

---

## Quick Start (WSL)

> All commands are run inside your **WSL terminal**.
> Your Windows project path `e:\Personal\Kafka and ClickHouse` maps to
> `/mnt/e/Personal/Kafka and ClickHouse` in WSL.

---

### Prerequisites

| Requirement | Notes |
|---|---|
| Docker Desktop ≥ 4.x | Enable **WSL2 integration** in Docker Desktop → Settings → Resources → WSL Integration |
| Python 3.10+ | `python3 --version` — install via `sudo apt install python3.11 python3.11-venv` if missing |
| ~1.5 GB free RAM | Stop Spark / Airflow containers first |
| Internet access | CoinGecko free-tier API |

---

### Step 1 — Navigate to the project in WSL

```bash
cd "/mnt/e/Personal/Kafka and ClickHouse"
```

---

### Step 2 — Create the shared Python virtual environment

This project uses **one single `.venv`** at the project root.
It covers the producer, anomaly consumer, and manual consumer — you never need to activate separate envs.

```bash
# Create the virtual environment
python3 -m venv .venv
```

```bash
# Activate it
source .venv/bin/activate
```

```bash
# Install system-level build dependencies FIRST
# confluent-kafka is a C extension that wraps librdkafka —
# the headers must be present before pip can compile it.
sudo apt update && sudo apt install -y librdkafka-dev python3-dev gcc
```

```bash
# Upgrade pip and install all project dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

Verify the install:

```bash
pip list
# Should show: confluent-kafka, requests, python-dotenv
```

> **Keep the venv active** for all Python commands in the steps below.
> To reactivate later: `source .venv/bin/activate`
> To deactivate: `deactivate`

---

### Step 3 — Start the infrastructure stack

```bash
docker compose up -d zookeeper kafka clickhouse kafka-init
```

Check service health (~30 seconds):

```bash
docker compose ps
# zookeeper      — healthy
# kafka          — healthy
# clickhouse     — healthy
# kafka-init     — exited (0)   ← one-shot topic creator, exit 0 = success
```

---

### Step 4 — Apply ClickHouse migrations

Run each SQL file directly inside the ClickHouse container.
This bypasses any WSL localhost networking/proxy bugs.

```bash
docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/01_kafka_engine.sql
echo "01 done"

docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/02_raw_table.sql
echo "02 done"

docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/03_mv_kafka_to_raw.sql
echo "03 done"

docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/04_1min_aggregation.sql
echo "04 done"

docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/migrations/05_5min_aggregation.sql
echo "05 done"
```

Verify tables were created:

```bash
docker exec clickhouse clickhouse-client -q 'SHOW TABLES'
# Expected output:
# crypto_prices_raw
# kafka_raw_prices
# mv_1min
# mv_5min
# mv_kafka_to_raw
# prices_1min
# prices_5min
```

---

### Step 5 — Run the Producer

#### Option A — Inside Docker (recommended for long runs)

```bash
docker compose up -d producer
docker compose logs -f producer
```

#### Option B — Natively in WSL (venv must be active)

> Use this when you want to inspect or debug the producer interactively.
> Kafka is exposed on `localhost:29092` and ClickHouse on `localhost:8123` from WSL.

```bash
source .venv/bin/activate

KAFKA_BROKER=localhost:29092 \
POLL_INTERVAL_SEC=3 \
COINS=bitcoin,ethereum,solana,binancecoin \
python producer/producer.py
```

Expected output:

```
Published 4 messages | prices: {'bitcoin': '$61,432.00', 'ethereum': '$3,120.50', ...}
```

---

### Step 6 — Verify data in ClickHouse

Open in your **Windows browser**: **http://localhost:8123/play**

Or use `clickhouse-client` from your WSL terminal (this avoids HTTP proxy issues):

```bash
# Row count per coin — should grow every 3 seconds
docker exec clickhouse clickhouse-client -q '
SELECT coin, count(), max(event_ts) AS latest
FROM crypto_prices_raw
GROUP BY coin ORDER BY coin'

# 1-min candles for Bitcoin (available after ~60 seconds of data)
docker exec clickhouse clickhouse-client -q '
SELECT * FROM prices_1min
WHERE coin = '\''bitcoin'\''
ORDER BY window_start DESC LIMIT 5'

# Data lag check
docker exec clickhouse clickhouse-client -q '
SELECT coin, now()-max(event_ts) AS lag_sec
FROM crypto_prices_raw GROUP BY coin'
```

---

### Step 7 — Run the Anomaly Consumer

#### Option A — Inside Docker

```bash
docker compose up -d anomaly_consumer
docker compose logs -f anomaly_consumer
```

#### Option B — Natively in WSL (venv must be active)

```bash
source .venv/bin/activate

KAFKA_BROKER=localhost:29092 \
CLICKHOUSE_HOST=localhost \
CLICKHOUSE_PORT=8123 \
Z_SCORE_THRESHOLD=2.5 \
ROLLING_WINDOW=30 \
python anomaly/anomaly_consumer.py
```

Watch for `[ALERT]` lines:
```
[ALERT #1] BITCOIN spike ↑  price=$64,210.00  z=2.91  μ=$61,432.00  σ=$958.23
```

---

### Step 8 — (Optional) Manual Consumer — Comparison Study

#### Option A — Inside Docker

```bash
docker compose up -d manual_consumer
docker compose logs -f manual_consumer
```

#### Option B — Natively in WSL (venv must be active)

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

Compare this against the ClickHouse Kafka Engine (which you can't directly measure — instead observe `prices_1min` fill rate vs `crypto_prices_manual` fill rate).

---

### Teardown

```bash
# Stop all containers, keep ClickHouse data volume
docker compose down

# Full teardown including all data (WARNING: deletes all ClickHouse data)
docker compose down -v

# Deactivate the Python venv
deactivate
```

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
| **Materialized Views** | In ClickHouse, MVs are *triggers*, not snapshots — they fire on every INSERT |
| **Kafka Engine internals** | ClickHouse polls Kafka in background C++ threads; `kafka_num_consumers` = thread count |
| **At-least-once delivery** | Kafka Engine gives at-least-once; handle duplicates at query time |
| **argMin / argMax** | ClickHouse functions to get first/last value in a group — essential for OHLCV open/close |

---

## Gotchas

> **Offset reset on DROP**: If you `DROP TABLE kafka_raw_prices` and recreate it,
> ClickHouse resets to `latest` offset — messages produced during downtime are lost.

> **MVs don't backfill**: Aggregation tables only contain data from *after* the MV
> was created. No historical replay.

> **Never SELECT from the Kafka Engine table in production** — each SELECT consumes
> and commits offsets, starving the Materialized View.

---

## Verification Queries

```sql
-- Consumer group lag (run from host shell, not ClickHouse):
-- docker exec kafka kafka-consumer-groups \
--   --bootstrap-server kafka:9092 \
--   --group clickhouse-consumer --describe

-- Row count per coin
SELECT coin, count() AS rows, max(event_ts) AS last_seen
FROM crypto_prices_raw GROUP BY coin ORDER BY coin;

-- Latest 1-min candles for BTC
SELECT * FROM prices_1min WHERE coin = 'bitcoin'
ORDER BY window_start DESC LIMIT 10;

-- Latest 5-min candles
SELECT * FROM prices_5min ORDER BY window_start DESC LIMIT 20;

-- Anomalies detected
SELECT * FROM price_anomalies ORDER BY detected_at DESC LIMIT 10;
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `POLL_INTERVAL_SEC` | `3` | Seconds between CoinGecko polls |
| `COINS` | `bitcoin,ethereum,solana,binancecoin` | Comma-separated coin IDs |
| `Z_SCORE_THRESHOLD` | `2.5` | Anomaly alert trigger threshold |
| `ROLLING_WINDOW` | `30` | Number of ticks in z-score window |
| `BATCH_SIZE` | `50` | Messages per ClickHouse HTTP insert (manual consumer) |
| `FLUSH_INTERVAL_SEC` | `5` | Max seconds between flushes (manual consumer) |

---

## Portfolio Differentiators

1. **ClickHouse native Kafka Engine** — used at Cloudflare, Contentsquare, and high-throughput analytics shops; rarely covered in tutorials
2. **Real-time OHLCV materialisation** — demonstrates streaming aggregation, not just ingestion
3. **Dual-consumer architecture** — analytics + ML anomaly detection on the same Kafka stream
4. **Documented trade-off analysis** — native vs. manual consumer with measurable latency comparison
