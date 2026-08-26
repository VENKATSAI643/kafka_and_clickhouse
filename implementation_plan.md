# Project 4 — Kafka → ClickHouse Real-Time Pipeline
## In-Depth Implementation Plan

---

## 1. Project Goal

Build a **production-grade real-time streaming analytics pipeline** that:
1. Ingests live cryptocurrency price data from CoinGecko every few seconds
2. Publishes that data to an Apache Kafka topic
3. Consumes it natively inside ClickHouse via the **Kafka Table Engine**
4. Aggregates it with **Materialized Views** into rolling 1-min / 5-min OHLCV windows
5. Runs a downstream **anomaly detector** (z-score on rolling price) as a second consumer
6. Compares the **native Kafka engine** approach vs. a **manual Python consumer** approach

---

## 2. Source Data — CoinGecko API

### Why CoinGecko?
- **Free tier** — no API key required for basic endpoints
- Returns real prices with meaningful volatility (good for anomaly testing)
- Rate limit: ~30 calls/min on the free tier (1 call every ~2 sec is safe)

### Endpoint Used
```
GET https://api.coingecko.com/api/v3/simple/price
  ?ids=bitcoin,ethereum,solana,binancecoin
  &vs_currencies=usd
  &include_24hr_change=true
  &include_last_updated_at=true
```

### Sample API Response
```json
{
  "bitcoin":      { "usd": 61432.00, "usd_24h_change": 1.23, "last_updated_at": 1724421000 },
  "ethereum":     { "usd": 3120.50,  "usd_24h_change": -0.45, "last_updated_at": 1724421000 },
  "solana":       { "usd": 142.10,   "usd_24h_change": 2.10, "last_updated_at": 1724421000 },
  "binancecoin":  { "usd": 570.30,   "usd_24h_change": 0.78, "last_updated_at": 1724421000 }
}
```

### Kafka Message Schema (JSON per message)
Each poll flattens into **one Kafka message per coin**:
```json
{
  "coin":         "bitcoin",
  "price_usd":   61432.00,
  "change_24h":  1.23,
  "event_ts":    "2024-08-23T14:10:00Z",
  "ingested_at": "2024-08-23T14:10:01.123Z"
}
```
> This schema is deliberately simple — one record per coin per poll — making it easy to reason about aggregations downstream.

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Compose Network                        │
│                                                                       │
│  ┌──────────────┐     ┌──────────────────────────────────────────┐   │
│  │  Python      │     │            Apache Kafka                   │   │
│  │  Producer    │────▶│  Topic: crypto_prices                    │   │
│  │  (polls      │     │  Partitions: 4  Replication: 1           │   │
│  │  CoinGecko   │     │  Retention: 24h                          │   │
│  │  every 3s)   │     └──────────────────────────────────────────┘   │
│  └──────────────┘              │                  │                   │
│                                │                  │                   │
│                    ┌───────────▼──────┐  ┌────────▼──────────────┐   │
│                    │  ClickHouse      │  │  Python Anomaly        │   │
│                    │  Kafka Engine    │  │  Consumer              │   │
│                    │  (native ingest) │  │  (z-score detector)    │   │
│                    └───────────┬──────┘  └────────────────────────┘   │
│                                │                                       │
│                    ┌───────────▼──────────────────┐                   │
│                    │  ClickHouse Tables            │                   │
│                    │                               │                   │
│                    │  kafka_raw_prices  (engine)   │                   │
│                    │  crypto_prices_raw (MergeTree)│                   │
│                    │  prices_1min_mv    (Mat.View) │                   │
│                    │  prices_5min_mv    (Mat.View) │                   │
│                    └───────────────────────────────┘                   │
│                                                                         │
│  ┌────────────┐                                                         │
│  │ Zookeeper  │ (required by Kafka for broker coordination)             │
│  └────────────┘                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Role | Tech |
|---|---|---|
| **Python Producer** | Polls CoinGecko, publishes JSON to Kafka | `confluent-kafka` / `kafka-python` |
| **Kafka Broker** | Durable message buffer, topic management | Apache Kafka 3.x |
| **Zookeeper** | Kafka cluster metadata (classic mode) | Apache Zookeeper 3.x |
| **ClickHouse Kafka Engine** | Native consumer — reads from topic, no external process | ClickHouse 23.x+ |
| **Materialized Views** | Continuous incremental aggregation | ClickHouse SQL |
| **Python Anomaly Consumer** | Independent consumer group, z-score alerting | `confluent-kafka` + `statistics` |
| **ClickHouse HTTP Interface** | Query layer for verification / dashboards | Port 8123 |

---

## 4. Infrastructure — Docker Compose

### Services & Resource Budget

| Service | Image | RAM Estimate | Ports |
|---|---|---|---|
| Zookeeper | `confluentinc/cp-zookeeper:7.6` | ~256 MB | 2181 |
| Kafka Broker | `confluentinc/cp-kafka:7.6` | ~512 MB | 9092, 29092 |
| ClickHouse | `clickhouse/clickhouse-server:23.8` | ~512 MB | 8123, 9000 |
| Python Producer | Custom Dockerfile | ~64 MB | — |
| Python Anomaly | Custom Dockerfile | ~64 MB | — |
| **Total** | | **~1.4 GB** | |

> ⚠️ Stop Spark/Airflow containers before starting. This stack needs ~1.5 GB free RAM.

### docker-compose.yml Structure
```yaml
version: "3.8"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.6
    depends_on: [zookeeper]
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_NUM_PARTITIONS: 4
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_LOG_RETENTION_HOURS: 24

  clickhouse:
    image: clickhouse/clickhouse-server:23.8
    volumes:
      - ./clickhouse/config:/etc/clickhouse-server
      - clickhouse_data:/var/lib/clickhouse
    ports:
      - "8123:8123"   # HTTP
      - "9000:9000"   # Native TCP

  producer:
    build: ./producer
    depends_on: [kafka]
    environment:
      KAFKA_BROKER: kafka:9092
      POLL_INTERVAL_SEC: 3

  anomaly_consumer:
    build: ./anomaly
    depends_on: [kafka]
    environment:
      KAFKA_BROKER: kafka:9092
      CONSUMER_GROUP: anomaly-detector
```

---

## 5. Kafka Topic Design

```
Topic Name:   crypto_prices
Partitions:   4  (one per coin: BTC, ETH, SOL, BNB — use coin name as key)
Replication:  1  (single broker, local dev)
Retention:    24 hours
Cleanup:      delete (not compacted — we want time-series, not latest-value)
```

**Why 4 partitions?**  
Using the coin name as the **Kafka message key** ensures all messages for a given coin land in the same partition → ordering guarantees per coin, easier per-coin aggregation.

---

## 6. Python Producer

### Logic
```
Loop every POLL_INTERVAL_SEC:
  1. GET CoinGecko API
  2. For each coin in response:
       - Build message dict
       - Serialize to JSON bytes
       - Produce to Kafka with key=coin_name
  3. Sleep until next poll
```

### Key Libraries
```
confluent-kafka==2.3.0   # preferred — C-based, faster
requests==2.31.0
python-dotenv
```

### Error Handling
- Retry on HTTP 429 (rate limited) with exponential backoff
- Reconnect on Kafka broker unavailability
- Dead-letter: log failed messages to `failed_messages.jsonl`

---

## 7. ClickHouse Schema — Full DDL

### Step 1: Kafka Engine Table (Read-Through Layer)
```sql
CREATE TABLE kafka_raw_prices
(
    coin        String,
    price_usd   Float64,
    change_24h  Float64,
    event_ts    DateTime,
    ingested_at DateTime64(3)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list     = 'kafka:9092',
    kafka_topic_list      = 'crypto_prices',
    kafka_group_name      = 'clickhouse-consumer',
    kafka_format          = 'JSONEachRow',
    kafka_num_consumers   = 2,
    kafka_skip_broken_messages = 10;
```
> **How it works**: This is NOT a real storage table. It acts as a **virtual stream cursor**. Every SELECT from it consumes messages from Kafka — but you never query it directly. Materialized Views do the consuming.

### Step 2: Raw Storage Table (MergeTree)
```sql
CREATE TABLE crypto_prices_raw
(
    coin        String,
    price_usd   Float64,
    change_24h  Float64,
    event_ts    DateTime,
    ingested_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (coin, event_ts);
```

### Step 3: Materialized View — Wire Kafka → Raw Storage
```sql
CREATE MATERIALIZED VIEW mv_kafka_to_raw TO crypto_prices_raw AS
SELECT
    coin,
    price_usd,
    change_24h,
    event_ts,
    ingested_at
FROM kafka_raw_prices;
```
> This MV **continuously pulls** from `kafka_raw_prices` and inserts into `crypto_prices_raw`. It runs inside ClickHouse — no external process needed.

### Step 4: 1-Minute OHLCV Aggregation
```sql
CREATE TABLE prices_1min
(
    coin        String,
    window_start DateTime,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    avg_price   Float64,
    tick_count  UInt32
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (coin, window_start);

CREATE MATERIALIZED VIEW mv_1min TO prices_1min AS
SELECT
    coin,
    toStartOfMinute(event_ts)        AS window_start,
    argMin(price_usd, event_ts)      AS open,
    max(price_usd)                   AS high,
    min(price_usd)                   AS low,
    argMax(price_usd, event_ts)      AS close,
    avg(price_usd)                   AS avg_price,
    count()                          AS tick_count
FROM crypto_prices_raw
GROUP BY coin, window_start;
```

### Step 5: 5-Minute OHLCV Aggregation
```sql
CREATE MATERIALIZED VIEW mv_5min
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (coin, window_start)
AS
SELECT
    coin,
    toStartOfFiveMinutes(event_ts)   AS window_start,
    argMin(price_usd, event_ts)      AS open,
    max(price_usd)                   AS high,
    min(price_usd)                   AS low,
    argMax(price_usd, event_ts)      AS close,
    avg(price_usd)                   AS avg_price,
    count()                          AS tick_count
FROM crypto_prices_raw
GROUP BY coin, window_start;
```

---

## 8. Approach Comparison: Native Engine vs. Manual Python Consumer

| Dimension | ClickHouse Kafka Engine (Native) | Manual Python Consumer |
|---|---|---|
| **Complexity** | Low — pure SQL, no extra process | Medium — separate service, connection mgmt |
| **Throughput** | Very high (C++ internals, batch inserts) | Moderate (depends on batch tuning) |
| **Latency** | ~1–2 sec (ClickHouse polls internally) | Configurable — can be sub-second |
| **Error handling** | Limited (skip broken msgs, no DLQ) | Full control (DLQ, retry, alerting) |
| **Schema evolution** | Requires DDL change + consumer restart | Can deserialize flexibly in Python |
| **Offset management** | Managed by ClickHouse (opaque) | Managed by Kafka consumer group (visible) |
| **Monitoring** | Limited visibility | Full control — expose metrics |
| **Best for** | Bulk ingestion, simple schemas | Complex ETL, multi-step transforms |

> **Your project uses both** — native engine for primary ingestion, Python consumer for anomaly detection — giving you firsthand experience with both patterns.

---

## 9. Anomaly Detection — Python Consumer

### Algorithm: Rolling Z-Score
```
For each incoming price tick:
  1. Maintain a deque of last N=30 prices for that coin
  2. Compute mean μ and std σ over the window
  3. z = (current_price - μ) / σ
  4. If |z| > threshold (e.g. 2.5) → ANOMALY ALERT
```

### Consumer Group
- Group ID: `anomaly-detector` (independent from ClickHouse consumer group)
- Reads from the **same Kafka topic** — Kafka fan-out handles this
- Offset: `earliest` on first start, then committed

### Output
- Print alerts to stdout: `[ALERT] BTC price spike: $65432 (z=3.2)`
- Optionally write anomalies to ClickHouse table `price_anomalies` via HTTP insert

---

## 10. Project File Structure

```
kafka-clickhouse-pipeline/
├── docker-compose.yml
├── .env
├── README.md
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
│
├── anomaly/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── anomaly_consumer.py
│
├── clickhouse/
│   ├── config/
│   │   └── users.xml
│   └── migrations/
│       ├── 01_kafka_engine.sql
│       ├── 02_raw_table.sql
│       ├── 03_mv_kafka_to_raw.sql
│       ├── 04_1min_aggregation.sql
│       └── 05_5min_aggregation.sql
│
└── notebooks/
    └── analysis.ipynb   # Query ClickHouse, visualize OHLCV, compare approaches
```

---

## 11. Phased Execution Plan

### Phase 1 — Infrastructure (Day 1–2)
- [ ] Write `docker-compose.yml` with Zookeeper, Kafka, ClickHouse
- [ ] Bring up stack, verify Kafka broker is reachable
- [ ] Create `crypto_prices` topic via `kafka-topics.sh`
- [ ] Verify ClickHouse HTTP interface at `localhost:8123`

### Phase 2 — Producer (Day 2–3)
- [ ] Write `producer.py` — poll CoinGecko, serialize JSON, publish to Kafka
- [ ] Test with `kafka-console-consumer` to confirm messages are flowing
- [ ] Add retry logic and error handling

### Phase 3 — ClickHouse Native Ingestion (Day 4–6)
- [ ] Run SQL migrations (01 → 03)
- [ ] Verify `crypto_prices_raw` is filling up in real time
- [ ] Query raw table, confirm data looks correct

### Phase 4 — Aggregations (Day 6–7)
- [ ] Run migrations 04 and 05
- [ ] Wait 5+ minutes, query `prices_1min` and `prices_5min`
- [ ] Verify OHLCV calculations are correct

### Phase 5 — Anomaly Consumer (Day 8–9)
- [ ] Write `anomaly_consumer.py` with rolling z-score logic
- [ ] Containerize it, add to `docker-compose.yml`
- [ ] Verify it reads from Kafka independently (separate consumer group)
- [ ] Trigger a fake spike (edit producer temporarily) and confirm alert fires

### Phase 6 — Manual Python Consumer Comparison (Day 10)
- [ ] Write `manual_consumer.py` — Python reads from Kafka, batch-inserts to ClickHouse
- [ ] Compare ingestion latency, throughput, and code complexity
- [ ] Document trade-offs (add to README)

### Phase 7 — Analysis & Portfolio Polish (Day 11–14)
- [ ] Jupyter notebook: plot OHLCV candlestick charts from ClickHouse data
- [ ] Write README with architecture diagram, setup steps, key learnings
- [ ] Record a short demo video of the live pipeline

---

## 12. Key Concepts to Understand Before Starting

| Concept | Why It Matters |
|---|---|
| **Kafka Consumer Groups** | Two groups (ClickHouse + anomaly) consume the same topic independently — critical to understand offset management |
| **ClickHouse MergeTree Family** | The foundation of all ClickHouse storage; `AggregatingMergeTree` is needed for correct incremental aggregation |
| **Materialized Views in ClickHouse** | Unlike PostgreSQL MVs (static snapshots), ClickHouse MVs are **triggers** — they fire on every INSERT |
| **Kafka Engine Internals** | ClickHouse polls Kafka in background threads; number of threads = `kafka_num_consumers` |
| **Exactly-Once vs At-Least-Once** | Kafka Engine gives **at-least-once** — duplicate handling must be done at query time or via dedup tables |
| **Partition Keys** | Partitioning by date + ordering by `(coin, event_ts)` enables fast range scans per coin |
| **argMin / argMax** | ClickHouse aggregate functions to get the first/last value in a group — essential for OHLCV open/close |

---

## 13. Gotchas & Known Issues

> [!WARNING]
> **ClickHouse Kafka Engine offset reset**: If you restart ClickHouse, it resumes from the last committed Kafka offset — but if you `DROP` and recreate the Kafka engine table, offsets reset to `latest`. You'll lose messages produced during downtime.

> [!WARNING]
> **Materialized Views only trigger on new inserts** — they do NOT backfill historical data. Your aggregation tables will only contain data from the moment you created the MV onward.

> [!CAUTION]
> **Never SELECT directly from the Kafka Engine table in production** — each SELECT consumes and commits Kafka offsets, potentially losing data that the Materialized View hasn't processed yet.

> [!NOTE]
> **CoinGecko `last_updated_at` is in Unix timestamp (integer)** — convert it in the producer before publishing: `datetime.utcfromtimestamp(ts).isoformat()`.

> [!NOTE]
> **ClickHouse DateTime vs DateTime64** — use `DateTime` for second-precision event timestamps, `DateTime64(3)` for millisecond ingestion timestamps.

---

## 14. Verification Queries

Once the pipeline is running, use these to confirm correctness:

```sql
-- Count of raw messages per coin (should grow every 3 sec)
SELECT coin, count(), max(event_ts) AS latest
FROM crypto_prices_raw
GROUP BY coin ORDER BY coin;

-- Last 1-min candles for BTC
SELECT * FROM prices_1min
WHERE coin = 'bitcoin'
ORDER BY window_start DESC LIMIT 10;

-- Check for data freshness (lag detection)
SELECT coin,
       now() - max(event_ts) AS lag_seconds
FROM crypto_prices_raw
GROUP BY coin;

-- Consumer group offset lag (run from host, not ClickHouse)
-- kafka-consumer-groups.sh --bootstrap-server localhost:29092
--   --group clickhouse-consumer --describe
```

---

## 15. Portfolio Differentiators

This project stands out because it demonstrates:
1. **ClickHouse's native Kafka engine** — rarely covered, used at Cloudflare, Contentsquare, and other high-throughput analytics shops
2. **Real-time OHLCV materialization** — shows understanding of streaming aggregation, not just ingestion
3. **Dual-consumer architecture** — analytics + ML/anomaly detection on the same stream
4. **Documented trade-off analysis** — native vs. manual consumer comparison shows engineering maturity
