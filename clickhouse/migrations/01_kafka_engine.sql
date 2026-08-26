-- ────────────────────────────────────────────────────────────────────────────
--  Migration 01 — Kafka Engine Table
--  Run order: FIRST (before any other migrations)
-- ────────────────────────────────────────────────────────────────────────────
--
--  This table is a VIRTUAL STREAM CURSOR, NOT a storage table.
--  It wraps the 'crypto_prices' Kafka topic as a ClickHouse table.
--  Data flows: Kafka topic → kafka_raw_prices (in-memory) → MV → MergeTree
--
--  IMPORTANT: Never SELECT from this table in production.
--  Every SELECT consumes and commits Kafka offsets — the MV will miss them.
--
--  kafka_num_consumers = 2 → ClickHouse spawns 2 background threads
--  to read from the 4 Kafka partitions (each thread handles 2 partitions).
--
--  NOTE: event_ts and ingested_at are stored as String here because the
--  producer emits ISO 8601 format (e.g. "2026-08-25T16:50:10Z") which
--  ClickHouse DateTime/DateTime64 cannot parse natively in JSONEachRow.
--  The MV (migration 03) casts them to DateTime using parseDateTime64BestEffort.
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS kafka_raw_prices
(
    coin        String,
    price_usd   Float64,
    change_24h  Float64,
    event_ts    String,
    ingested_at String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:9092',
    kafka_topic_list           = 'crypto_prices',
    kafka_group_name           = 'clickhouse-consumer',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 2,
    kafka_skip_broken_messages = 10,
    kafka_max_block_size       = 65536;
