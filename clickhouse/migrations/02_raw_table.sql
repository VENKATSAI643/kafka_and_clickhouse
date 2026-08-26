-- ────────────────────────────────────────────────────────────────────────────
--  Migration 02 — Raw Storage Table (MergeTree)
--  Run order: SECOND (before the materialised view in 03)
-- ────────────────────────────────────────────────────────────────────────────
--
--  This is the actual durable storage for every price tick.
--  The MV in 03_mv_kafka_to_raw.sql will INSERT rows here automatically.
--
--  Design decisions:
--    - PARTITION BY toYYYYMMDD(event_ts)  → daily partitions for cheap TTL drops
--    - ORDER BY (coin, event_ts)          → fast range scans per coin over time
--    - TTL clause (commented out)         → uncomment to auto-expire data after 30d
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS crypto_prices_raw
(
    coin        String,
    price_usd   Float64,
    change_24h  Float64,
    event_ts    DateTime,
    ingested_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (coin, event_ts)
-- TTL event_ts + INTERVAL 30 DAY DELETE  -- uncomment to auto-expire after 30 days
SETTINGS
    index_granularity = 8192;
