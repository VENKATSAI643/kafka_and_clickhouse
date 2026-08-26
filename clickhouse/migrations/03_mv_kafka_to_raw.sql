-- ────────────────────────────────────────────────────────────────────────────
--  Migration 03 — Materialized View: Kafka → Raw Storage
--  Run order: THIRD (after 01 and 02)
-- ────────────────────────────────────────────────────────────────────────────
--
--  This MV is the bridge between the Kafka Engine and the MergeTree table.
--
--  How it works:
--    1. ClickHouse's Kafka Engine background threads poll the broker.
--    2. Each batch of messages is presented as a block to INSERT.
--    3. This MV triggers on every such INSERT into kafka_raw_prices.
--    4. It SELECTs from the incoming block and INSERTs into crypto_prices_raw.
--
--  The MV is NOT a query — it's a trigger. No data is stored in the MV itself
--  (it targets crypto_prices_raw via the TO clause).
--
--  WARNING: This MV only processes messages received AFTER it is created.
--  It does NOT backfill historical Kafka messages.
-- ────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_raw
TO crypto_prices_raw
AS
SELECT
    coin,
    price_usd,
    change_24h,
    -- Producer emits ISO 8601 (e.g. "2026-08-25T16:50:10Z");
    -- parseDateTime64BestEffort handles T/Z format correctly.
    toDateTime(parseDateTime64BestEffort(event_ts))    AS event_ts,
    parseDateTime64BestEffort(ingested_at, 3)          AS ingested_at
FROM kafka_raw_prices;
