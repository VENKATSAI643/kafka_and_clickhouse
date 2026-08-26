-- ────────────────────────────────────────────────────────────────────────────
--  Migration 05 — 5-Minute OHLCV Aggregation
--  Run order: FIFTH (after 02_raw_table.sql is created)
-- ────────────────────────────────────────────────────────────────────────────
--
--  Same design as 04_1min_aggregation.sql but with 5-minute windows.
--
--  toStartOfFiveMinutes() rounds down to the nearest 5-min boundary:
--    14:07:32 → 14:05:00
--    14:12:00 → 14:10:00
--
--  This MV reads from crypto_prices_raw (NOT from prices_1min).
--  Reading from the raw table avoids compounding rounding errors.
-- ────────────────────────────────────────────────────────────────────────────

-- Storage table for 5-minute OHLCV candles
CREATE TABLE IF NOT EXISTS prices_5min
(
    coin         String,
    window_start DateTime,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    avg_price    Float64,
    tick_count   UInt32
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (coin, window_start)
SETTINGS
    index_granularity = 8192;


-- Trigger: fires on every INSERT into crypto_prices_raw
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_5min
TO prices_5min
AS
SELECT
    coin,
    toStartOfFiveMinutes(event_ts) AS window_start,
    argMin(price_usd, event_ts)    AS open,
    max(price_usd)                 AS high,
    min(price_usd)                 AS low,
    argMax(price_usd, event_ts)    AS close,
    avg(price_usd)                 AS avg_price,
    count()                        AS tick_count
FROM crypto_prices_raw
GROUP BY coin, window_start;
