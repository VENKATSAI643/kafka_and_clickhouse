-- ────────────────────────────────────────────────────────────────────────────
--  Migration 04 — 1-Minute OHLCV Aggregation
--  Run order: FOURTH (after 02_raw_table.sql is created)
-- ────────────────────────────────────────────────────────────────────────────
--
--  Architecture:
--    crypto_prices_raw (INSERT) → mv_1min (trigger) → prices_1min (storage)
--
--  AggregatingMergeTree notes:
--    - Used with aggregate state columns for partial aggregations.
--    - Here we use regular aggregation functions (min, max, avg, count, argMin,
--      argMax) which are compatible because the MV inserts complete rows.
--    - ClickHouse will merge partial states in the background.
--
--  argMin(price_usd, event_ts) = price of the FIRST event in the window (OPEN)
--  argMax(price_usd, event_ts) = price of the LAST  event in the window (CLOSE)
-- ────────────────────────────────────────────────────────────────────────────

-- Storage table for 1-minute OHLCV candles
CREATE TABLE IF NOT EXISTS prices_1min
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
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_1min
TO prices_1min
AS
SELECT
    coin,
    toStartOfMinute(event_ts)   AS window_start,
    argMin(price_usd, event_ts) AS open,
    max(price_usd)              AS high,
    min(price_usd)              AS low,
    argMax(price_usd, event_ts) AS close,
    avg(price_usd)              AS avg_price,
    count()                     AS tick_count
FROM crypto_prices_raw
GROUP BY coin, window_start;
