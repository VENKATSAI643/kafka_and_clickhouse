"""
manual_consumer.py — Manual Python → ClickHouse Consumer
==========================================================
PURPOSE (Phase 6 — Comparison study):
  This module demonstrates the ALTERNATIVE approach to ingesting Kafka data
  into ClickHouse.  Instead of using ClickHouse's native Kafka Engine, a
  Python process manually:
    1. Polls messages from Kafka in micro-batches
    2. Buffers them in memory
    3. Batch-inserts into ClickHouse via the HTTP JSONEachRow interface

  This lets you compare the two approaches side-by-side:

  ┌─────────────────────────────┬───────────────────────────────┐
  │  ClickHouse Kafka Engine    │  Manual Python Consumer       │
  ├─────────────────────────────┼───────────────────────────────┤
  │  Zero external process      │  Separate container/process   │
  │  C++ internals, very fast   │  Tunable batch size/interval  │
  │  Opaque offset management   │  Full Kafka offset visibility │
  │  No DLQ / retry             │  DLQ, retry, custom logic     │
  │  Schema: DDL only           │  Schema: Python dict mapping  │
  └─────────────────────────────┴───────────────────────────────┘

Target table: crypto_prices_manual (MergeTree, same schema as crypto_prices_raw)
Consumer group: manual-python-consumer (independent from ClickHouse group)

Environment variables:
  KAFKA_BROKER              e.g. kafka:9092
  KAFKA_TOPIC               e.g. crypto_prices
  MANUAL_CONSUMER_GROUP     e.g. manual-python-consumer
  BATCH_SIZE                int, default 50   (messages per HTTP insert)
  FLUSH_INTERVAL_SEC        int, default 5    (max seconds between flushes)
  CLICKHOUSE_HOST           e.g. clickhouse
  CLICKHOUSE_PORT           e.g. 8123
  CLICKHOUSE_DB             e.g. default
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

# ──────────────────────────────────────────────────────────────
#  Configuration
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("manual_consumer")

KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC", "crypto_prices")
CONSUMER_GROUP    = os.getenv("MANUAL_CONSUMER_GROUP", "manual-python-consumer")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "50"))
FLUSH_INTERVAL    = int(os.getenv("FLUSH_INTERVAL_SEC", "5"))

CLICKHOUSE_HOST   = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT   = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB     = os.getenv("CLICKHOUSE_DB", "default")
CLICKHOUSE_URL    = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"

TARGET_TABLE      = f"{CLICKHOUSE_DB}.crypto_prices_manual"


# ──────────────────────────────────────────────────────────────
#  ClickHouse DDL — target table
# ──────────────────────────────────────────────────────────────
MANUAL_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
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
"""


def ensure_target_table(session: requests.Session) -> None:
    """Create the manual consumer's target table in ClickHouse."""
    try:
        resp = session.post(CLICKHOUSE_URL, data=MANUAL_TABLE_DDL.encode(), timeout=15)
        if resp.status_code == 200:
            logger.info("Target table '%s' is ready.", TARGET_TABLE)
        else:
            logger.warning("DDL warning (%s): %s", resp.status_code, resp.text[:200])
    except requests.exceptions.RequestException as exc:
        logger.warning("ClickHouse unreachable during setup: %s", exc)


# ──────────────────────────────────────────────────────────────
#  Batch insert into ClickHouse
# ──────────────────────────────────────────────────────────────
class ClickHouseBatchInserter:
    """
    Buffers rows in memory and flushes to ClickHouse when either:
      - `max_size` rows have accumulated, OR
      - `flush_interval` seconds have elapsed since the last flush

    Tracks insert latency for comparison with the native Kafka Engine.
    """

    def __init__(
        self,
        session: requests.Session,
        max_size: int = 50,
        flush_interval: int = 5,
    ):
        self.session        = session
        self.max_size       = max_size
        self.flush_interval = flush_interval
        self._buffer: list[dict] = []
        self._last_flush    = time.monotonic()

        # Metrics
        self.total_inserted  = 0
        self.total_batches   = 0
        self.total_errors    = 0
        self.latencies: list[float] = []   # seconds per batch

    def add(self, row: dict) -> None:
        self._buffer.append(row)
        if (
            len(self._buffer) >= self.max_size
            or (time.monotonic() - self._last_flush) >= self.flush_interval
        ):
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        batch      = self._buffer[:]
        self._buffer.clear()
        self._last_flush = time.monotonic()

        ndjson     = "\n".join(json.dumps(row) for row in batch) + "\n"
        query      = f"INSERT INTO {TARGET_TABLE} FORMAT JSONEachRow"
        t_start    = time.monotonic()

        try:
            resp = self.session.post(
                CLICKHOUSE_URL,
                params={"query": query},
                data=ndjson.encode("utf-8"),
                timeout=15,
            )
            elapsed = time.monotonic() - t_start

            if resp.status_code == 200:
                self.total_inserted += len(batch)
                self.total_batches  += 1
                self.latencies.append(elapsed)
                logger.info(
                    "Inserted batch of %d rows in %.3fs | total=%d",
                    len(batch), elapsed, self.total_inserted,
                )
            else:
                self.total_errors += 1
                logger.error("ClickHouse insert failed (%s): %s",
                             resp.status_code, resp.text[:300])
                # Re-add to buffer (simple at-least-once retry)
                self._buffer.extend(batch)

        except requests.exceptions.RequestException as exc:
            self.total_errors += 1
            logger.error("Network error during flush: %s", exc)
            self._buffer.extend(batch)   # retry on next flush

    def stats(self) -> dict:
        avg_lat = (sum(self.latencies) / len(self.latencies)) if self.latencies else 0.0
        return {
            "total_inserted": self.total_inserted,
            "total_batches":  self.total_batches,
            "total_errors":   self.total_errors,
            "avg_latency_ms": round(avg_lat * 1000, 2),
            "min_latency_ms": round(min(self.latencies, default=0) * 1000, 2),
            "max_latency_ms": round(max(self.latencies, default=0) * 1000, 2),
        }


# ──────────────────────────────────────────────────────────────
#  Kafka Consumer setup
# ──────────────────────────────────────────────────────────────
def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers":    KAFKA_BROKER,
        "group.id":             CONSUMER_GROUP,
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,   # manual commit after successful flush
        "session.timeout.ms":   30000,
        "heartbeat.interval.ms": 10000,
    }

    while True:
        try:
            consumer = Consumer(config)
            meta = consumer.list_topics(timeout=10)
            if KAFKA_TOPIC not in meta.topics:
                logger.warning("Topic '%s' not yet available. Retrying …", KAFKA_TOPIC)
                consumer.close()
                time.sleep(10)
                continue
            logger.info("Consumer connected to %s | group=%s", KAFKA_BROKER, CONSUMER_GROUP)
            return consumer
        except KafkaException as exc:
            logger.warning("Kafka unavailable: %s. Retrying in 5s …", exc)
            time.sleep(5)


# ──────────────────────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────────────────────
def main():
    logger.info("Starting manual Python consumer (comparison mode)")
    logger.info("  Broker:          %s", KAFKA_BROKER)
    logger.info("  Topic:           %s", KAFKA_TOPIC)
    logger.info("  Consumer group:  %s", CONSUMER_GROUP)
    logger.info("  Target table:    %s", TARGET_TABLE)
    logger.info("  Batch size:      %d", BATCH_SIZE)
    logger.info("  Flush interval:  %ds", FLUSH_INTERVAL)

    http_session = requests.Session()
    ensure_target_table(http_session)

    inserter = ClickHouseBatchInserter(
        session=http_session,
        max_size=BATCH_SIZE,
        flush_interval=FLUSH_INTERVAL,
    )

    consumer  = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    msg_count  = 0
    parse_errs = 0
    last_stats_log = time.monotonic()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No new message — check if we should flush the buffer on interval
                inserter.flush()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            # ── Parse ──────────────────────────────────────────
            try:
                record = json.loads(msg.value().decode("utf-8"))
                row = {
                    "coin":        str(record["coin"]),
                    "price_usd":   float(record["price_usd"]),
                    "change_24h":  float(record.get("change_24h", 0.0)),
                    "event_ts":    record["event_ts"],
                    "ingested_at": record.get(
                        "ingested_at",
                        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
                    ),
                }
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                parse_errs += 1
                logger.warning("Malformed message (skip #%d): %s", parse_errs, exc)
                continue

            # ── Buffer → batch insert ──────────────────────────
            inserter.add(row)
            msg_count += 1

            # Manual offset commit after adding to buffer
            # (at-least-once: commit after buffer accepts, before flush succeeds)
            consumer.commit(asynchronous=True)

            # ── Periodic stats log ─────────────────────────────
            if time.monotonic() - last_stats_log > 30:
                s = inserter.stats()
                logger.info(
                    "Stats (30s snapshot) | messages=%d | %s",
                    msg_count, s,
                )
                last_stats_log = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Shutting down manual consumer …")
    finally:
        # Final flush
        inserter.flush()
        consumer.close()
        logger.info("Final stats: messages=%d parse_errors=%d | %s",
                    msg_count, parse_errs, inserter.stats())


if __name__ == "__main__":
    main()
