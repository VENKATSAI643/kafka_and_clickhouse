"""
anomaly_consumer.py — Rolling Z-Score Anomaly Detector
=======================================================
Subscribes to the 'crypto_prices' Kafka topic under an independent consumer
group (separate from ClickHouse).  For each incoming price tick it maintains
a rolling window of the last N prices per coin and computes a z-score.
Alerts are printed to stdout and optionally persisted to a ClickHouse
'price_anomalies' table via the HTTP interface.

Environment variables:
  KAFKA_BROKER          e.g. kafka:9092
  KAFKA_TOPIC           e.g. crypto_prices
  CONSUMER_GROUP        e.g. anomaly-detector
  Z_SCORE_THRESHOLD     float, default 2.5
  ROLLING_WINDOW        int,   default 30   (number of ticks per coin)
  CLICKHOUSE_HOST       e.g. clickhouse
  CLICKHOUSE_PORT       e.g. 8123
  CLICKHOUSE_DB         e.g. default
"""

import json
import logging
import math
import os
import time
from collections import defaultdict, deque
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
logger = logging.getLogger("anomaly")

KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC", "crypto_prices")
CONSUMER_GROUP    = os.getenv("CONSUMER_GROUP", "anomaly-detector")
Z_THRESHOLD       = float(os.getenv("Z_SCORE_THRESHOLD", "2.5"))
ROLLING_WINDOW    = int(os.getenv("ROLLING_WINDOW", "30"))

CLICKHOUSE_HOST   = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT   = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB     = os.getenv("CLICKHOUSE_DB", "default")
CLICKHOUSE_URL    = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"


# ──────────────────────────────────────────────────────────────
#  ClickHouse anomaly table DDL (run once on startup)
# ──────────────────────────────────────────────────────────────
ANOMALY_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS price_anomalies
(
    coin         String,
    price_usd    Float64,
    z_score      Float64,
    window_mean  Float64,
    window_std   Float64,
    event_ts     DateTime64(3),
    detected_at  DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (coin, event_ts)
"""


def ensure_anomaly_table(session: requests.Session) -> None:
    """Create the price_anomalies table in ClickHouse if it doesn't exist.
    Retries up to 10 times with 3-second backoff — ClickHouse may still be
    warming up when this container starts.
    """
    for attempt in range(1, 11):
        try:
            resp = session.post(
                CLICKHOUSE_URL,
                params={"database": CLICKHOUSE_DB},
                data=ANOMALY_TABLE_DDL.encode(),
                timeout=15,
            )
            if resp.status_code == 200:
                logger.info("Anomaly table ready in ClickHouse.")
                return
            else:
                logger.warning("Could not create anomaly table (attempt %d): %s — %s",
                               attempt, resp.status_code, resp.text[:200])
        except requests.exceptions.RequestException as exc:
            logger.warning("ClickHouse unreachable during table setup (attempt %d/10): %s",
                           attempt, exc)
        time.sleep(3)
    logger.error("Gave up waiting for ClickHouse after 10 attempts. "
                 "Anomalies will be detected but NOT persisted.")


# ──────────────────────────────────────────────────────────────
#  Insert anomaly into ClickHouse
# ──────────────────────────────────────────────────────────────
def persist_anomaly(
    session: requests.Session,
    coin: str,
    price: float,
    z: float,
    mean: float,
    std: float,
    event_ts: str,
) -> None:
    """
    POST a single anomaly row to ClickHouse via the HTTP JSONEachRow interface.
    Non-blocking: failures are logged but do not crash the consumer.
    """
    detected_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    row = {
        "coin":        coin,
        "price_usd":   price,
        "z_score":     round(z, 4),
        "window_mean": round(mean, 4),
        "window_std":  round(std, 4),
        "event_ts":    event_ts,
        "detected_at": detected_at,
    }
    query = f"INSERT INTO {CLICKHOUSE_DB}.price_anomalies FORMAT JSONEachRow"
    try:
        resp = session.post(
            CLICKHOUSE_URL,
            params={"query": query},
            data=(json.dumps(row) + "\n").encode(),
            timeout=5,
        )
        if resp.status_code != 200:
            logger.warning("ClickHouse insert failed (%s): %s",
                           resp.status_code, resp.text[:200])
    except requests.exceptions.RequestException as exc:
        logger.warning("Could not persist anomaly to ClickHouse: %s", exc)


# ──────────────────────────────────────────────────────────────
#  Rolling Z-Score engine
# ──────────────────────────────────────────────────────────────
class RollingZScore:
    """
    Maintains a fixed-size rolling window of prices per coin.
    Computes population z-score: z = (x - μ) / σ

    Requires at least `min_samples` observations before alerting,
    to avoid false positives during warm-up.
    """

    def __init__(self, window: int = 30, min_samples: int = 10):
        self.window      = window
        self.min_samples = min_samples
        # coin → deque of float prices
        self._windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))

    def update(self, coin: str, price: float) -> tuple[float | None, float, float]:
        """
        Add a new price observation.
        Returns (z_score_or_None, mean, std).
        z_score is None if there are fewer than min_samples observations.
        """
        buf = self._windows[coin]
        buf.append(price)

        n = len(buf)
        if n < self.min_samples:
            return None, 0.0, 0.0

        mean = sum(buf) / n
        variance = sum((x - mean) ** 2 for x in buf) / n
        std = math.sqrt(variance)

        if std < 1e-9:
            # All prices identical — no anomaly possible
            return 0.0, mean, std

        z = (price - mean) / std
        return z, mean, std

    def window_size(self, coin: str) -> int:
        return len(self._windows[coin])


# ──────────────────────────────────────────────────────────────
#  Kafka Consumer setup
# ──────────────────────────────────────────────────────────────
def create_consumer() -> Consumer:
    """
    Build and return a confluent-kafka Consumer.
    Blocks until the broker is reachable.
    """
    config = {
        "bootstrap.servers":        KAFKA_BROKER,
        "group.id":                 CONSUMER_GROUP,
        "auto.offset.reset":        "earliest",   # read from beginning on first start
        "enable.auto.commit":       True,
        "auto.commit.interval.ms":  5000,
        "session.timeout.ms":       30000,
        "heartbeat.interval.ms":    10000,
    }

    while True:
        try:
            consumer = Consumer(config)
            # Verify broker is reachable
            meta = consumer.list_topics(timeout=10)
            if KAFKA_TOPIC not in meta.topics:
                logger.warning("Topic '%s' not found. Retrying in 10s …", KAFKA_TOPIC)
                consumer.close()
                time.sleep(10)
                continue
            logger.info("Connected to Kafka broker: %s", KAFKA_BROKER)
            logger.info("Consumer group: %s", CONSUMER_GROUP)
            return consumer
        except KafkaException as exc:
            logger.warning("Kafka unavailable: %s. Retrying in 5s …", exc)
            time.sleep(5)


# ──────────────────────────────────────────────────────────────
#  Main consumer loop
# ──────────────────────────────────────────────────────────────
def main():
    logger.info("Starting anomaly consumer")
    logger.info("  Broker:          %s", KAFKA_BROKER)
    logger.info("  Topic:           %s", KAFKA_TOPIC)
    logger.info("  Consumer group:  %s", CONSUMER_GROUP)
    logger.info("  Z threshold:     %.2f", Z_THRESHOLD)
    logger.info("  Rolling window:  %d ticks/coin", ROLLING_WINDOW)

    http_session = requests.Session()
    ensure_anomaly_table(http_session)

    detector = RollingZScore(window=ROLLING_WINDOW, min_samples=10)
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    alert_counts: dict[str, int] = defaultdict(int)
    msg_count    = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition %s/%s",
                                 msg.topic(), msg.partition())
                else:
                    logger.error("Kafka error: %s", msg.error())
                continue

            # ── Parse message ──────────────────────────────────
            try:
                record = json.loads(msg.value().decode("utf-8"))
                coin      = record["coin"]
                price     = float(record["price_usd"])
                event_ts  = record.get("event_ts", "")
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                logger.warning("Malformed message: %s — %s", msg.value(), exc)
                continue

            msg_count += 1

            # ── Compute z-score ────────────────────────────────
            z, mean, std = detector.update(coin, price)

            if z is None:
                # Still warming up the window
                logger.debug("[%s] warming up (%d/%d samples)",
                             coin, detector.window_size(coin), ROLLING_WINDOW)
                continue

            # ── Anomaly check ──────────────────────────────────
            if abs(z) > Z_THRESHOLD:
                direction = "spike ↑" if z > 0 else "drop ↓"
                alert_counts[coin] += 1

                logger.warning(
                    "[ALERT #%d] %s %s  price=$%s  z=%.3f  μ=$%.2f  σ=$%.2f",
                    alert_counts[coin],
                    coin.upper(),
                    direction,
                    f"{price:,.2f}",
                    z,
                    mean,
                    std,
                )

                # Persist to ClickHouse
                persist_anomaly(http_session, coin, price, z, mean, std, event_ts)

            else:
                logger.debug("[%s] $%.2f  z=%.3f  μ=$%.2f  (normal)",
                             coin, price, z, mean)

            if msg_count % 100 == 0:
                logger.info("Processed %d messages | alerts: %s",
                            msg_count, dict(alert_counts))

    except KeyboardInterrupt:
        logger.info("Shutting down anomaly consumer …")
    finally:
        consumer.close()
        logger.info("Consumer closed. Total messages: %d, Total alerts: %d",
                    msg_count, sum(alert_counts.values()))


if __name__ == "__main__":
    main()
