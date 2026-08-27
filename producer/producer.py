"""
producer.py — CoinGecko → Kafka Producer
=========================================
Polls the CoinGecko Simple Price API every POLL_INTERVAL_SEC seconds,
flattens each coin into a separate JSON message, and publishes to the
'crypto_prices' Kafka topic using the coin name as the partition key.

Environment variables (all optional, have defaults):
  KAFKA_BROKER         Kafka bootstrap server   (default: kafka:9092)
  POLL_INTERVAL_SEC    Seconds between polls    (default: 3)
  COINS                Comma-separated coins    (default: bitcoin,ethereum,solana,binancecoin)

Dead-letter file: failed_messages.jsonl (appended on delivery failure)
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer, KafkaException

# ──────────────────────────────────────────────────────────────
#  Configuration
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("producer")

KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC", "crypto_prices")
# CoinGecko free tier: ~10-30 req/min. Default 10 s ≈ 6 req/min — well under limit.
# Set POLL_INTERVAL_SEC=3 only if you have a CoinGecko Pro API key.
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "10"))
COINS             = os.getenv("COINS", "bitcoin,ethereum,solana,binancecoin").split(",")

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_PARAMS = {
    "ids": ",".join(COINS),
    "vs_currencies": "usd",
    "include_24hr_change": "true",
    "include_last_updated_at": "true",
}

DEAD_LETTER_FILE = "/app/failed_messages.jsonl"

# Retry config for HTTP errors
MAX_HTTP_RETRIES  = 5
HTTP_BACKOFF_BASE = 2   # seconds (exponential: 2, 4, 8, 16, 32)


# ──────────────────────────────────────────────────────────────
#  Kafka delivery report callback
# ──────────────────────────────────────────────────────────────
def delivery_report(err, msg):
    """
    Called by confluent-kafka once per message after broker ack.
    On error, the raw message value is appended to the dead-letter file.
    """
    if err is not None:
        logger.error("Delivery failed: topic=%s key=%s error=%s",
                     msg.topic(), msg.key(), err)
        try:
            with open(DEAD_LETTER_FILE, "a") as dlq:
                dlq.write(msg.value().decode("utf-8") + "\n")
        except OSError as file_err:
            logger.error("Failed to write DLQ: %s", file_err)
    else:
        logger.debug(
            "Delivered: topic=%s partition=%s offset=%s key=%s",
            msg.topic(), msg.partition(), msg.offset(),
            msg.key().decode("utf-8") if msg.key() else None,
        )


# ──────────────────────────────────────────────────────────────
#  CoinGecko fetch with exponential backoff
# ──────────────────────────────────────────────────────────────
def fetch_prices(session: requests.Session) -> dict | None:
    """
    Fetch current prices from CoinGecko.
    Retries on rate-limit (429) or transient errors.
    Returns the parsed JSON dict, or None on permanent failure.
    """
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            response = session.get(
                COINGECKO_URL,
                params=COINGECKO_PARAMS,
                timeout=10,
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                # Rate limited — back off generously
                wait = HTTP_BACKOFF_BASE ** attempt
                logger.warning("Rate limited (429). Retry %d/%d in %ds",
                               attempt, MAX_HTTP_RETRIES, wait)
                time.sleep(wait)
                continue

            logger.error("HTTP %s from CoinGecko. Skipping poll.",
                         response.status_code)
            return None

        except requests.exceptions.ConnectionError as exc:
            wait = HTTP_BACKOFF_BASE ** attempt
            logger.warning("Connection error (attempt %d/%d): %s. Retry in %ds",
                           attempt, MAX_HTTP_RETRIES, exc, wait)
            time.sleep(wait)

        except requests.exceptions.Timeout:
            logger.warning("CoinGecko request timed out (attempt %d/%d)", attempt, MAX_HTTP_RETRIES)
            time.sleep(HTTP_BACKOFF_BASE ** attempt)

    logger.error("CoinGecko fetch failed after %d attempts.", MAX_HTTP_RETRIES)
    return None


# ──────────────────────────────────────────────────────────────
#  Message builder
# ──────────────────────────────────────────────────────────────
def build_messages(api_response: dict) -> list[dict]:
    """
    Flatten the CoinGecko response into one message per coin.

    Input format:
      {
        "bitcoin": {"usd": 61432.00, "usd_24h_change": 1.23, "last_updated_at": 1724421000},
        ...
      }

    Output per message:
      {
        "coin":         "bitcoin",
        "price_usd":   61432.00,
        "change_24h":  1.23,
        "event_ts":    "2024-08-23T14:10:00Z",   ← from CoinGecko last_updated_at
        "ingested_at": "2024-08-23T14:10:01.123Z" ← wall-clock at producer
      }
    """
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    messages = []

    for coin, data in api_response.items():
        try:
            last_updated_ts = data.get("last_updated_at")
            event_ts = (
                datetime.fromtimestamp(last_updated_ts, tz=timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ")
                if last_updated_ts
                else ingested_at
            )

            message = {
                "coin":         coin,
                "price_usd":   float(data["usd"]),
                "change_24h":  float(data.get("usd_24h_change", 0.0)),
                "event_ts":    event_ts,
                "ingested_at": ingested_at,
            }
            messages.append(message)

        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Malformed data for coin '%s': %s. Skipping.", coin, exc)

    return messages


# ──────────────────────────────────────────────────────────────
#  Kafka producer setup
# ──────────────────────────────────────────────────────────────
def create_producer() -> Producer:
    """
    Build and return a confluent-kafka Producer.
    Blocks until the broker is reachable (retries indefinitely).
    """
    config = {
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",                  # wait for all in-sync replicas
        "retries": 5,
        "retry.backoff.ms": 500,
        "linger.ms": 10,               # micro-batch for throughput
        "compression.type": "lz4",
        "enable.idempotence": True,    # exactly-once producer side
    }

    while True:
        try:
            producer = Producer(config)
            # Trigger a metadata fetch to verify broker connectivity
            producer.list_topics(timeout=10)
            logger.info("Connected to Kafka broker: %s", KAFKA_BROKER)
            return producer
        except KafkaException as exc:
            logger.warning("Kafka unavailable: %s. Retrying in 5s …", exc)
            time.sleep(5)


# ──────────────────────────────────────────────────────────────
#  Main loop
# ──────────────────────────────────────────────────────────────
def main():
    logger.info("Starting CoinGecko → Kafka producer")
    logger.info("  Broker:        %s", KAFKA_BROKER)
    logger.info("  Topic:         %s", KAFKA_TOPIC)
    logger.info("  Coins:         %s", COINS)
    logger.info("  Poll interval: %ds", POLL_INTERVAL_SEC)

    producer   = create_producer()
    session    = requests.Session()
    poll_count = 0
    stats      = defaultdict(int)   # {"bitcoin": message_count, ...}
    _last_data: dict | None = None  # cache of last successful CoinGecko response

    try:
        while True:
            loop_start = time.monotonic()

            # 1. Fetch prices from CoinGecko
            data = fetch_prices(session)

            if data is None:
                if _last_data is not None:
                    # ── Rate-limited: publish last known prices rather than
                    #    dropping the cycle. Downstream consumers keep running;
                    #    stale prices are expected during API outages.
                    logger.warning(
                        "[STALE] CoinGecko unavailable — republishing last known prices."
                    )
                    data = _last_data
                else:
                    logger.error("Skipping this poll cycle — no data and no cache yet.")

            if data is not None:
                _last_data = data          # update cache on every successful fetch
                messages = build_messages(data)

                # 2. Publish each coin message
                for msg in messages:
                    payload = json.dumps(msg).encode("utf-8")
                    key     = msg["coin"].encode("utf-8")
                    producer.produce(
                        topic    = KAFKA_TOPIC,
                        key      = key,
                        value    = payload,
                        callback = delivery_report,
                    )
                    stats[msg["coin"]] += 1

                # 3. Flush (trigger delivery callbacks)
                producer.poll(0)
                poll_count += 1

                if poll_count % 10 == 0:
                    logger.info("Poll #%d — message counts: %s",
                                poll_count, dict(stats))

                # Log summary every poll
                prices_summary = {m["coin"]: f"${m['price_usd']:,.2f}" for m in messages}
                logger.info("Published %d messages | prices: %s",
                            len(messages), prices_summary)

            # 4. Sleep for the remainder of the interval
            elapsed = time.monotonic() - loop_start
            sleep_time = max(0, POLL_INTERVAL_SEC - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Shutting down producer …")
    finally:
        # Flush all pending messages before exit
        remaining = producer.flush(timeout=15)
        if remaining > 0:
            logger.warning("%d messages were NOT delivered before shutdown.", remaining)
        logger.info("Producer stopped.")


if __name__ == "__main__":
    main()
