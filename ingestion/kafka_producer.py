import os
import json
import time
import logging
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")
EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events"
TOPIC_NAME = "nasa-eonet-raw"
BOOTSTRAP_SERVERS = [
    "192.168.56.10:9092",
    "192.168.56.11:9092",
    "192.168.56.12:9092"
]


def create_producer_with_backoff():
    """Connects to the 3-node Kafka cluster with exponential backoff."""
    delay = 2
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",                  # Wait for all in-sync replicas
                retries=5,                   # Built-in transient send retries
                retry_backoff_ms=500
            )
            logging.info("Connected to Kafka cluster.")
            return producer
        except NoBrokersAvailable:
            logging.warning(f"Kafka brokers unavailable. Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 60)


def fetch_and_produce(producer):
    params = {
        "status": "open",
        "limit": 100,
        "api_key": NASA_API_KEY
    }

    try:
        response = requests.get(EONET_URL, params=params, timeout=15)

        remaining = response.headers.get("X-RateLimit-Remaining")
        if remaining is not None:
            logging.info(f"[RateLimit] Requests left in quota: {remaining}")

        if response.status_code == 429:
            logging.warning("[WARNING] Quota reached (HTTP 429). Pausing for 15 min...")
            time.sleep(900)
            return

        response.raise_for_status()
        events = response.json().get("events", [])
        ingested_at = datetime.now(timezone.utc).isoformat()

        published_count = 0
        for event in events:
            # Stamp UTC ingestion time (Step 3)
            event["ingested_at"] = ingested_at

            # Partition key by hazard category (or use event["id"] for strict per-event order)
            categories = event.get("categories", [])
            category_key = categories[0]["id"] if categories else "general"

            producer.send(
                topic=TOPIC_NAME,
                key=category_key,
                value=event
            )
            published_count += 1

        producer.flush()
        logging.info(f"[OK] {published_count} NASA EONET events published to '{TOPIC_NAME}'.")

    except requests.exceptions.RequestException as e:
        logging.error(f"[API Error] EONET request failed: {e}")
    except KafkaError as e:
        logging.error(f"[Kafka Error] Failed to publish batch: {e}")
        raise


if __name__ == "__main__":
    POLL_INTERVAL = 1800 if NASA_API_KEY == "DEMO_KEY" else 300
    logging.info(f"--- Starting Kafka Ingestion (Interval: {POLL_INTERVAL}s) ---")

    producer = create_producer_with_backoff()
    backoff = 5

    while True:
        try:
            fetch_and_produce(producer)
            backoff = 5  # Reset backoff on success
            time.sleep(POLL_INTERVAL)
        except KafkaError:
            logging.warning(f"Reconnecting producer in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)
            producer = create_producer_with_backoff()
