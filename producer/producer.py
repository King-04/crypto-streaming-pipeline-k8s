"""
Crypto Price Producer

Fetches live cryptocurrency prices from the CoinGecko API and publishes
each price tick as a JSON message to a Kafka topic.

Configuration is read entirely from environment variables so the same image
runs unchanged in Docker Compose, Minikube, and production Kubernetes.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Configuration (env-driven for 12-factor compliance)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")
COINGECKO_URL = os.getenv(
    "COINGECKO_URL",
    "https://api.coingecko.com/api/v3/simple/price",
)
COINS = os.getenv("COINS", "bitcoin,ethereum,solana,cardano,ripple")
VS_CURRENCY = os.getenv("VS_CURRENCY", "usd")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("producer")


def build_producer(bootstrap: str, retries: int = 10) -> KafkaProducer:
    """Connect to Kafka with retry — broker may not be ready on first boot."""
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8") if v else None,
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable:
            wait = min(2 ** attempt, 30)
            log.warning(
                "Kafka not ready (attempt %s/%s). Retrying in %ss...",
                attempt, retries, wait,
            )
            time.sleep(wait)
    log.error("Could not connect to Kafka after %s attempts. Exiting.", retries)
    sys.exit(1)


def fetch_prices() -> dict | None:
    """Hit CoinGecko for the latest prices. Returns None on failure."""
    params = {
        "ids": COINS,
        "vs_currencies": VS_CURRENCY,
        "include_24hr_change": "true",
        "include_24hr_vol": "true",
    }
    try:
        resp = requests.get(COINGECKO_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        log.error("CoinGecko request failed: %s", exc)
        return None


def to_messages(payload: dict) -> list[dict]:
    """Flatten CoinGecko's nested response into one record per coin."""
    now = datetime.now(timezone.utc).isoformat()
    messages = []
    for coin, data in payload.items():
        messages.append({
            "coin": coin,
            "currency": VS_CURRENCY,
            "price": data.get(VS_CURRENCY),
            "change_24h_pct": data.get(f"{VS_CURRENCY}_24h_change"),
            "volume_24h": data.get(f"{VS_CURRENCY}_24h_vol"),
            "timestamp": now,
        })
    return messages


def main() -> None:
    log.info("Starting producer | topic=%s | coins=%s", KAFKA_TOPIC, COINS)
    producer = build_producer(KAFKA_BOOTSTRAP)

    while True:
        payload = fetch_prices()
        if payload:
            for msg in to_messages(payload):
                producer.send(KAFKA_TOPIC, key=msg["coin"], value=msg)
                log.info("Sent %s @ %s %s",
                         msg["coin"], msg["price"], msg["currency"].upper())
            producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Producer stopped by user.")
