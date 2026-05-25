"""
Crypto Price Consumer

Subscribes to the Kafka topic, persists every tick to PostgreSQL, and
computes rolling analytics (per-coin moving average and volatility over
the last N ticks) which are upserted into an analytics table.

This consumer is horizontally scalable: running multiple replicas in the
same Kafka consumer group automatically partitions the workload.
"""

import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict, deque
from statistics import mean, pstdev

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "crypto-consumer")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crypto")
PG_USER = os.getenv("PG_USER", "crypto")
PG_PASSWORD = os.getenv("PG_PASSWORD", "crypto")

# Rolling-window size for analytics (number of recent ticks per coin)
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "20"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("consumer")

# In-memory rolling window per coin (resets if pod restarts — analytics
# table acts as the durable view of recent state)
windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS price_ticks (
    id           BIGSERIAL PRIMARY KEY,
    coin         TEXT        NOT NULL,
    currency     TEXT        NOT NULL,
    price        NUMERIC,
    change_24h   NUMERIC,
    volume_24h   NUMERIC,
    ts           TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_price_ticks_coin_ts
    ON price_ticks (coin, ts DESC);

CREATE TABLE IF NOT EXISTS price_analytics (
    coin          TEXT PRIMARY KEY,
    last_price    NUMERIC,
    moving_avg    NUMERIC,
    volatility    NUMERIC,
    window_size   INTEGER,
    updated_at    TIMESTAMPTZ NOT NULL
);
"""


def connect_postgres(retries: int = 15):
    """Connect to Postgres with retry — DB may be initializing."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
            )
            conn.autocommit = True
            log.info("Connected to Postgres at %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
            return conn
        except psycopg2.OperationalError as exc:
            wait = min(2 ** attempt, 30)
            log.warning("Postgres not ready (attempt %s/%s): %s — retry in %ss",
                        attempt, retries, exc, wait)
            time.sleep(wait)
    log.error("Could not connect to Postgres. Exiting.")
    sys.exit(1)


def connect_kafka(retries: int = 10) -> KafkaConsumer:
    """Connect to Kafka with retry."""
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            wait = min(2 ** attempt, 30)
            log.warning("Kafka not ready (attempt %s/%s) — retry in %ss",
                        attempt, retries, wait)
            time.sleep(wait)
    log.error("Could not connect to Kafka. Exiting.")
    sys.exit(1)


def init_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    log.info("Schema initialised.")


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------
def persist_tick(conn, msg: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO price_ticks (coin, currency, price, change_24h, volume_24h, ts)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                msg["coin"], msg["currency"], msg.get("price"),
                msg.get("change_24h_pct"), msg.get("volume_24h"),
                msg["timestamp"],
            ),
        )


def update_analytics(conn, coin: str, latest_price: float, ts: str) -> None:
    window = windows[coin]
    window.append(latest_price)

    if len(window) < 2:
        return  # need at least 2 points for volatility

    mavg = mean(window)
    vol = pstdev(window)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO price_analytics
                (coin, last_price, moving_avg, volatility, window_size, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (coin) DO UPDATE SET
                last_price  = EXCLUDED.last_price,
                moving_avg  = EXCLUDED.moving_avg,
                volatility  = EXCLUDED.volatility,
                window_size = EXCLUDED.window_size,
                updated_at  = EXCLUDED.updated_at;
            """,
            (coin, latest_price, mavg, vol, len(window), ts),
        )


def handle_message(conn, msg: dict) -> None:
    if msg.get("price") is None:
        log.warning("Skipping message with no price: %s", msg)
        return
    persist_tick(conn, msg)
    update_analytics(conn, msg["coin"], float(msg["price"]), msg["timestamp"])
    log.info("Processed %-10s price=%-12s mavg-window=%d",
             msg["coin"], msg["price"], len(windows[msg["coin"]]))


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Starting consumer | topic=%s | group=%s", KAFKA_TOPIC, KAFKA_GROUP)
    conn = connect_postgres()
    init_schema(conn)
    consumer = connect_kafka()

    # Graceful shutdown
    def shutdown(*_):
        log.info("Shutdown signal received, closing connections...")
        consumer.close()
        conn.close()
        sys.exit(0)
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    for record in consumer:
        try:
            handle_message(conn, record.value)
        except Exception as exc:
            log.exception("Error processing message: %s", exc)


if __name__ == "__main__":
    main()
