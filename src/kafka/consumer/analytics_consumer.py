"""Store Kafka trades in PostgreSQL and expose one-minute metrics."""

import hashlib
import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv
from psycopg2.extras import Json

from stream_data_consumer import validate_trade

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("ANALYTICS_BATCH_SIZE", "100"))
FLUSH_INTERVAL = int(os.getenv("ANALYTICS_FLUSH_INTERVAL_SECONDS", "5"))


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not configured")
    return value


def create_connection():
    return psycopg2.connect(
        host=required_env("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=required_env("POSTGRES_DB"),
        user=required_env("POSTGRES_USER"),
        password=required_env("POSTGRES_PASSWORD"),
    )


def create_schema(connection) -> None:
    """Create the trade table and live one-minute metrics view."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                event_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                trade_time TIMESTAMPTZ NOT NULL,
                trade_timestamp_ms BIGINT NOT NULL,
                conditions JSONB NOT NULL DEFAULT '[]'::jsonb,
                source TEXT NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
                ON trades (symbol, trade_time DESC);

            CREATE OR REPLACE VIEW one_minute_metrics AS
            WITH minute_bars AS (
                SELECT
                    symbol,
                    date_trunc('minute', trade_time) AS minute,
                    (array_agg(price ORDER BY trade_time, event_id))[1]
                        AS open_price,
                    MAX(price) AS high_price,
                    MIN(price) AS low_price,
                    (array_agg(price ORDER BY trade_time DESC, event_id DESC))[1]
                        AS close_price,
                    SUM(volume) AS volume,
                    COUNT(*) AS trade_count,
                    SUM(price * volume) / NULLIF(SUM(volume), 0) AS vwap
                FROM trades
                GROUP BY symbol, date_trunc('minute', trade_time)
            )
            SELECT
                symbol,
                minute,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                trade_count,
                vwap,
                close_price - open_price AS price_change,
                AVG(close_price) OVER (
                    PARTITION BY symbol
                    ORDER BY minute
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS moving_average_5,
                AVG(close_price) OVER (
                    PARTITION BY symbol
                    ORDER BY minute
                    ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
                ) AS moving_average_15,
                STDDEV_SAMP(close_price) OVER (
                    PARTITION BY symbol
                    ORDER BY minute
                    ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
                ) AS rolling_volatility_15
            FROM minute_bars;
            """
        )
    connection.commit()


def event_id_for(trade: dict) -> str:
    """Use the producer ID or derive one for older Kafka records."""
    if trade.get("event_id"):
        return trade["event_id"]

    fingerprint_fields = {
        key: trade[key]
        for key in (
            "symbol",
            "price",
            "volume",
            "timestamp",
            "trade_timestamp_ms",
            "conditions",
            "source",
        )
        if key in trade
    }
    fingerprint = json.dumps(
        fingerprint_fields,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()


def store_trades(connection, trades: list[dict]) -> tuple[int, int]:
    """Insert new trades and ignore duplicate event IDs."""
    inserted = 0
    with connection.cursor() as cursor:
        for trade in trades:
            cursor.execute(
                """
                INSERT INTO trades (
                    event_id,
                    symbol,
                    price,
                    volume,
                    trade_time,
                    trade_timestamp_ms,
                    conditions,
                    source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event_id_for(trade),
                    trade["symbol"],
                    trade["price"],
                    trade["volume"],
                    trade["timestamp"],
                    trade["trade_timestamp_ms"],
                    Json(trade.get("conditions", [])),
                    trade["source"],
                ),
            )
            inserted += cursor.rowcount

    connection.commit()
    return inserted, len(trades) - inserted


def consume_trades() -> None:
    connection = create_connection()
    create_schema(connection)

    consumer = Consumer(
        {
            "bootstrap.servers": required_env("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": os.getenv(
                "KAFKA_GROUP_ANALYTICS_ID",
                "stock-market-analytics-group",
            ),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([required_env("KAFKA_TOPIC_REALTIME")])

    trades = []
    has_messages = False
    last_flush = time.time()
    logger.info("Analytics consumer started")

    try:
        while True:
            kafka_message = consumer.poll(timeout=1.0)

            if kafka_message is not None:
                if kafka_message.error():
                    logger.error("Kafka consumer error: %s", kafka_message.error())
                else:
                    has_messages = True
                    try:
                        message = json.loads(
                            kafka_message.value().decode("utf-8")
                        )
                        trades.append(validate_trade(message))
                    except (
                        UnicodeDecodeError,
                        json.JSONDecodeError,
                        ValueError,
                    ) as error:
                        logger.warning("Skipping invalid trade: %s", error)

            batch_full = len(trades) >= BATCH_SIZE
            flush_due = time.time() - last_flush >= FLUSH_INTERVAL

            if has_messages and (batch_full or flush_due):
                inserted, duplicates = store_trades(connection, trades)
                consumer.commit(asynchronous=False)
                logger.info(
                    "Stored %s trades; ignored %s duplicates",
                    inserted,
                    duplicates,
                )
                trades = []
                has_messages = False
                last_flush = time.time()

    except KeyboardInterrupt:
        logger.info("Analytics consumer stopped")
    finally:
        if has_messages:
            inserted, duplicates = store_trades(connection, trades)
            consumer.commit(asynchronous=False)
            logger.info(
                "Stored %s trades; ignored %s duplicates",
                inserted,
                duplicates,
            )
        consumer.close()
        connection.close()


if __name__ == "__main__":
    consume_trades()
