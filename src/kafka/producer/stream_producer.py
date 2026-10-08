"""Read live trades from Finnhub WebSocket and publish them to Kafka."""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv
from websockets.sync.client import connect

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not configured")
    return value


def configured_symbols() -> list[str]:
    raw_symbols = os.getenv("STREAM_SYMBOLS")
    return [
        symbol.strip().upper()
        for symbol in raw_symbols.split(",")
        if symbol.strip()
    ]


def delivery_report(error, message) -> None:
    if error:
        logger.error("Kafka delivery failed: %s", error)


def normalize_trade(trade: dict) -> dict:
    """Convert Finnhub's short field names into our raw streaming schema."""
    timestamp_ms = int(trade["t"])
    record = {
        "symbol": trade["s"],
        "price": float(trade["p"]),
        "volume": float(trade["v"]),
        "timestamp": datetime.fromtimestamp(
            timestamp_ms / 1000,
            tz=timezone.utc,
        ).isoformat(),
        "trade_timestamp_ms": timestamp_ms,
        "conditions": trade.get("c", []),
        "source": "finnhub",
    }
    fingerprint = json.dumps(record, sort_keys=True, separators=(",", ":"))
    record["event_id"] = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return record


def stream_trades() -> None:
    api_key = required_env("FINNHUB_API_KEY")
    kafka_servers = required_env("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic = required_env("KAFKA_TOPIC_REALTIME")
    symbols = configured_symbols()

    producer = Producer(
        {
            "bootstrap.servers": kafka_servers,
            "client.id": "finnhub-stream-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    websocket_url = f"wss://ws.finnhub.io?token={api_key}"

    logger.info("Publishing Finnhub trades for %s to %s", symbols, kafka_topic)
    reconnect_delay = 5
    connection_attempt = 0

    while True:
        try:
            connection_attempt += 1
            logger.info(
                "Connecting to Finnhub WebSocket (attempt %s)",
                connection_attempt,
            )
            with connect(
                websocket_url,
                open_timeout=30,
                ping_interval=20,
                ping_timeout=20,
            ) as websocket:
                for symbol in symbols:
                    websocket.send(
                        json.dumps({"type": "subscribe", "symbol": symbol})
                    )
                logger.info("Finnhub WebSocket connected")

                received = 0
                for raw_message in websocket:
                    message = json.loads(raw_message)

                    if message.get("type") != "trade":
                        logger.debug("Finnhub message: %s", message)
                        continue

                    for trade in message.get("data", []):
                        record = normalize_trade(trade)
                        producer.produce(
                            topic=kafka_topic,
                            key=record["symbol"],
                            value=json.dumps(record),
                            callback=delivery_report,
                        )
                        producer.poll(0)
                        received += 1
                        reconnect_delay = 5

                    if received and received % 10 == 0:
                        logger.info("Published %s trades to Kafka", received)

        except KeyboardInterrupt:
            logger.info("Streaming producer stopped")
            break
        except Exception:
            logger.exception(
                "WebSocket disconnected; reconnecting in %s seconds",
                reconnect_delay,
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

    producer.flush()


if __name__ == "__main__":
    stream_trades()
