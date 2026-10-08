"""Consume live trades from Kafka and micro-batch them into MinIO."""

import io
import json
import logging
import os
import time
from collections import defaultdict

import pandas as pd
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("STREAM_BATCH_SIZE", "100"))
FLUSH_INTERVAL = int(os.getenv("STREAM_FLUSH_INTERVAL_SECONDS", "60"))


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not configured")
    return value


def create_minio_client() -> Minio:
    return Minio(
        required_env("MINIO_ENDPOINT"),
        access_key=required_env("MINIO_ACCESS_KEY"),
        secret_key=required_env("MINIO_SECRET_KEY"),
        secure=False,
    )


def ensure_bucket_exists(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def validate_trade(message: object) -> dict:
    """Return a valid trade or raise ValueError with the validation failure."""
    if not isinstance(message, dict):
        raise ValueError("message must be a JSON object")

    required_fields = {
        "symbol",
        "price",
        "volume",
        "timestamp",
        "trade_timestamp_ms",
        "source",
    }
    missing_fields = sorted(required_fields - message.keys())
    if missing_fields:
        raise ValueError(f"missing fields: {', '.join(missing_fields)}")
    if not isinstance(message["symbol"], str) or not message["symbol"].strip():
        raise ValueError("symbol must be a non-empty string")
    if not isinstance(message["price"], (int, float)) or message["price"] <= 0:
        raise ValueError("price must be greater than zero")
    if not isinstance(message["volume"], (int, float)) or message["volume"] < 0:
        raise ValueError("volume must be zero or greater")
    if not isinstance(message["timestamp"], str) or not message["timestamp"]:
        raise ValueError("timestamp must be a non-empty string")
    if not isinstance(message["trade_timestamp_ms"], int):
        raise ValueError("trade_timestamp_ms must be an integer")
    if message["source"] != "finnhub":
        raise ValueError("source must be finnhub")

    return message


def send_to_dlq(
    producer: Producer,
    topic: str,
    kafka_message,
    reason: str,
) -> None:
    """Send an invalid Kafka record and its error details to the DLQ."""
    record = {
        "error": reason,
        "source_topic": kafka_message.topic(),
        "source_partition": kafka_message.partition(),
        "source_offset": kafka_message.offset(),
        "original_value": kafka_message.value().decode("utf-8", errors="replace"),
    }
    producer.produce(
        topic=topic,
        key=kafka_message.key(),
        value=json.dumps(record),
    )


def flush_batch(
    client: Minio,
    bucket: str,
    messages: list[dict],
    offsets: dict[int, tuple[int, int]],
) -> None:
    """Write one CSV per symbol/date using Kafka offsets in the object name."""
    offset_span = "_".join(
        f"p{partition}-{first}-{last}"
        for partition, (first, last) in sorted(offsets.items())
    )

    grouped_messages = defaultdict(list)
    for message in messages:
        day = message["timestamp"][:10]
        grouped_messages[(message["symbol"], day)].append(message)

    for (symbol, day), rows in grouped_messages.items():
        object_name = (
            f"raw/realtime/symbol={symbol}/date={day}/"
            f"{symbol}_{offset_span}.csv"
        )
        payload = pd.DataFrame(rows).to_csv(index=False).encode("utf-8")

        client.put_object(
            bucket,
            object_name,
            io.BytesIO(payload),
            length=len(payload),
            content_type="text/csv",
        )
        logger.info("Wrote %s trades to %s", len(rows), object_name)


def consume_trades() -> None:
    bucket = required_env("MINIO_BUCKET")
    client = create_minio_client()
    ensure_bucket_exists(client, bucket)
    kafka_servers = required_env("KAFKA_BOOTSTRAP_SERVERS")
    dlq_topic = required_env("KAFKA_TOPIC_DLQ")

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_servers,
            "group.id": os.getenv(
                "KAFKA_GROUP_REALTIME_ID",
                "stock-market-consumer-group-realtime",
            ),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    dlq_producer = Producer(
        {
            "bootstrap.servers": kafka_servers,
            "client.id": "stock-market-dlq-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    consumer.subscribe([required_env("KAFKA_TOPIC_REALTIME")])

    messages = []
    offsets = {}
    last_flush = time.time()

    logger.info("Streaming consumer started")

    try:
        while True:
            kafka_message = consumer.poll(timeout=1.0)

            if kafka_message is not None:
                if kafka_message.error():
                    logger.error("Kafka consumer error: %s", kafka_message.error())
                else:
                    partition = kafka_message.partition()
                    offset = kafka_message.offset()
                    first_offset = offsets.get(partition, (offset, offset))[0]
                    offsets[partition] = (first_offset, offset)

                    try:
                        parsed_message = json.loads(
                            kafka_message.value().decode("utf-8")
                        )
                        messages.append(validate_trade(parsed_message))
                    except (
                        UnicodeDecodeError,
                        json.JSONDecodeError,
                        ValueError,
                    ) as error:
                        send_to_dlq(
                            dlq_producer,
                            dlq_topic,
                            kafka_message,
                            str(error),
                        )
                        logger.warning(
                            "Sent invalid message at partition %s offset %s to %s: %s",
                            partition,
                            offset,
                            dlq_topic,
                            error,
                        )

            batch_full = len(messages) >= BATCH_SIZE
            flush_due = time.time() - last_flush >= FLUSH_INTERVAL

            if offsets and (batch_full or flush_due):
                if messages:
                    flush_batch(client, bucket, messages, offsets)
                if dlq_producer.flush() != 0:
                    raise RuntimeError("Failed to deliver one or more DLQ messages")
                consumer.commit(asynchronous=False)
                messages = []
                offsets = {}
                last_flush = time.time()

    except KeyboardInterrupt:
        logger.info("Streaming consumer stopped")
    finally:
        if offsets:
            if messages:
                flush_batch(client, bucket, messages, offsets)
            if dlq_producer.flush() != 0:
                raise RuntimeError("Failed to deliver one or more DLQ messages")
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    consume_trades()
