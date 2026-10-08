# Streaming ingestion explained

The streaming path collects live trades continuously.

## Current flow

```text
Finnhub WebSocket → Python producer → Kafka
                                      ├── Raw consumer → MinIO
                                      └── Analytics consumer → PostgreSQL → Streamlit
```

## Finnhub producer

File: `src/kafka/producer/stream_producer.py`

The producer:

1. Opens one authenticated Finnhub WebSocket connection.
2. Subscribes to the symbols in `STREAM_SYMBOLS`.
3. Normalizes Finnhub's abbreviated trade fields.
4. Publishes each trade as JSON to `stock-market-realtime`.
5. Uses the stock symbol as the Kafka message key.
6. Uses an idempotent producer with `acks=all`.
7. Reconnects with a 5-to-60-second exponential backoff after disconnections.

The normalized event contains:

```text
event_id, symbol, price, volume, timestamp, trade_timestamp_ms, conditions, source
```

`event_id` is a deterministic SHA-256 fingerprint of the trade. The same trade
received by multiple producer instances therefore has the same ID.

Using the symbol as the Kafka key keeps trades for one symbol ordered when a
topic has multiple partitions.

## Kafka topics

Kafka topic auto-creation is disabled. `kafka-init` creates:

- `stock-market-realtime`: valid trade input, 3 partitions
- `stock-market-dead-letter`: invalid records, 3 partitions

Both use replication factor 1 because the local environment has one broker.

Producer idempotence prevents Kafka retries from writing the same event more
than once within a producer session. `acks=all` requires acknowledgment from
all in-sync replicas before a send is considered successful. The local cluster
has one replica; a production three-broker cluster would normally use
replication factor 3 and `min.insync.replicas=2`.

## Kafka consumer

File: `src/kafka/consumer/stream_data_consumer.py`

The consumer validates the required fields and their basic data types. Invalid
JSON or invalid trades are written to `stock-market-dead-letter` with the error
and original Kafka location.

The consumer collects a micro-batch until either:

- 100 messages have arrived, or
- 60 seconds have passed.

It then groups trades by symbol and UTC date and writes CSV objects under:

```text
raw/realtime/symbol=AAPL/date=2026-10-07/AAPL_p0-1677-1776.csv
```

The numbers in the filename are Kafka partition and offset boundaries. Replaying
the same offset range generates the same object name, so MinIO overwrites it
instead of creating a duplicate.

## Offset handling

Automatic offset commits are disabled. The consumer commits Kafka offsets only
after all MinIO uploads succeed.

This ordering prevents data loss:

```text
consume → write to MinIO → commit Kafka offset
```

If the consumer stops before committing, Kafka sends those messages again.
Their deterministic object names make that replay safe.

## PostgreSQL analytics

File: `src/kafka/consumer/analytics_consumer.py`

The analytics consumer uses its own Kafka consumer group. It inserts trades
into PostgreSQL using `event_id` as the primary key, so duplicate events are
ignored. Kafka offsets are committed only after the database transaction
succeeds.

The `one_minute_metrics` view provides:

- OHLCV and trade count
- VWAP and price change
- 5-minute and 15-minute moving averages
- 15-minute rolling volatility

## Streamlit dashboard

Open `http://localhost:8501`.

- **Raw Trades** shows the latest deduplicated PostgreSQL trades.
- **Live Metrics** shows one-minute metrics and moving-average charts.

Both pages refresh every five seconds.

Dashboard timestamps use UTC and 24-hour formatting. Missing trade periods are
shown as gaps instead of connecting unrelated points.

## Health and tests

Docker health checks cover Kafka, PostgreSQL, MinIO, and Streamlit. The
dashboard sidebar reports the actual PostgreSQL connection and latest event.

Run the unit and PostgreSQL integration tests:

```bash
docker compose --profile test run --rm pipeline-tests
```

## Start and observe

Start the streaming services:

```bash
docker compose up -d --build stream-producer stream-consumer
```

Follow their logs:

```bash
docker compose logs -f stream-producer stream-consumer
```

Stop only the streaming services:

```bash
docker compose stop stream-producer stream-consumer
```

## Current scope

Completed:

- Live Finnhub WebSocket ingestion
- Idempotent Kafka publication keyed by symbol
- Kafka-to-MinIO micro-batching
- Explicit three-partition Kafka topics
- Trade validation and dead-letter routing
- Manual offset commits after successful writes
- Deterministic, replay-safe object names
- PostgreSQL trade deduplication and one-minute metrics
- Two-page live Streamlit dashboard
- Unit and PostgreSQL integration tests
- Service health checks and WebSocket reconnect monitoring
