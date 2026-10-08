# Project explanation

## One-line summary

A Docker-based streaming platform that receives live stock trades from
Finnhub, processes them through Kafka, archives raw data in MinIO, calculates
one-minute analytics in PostgreSQL, and displays them in Streamlit.

## End-to-end flow

1. The producer subscribes to Finnhub WebSocket trades.
2. It normalizes each trade and creates a deterministic SHA-256 `event_id`.
3. Kafka stores events in a three-partition topic keyed by stock symbol.
4. The raw consumer validates events and writes replayable CSV micro-batches to
   MinIO.
5. Invalid events go to the Kafka dead-letter topic with their error and source
   offset.
6. A separate consumer inserts trades into PostgreSQL. Its `event_id` primary
   key removes duplicates.
7. PostgreSQL calculates one-minute OHLCV, trade count, VWAP, price change,
   moving averages, and rolling volatility.
8. Streamlit queries PostgreSQL every five seconds for the Raw Trades and Live
   Metrics pages.

## Reliability decisions

- Kafka producer idempotence prevents duplicate retry writes from one producer.
- Deterministic `event_id` values handle duplicates across producer instances.
- Consumers commit Kafka offsets only after storage succeeds.
- MinIO object names contain Kafka offset ranges, making replay overwrite-safe.
- PostgreSQL uses `ON CONFLICT DO NOTHING` for idempotent replay.
- Invalid records are isolated in a dead-letter topic.
- The Finnhub connection retries with exponential backoff.
- Docker health checks cover Kafka, PostgreSQL, MinIO, and Streamlit.

## Why two Kafka consumers

The raw consumer preserves source events in MinIO for replay. The analytics
consumer independently creates queryable PostgreSQL data. Separate consumer
groups allow both consumers to receive every Kafka event without coupling
archival and analytics.

## Resume bullets

- Built a real-time stock market pipeline using Python, Kafka, MinIO,
  PostgreSQL, Docker Compose, Finnhub WebSockets, and Streamlit.
- Implemented partitioned event streaming, schema validation, dead-letter
  routing, manual offset commits, deterministic deduplication, and replay-safe
  storage.
- Developed live one-minute OHLCV, VWAP, moving-average, and volatility
  analytics with a continuously refreshing two-page dashboard.
- Added unit and integration tests, service health checks, and resilient
  WebSocket reconnection.

## Interview explanation

Start with the business goal, walk through the flow from Finnhub to Streamlit,
then explain reliability: Kafka buffering, consumer groups, manual commits,
dead-letter handling, and two levels of deduplication. Finish with the current
local limitation: one Kafka broker uses replication factor 1; a production
deployment would use multiple brokers and replicated storage.
