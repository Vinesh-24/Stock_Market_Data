#!/usr/bin/env bash

set -Eeuo pipefail

trap 'echo "END-TO-END TEST FAILED at line ${LINENO}" >&2' ERR

echo "1/7 Starting the platform"
docker compose up -d --build

echo "2/7 Waiting for services"
containers=(
    kafka
    minio
    stock-postgres
    stream-producer
    stream-consumer
    analytics-consumer
    stock-dashboard
)

for _ in {1..60}; do
    all_ready=true

    for container in "${containers[@]}"; do
        status=$(
            docker inspect --format \
                '{{if .State.Health}}{{.State.Health.Status}}{{else if .State.Running}}running{{else}}{{.State.Status}}{{end}}' \
                "$container"
        )
        if [[ "$status" != "healthy" && "$status" != "running" ]]; then
            all_ready=false
            break
        fi
    done

    if [[ "$all_ready" == "true" ]]; then
        break
    fi

    sleep 2
done

if [[ "$all_ready" != "true" ]]; then
    echo "Services did not become ready within 120 seconds" >&2
    docker compose ps
    exit 1
fi

echo "3/7 Showing recent pipeline logs"
docker compose logs --tail=10 \
    stream-producer \
    stream-consumer \
    analytics-consumer

echo "4/7 Checking PostgreSQL data"
trade_count=$(
    docker exec stock-postgres sh -c \
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT COUNT(*) FROM trades;"'
)
metric_count=$(
    docker exec stock-postgres sh -c \
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT COUNT(*) FROM one_minute_metrics;"'
)

if (( trade_count < 1 || metric_count < 1 )); then
    echo "PostgreSQL does not contain trades and metrics" >&2
    exit 1
fi

echo "5/7 Checking MinIO raw objects"
docker exec stream-consumer python -c '
import os
from minio import Minio

client = Minio(
    os.environ["MINIO_ENDPOINT"],
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False,
)
objects = client.list_objects(
    os.environ["MINIO_BUCKET"],
    prefix="raw/realtime/",
    recursive=True,
)
if next(objects, None) is None:
    raise SystemExit("No raw realtime objects found in MinIO")
'

echo "6/7 Checking Streamlit"
dashboard_status=$(curl --fail --silent http://localhost:8501/_stcore/health)
if [[ "$dashboard_status" != "ok" ]]; then
    echo "Streamlit health check failed" >&2
    exit 1
fi

echo "7/7 Running unit and integration tests"
docker compose --profile test run --rm pipeline-tests

echo "END-TO-END TEST PASSED"
