"""Fetch daily stock data from yfinance and write it directly to MinIO."""

import argparse
import io
import logging
import os
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "INTC",
    "JPM",
    "V",
]


def create_minio_client() -> Minio:
    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def get_symbols() -> list[str]:
    configured = os.getenv("BATCH_SYMBOLS")
    if not configured:
        return DEFAULT_SYMBOLS
    return [symbol.strip().upper() for symbol in configured.split(",") if symbol.strip()]


def has_historical_data(client: Minio, bucket: str) -> bool:
    """Return True after at least one historical object has been loaded."""
    objects = client.list_objects(
        bucket,
        prefix="raw/historical/",
        recursive=True,
    )
    return next(objects, None) is not None


def fetch_daily_data(symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch daily OHLCV data. yfinance treats end_date as exclusive."""
    data = yf.Ticker(symbol).history(
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
    )

    if data.empty:
        logger.warning("No data returned for %s", symbol)
        return pd.DataFrame()

    data = data.reset_index()
    data = data.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    data["date"] = pd.to_datetime(data["date"]).dt.strftime("%Y-%m-%d")
    data["symbol"] = symbol

    return data[["date", "symbol", "open", "high", "low", "close", "volume"]]


def write_to_minio(
    client: Minio,
    bucket: str,
    symbol: str,
    data: pd.DataFrame,
) -> int:
    """Write one deterministic CSV per trading day.

    Re-running an Airflow interval overwrites the same object, so backfills do
    not create duplicates.
    """
    uploaded = 0

    for row in data.to_dict(orient="records"):
        trade_date = row["date"]
        year, month, _ = trade_date.split("-")
        object_name = (
            f"raw/historical/symbol={symbol}/year={year}/month={month}/"
            f"{symbol}_{trade_date}.csv"
        )

        payload = pd.DataFrame([row]).to_csv(index=False).encode("utf-8")
        client.put_object(
            bucket,
            object_name,
            io.BytesIO(payload),
            length=len(payload),
            content_type="text/csv",
        )
        uploaded += 1

    return uploaded


def run(run_date: date, full_load: bool = False) -> None:
    bucket = os.environ["MINIO_BUCKET"]
    client = create_minio_client()

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    first_run = not has_historical_data(client, bucket)
    load_one_year = first_run or full_load
    start_date = run_date - timedelta(days=365 if load_one_year else 7)

    load_type = "initial one-year load" if load_one_year else "incremental load"
    logger.info(
        "Starting %s from %s through %s",
        load_type,
        start_date,
        run_date,
    )

    failures = []
    total_uploaded = 0

    for symbol in get_symbols():
        try:
            data = fetch_daily_data(symbol, start_date, run_date)
            uploaded = write_to_minio(client, bucket, symbol, data)
            total_uploaded += uploaded
            logger.info("Uploaded %s daily files for %s", uploaded, symbol)
        except Exception:
            logger.exception("Failed to ingest %s", symbol)
            failures.append(symbol)

    if failures:
        raise RuntimeError(f"Batch ingestion failed for: {', '.join(failures)}")

    logger.info("Batch ingestion complete. Uploaded %s files.", total_uploaded)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load daily stock data into MinIO.")
    parser.add_argument(
        "--run-date",
        default=date.today().isoformat(),
        help="Logical processing date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Reload the year ending on run-date.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    run(datetime.strptime(args.run_date, "%Y-%m-%d").date(), args.full_load)


if __name__ == "__main__":
    main()
