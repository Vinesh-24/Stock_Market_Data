"""PostgreSQL queries used by the Streamlit dashboard."""

import os

import pandas as pd
import psycopg2


def create_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "stock_market"),
        user=os.getenv("POSTGRES_USER", "stockmarket"),
        password=os.getenv("POSTGRES_PASSWORD", "stockmarket"),
    )


def query_dataframe(query: str, parameters=None) -> pd.DataFrame:
    with create_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def available_symbols() -> list[str]:
    frame = query_dataframe(
        "SELECT DISTINCT symbol FROM trades ORDER BY symbol"
    )
    return frame["symbol"].tolist()


def database_status() -> tuple[bool, object]:
    """Return database availability and the latest received event time."""
    try:
        frame = query_dataframe(
            "SELECT MAX(received_at) AS latest_received FROM trades"
        )
        return True, frame.iloc[0]["latest_received"]
    except Exception:
        return False, None
