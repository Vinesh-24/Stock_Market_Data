"""Integration tests for PostgreSQL trades and one-minute metrics."""

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src" / "kafka" / "consumer"))

from analytics_consumer import create_connection


class PostgresPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = create_connection()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def test_trade_event_ids_are_unique(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT event_id)
                FROM trades
                """
            )
            total, unique = cursor.fetchone()
        self.assertGreater(total, 0)
        self.assertEqual(total, unique)

    def test_one_minute_metrics_are_available(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM one_minute_metrics")
            metric_rows = cursor.fetchone()[0]
        self.assertGreater(metric_rows, 0)

    def test_ohlcv_values_are_valid(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM one_minute_metrics
                WHERE high_price < low_price
                   OR high_price < open_price
                   OR high_price < close_price
                   OR low_price > open_price
                   OR low_price > close_price
                   OR volume < 0
                   OR trade_count < 1
                """
            )
            invalid_rows = cursor.fetchone()[0]
        self.assertEqual(invalid_rows, 0)


if __name__ == "__main__":
    unittest.main()
