"""Unit tests for trade normalization, validation, and deduplication."""

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
KAFKA_ROOT = PROJECT_ROOT / "src" / "kafka"
sys.path.insert(0, str(KAFKA_ROOT / "producer"))
sys.path.insert(0, str(KAFKA_ROOT / "consumer"))

from analytics_consumer import event_id_for
from stream_data_consumer import validate_trade
from stream_producer import normalize_trade


class TradeProcessingTests(unittest.TestCase):
    def setUp(self):
        self.finnhub_trade = {
            "s": "AAPL",
            "p": 250.50,
            "v": 100,
            "t": 1791396000000,
            "c": ["@"],
        }

    def test_same_trade_gets_same_event_id(self):
        first = normalize_trade(self.finnhub_trade)
        second = normalize_trade(self.finnhub_trade)
        self.assertEqual(first["event_id"], second["event_id"])

    def test_changed_trade_gets_different_event_id(self):
        first = normalize_trade(self.finnhub_trade)
        changed = normalize_trade({**self.finnhub_trade, "p": 251.00})
        self.assertNotEqual(first["event_id"], changed["event_id"])

    def test_valid_trade_passes_validation(self):
        trade = normalize_trade(self.finnhub_trade)
        self.assertEqual(validate_trade(trade), trade)

    def test_invalid_trade_fails_validation(self):
        with self.assertRaisesRegex(ValueError, "missing fields"):
            validate_trade({"symbol": "AAPL"})

    def test_historical_event_id_is_deterministic(self):
        trade = normalize_trade(self.finnhub_trade)
        trade.pop("event_id")
        self.assertEqual(event_id_for(trade), event_id_for(trade))


if __name__ == "__main__":
    unittest.main()
