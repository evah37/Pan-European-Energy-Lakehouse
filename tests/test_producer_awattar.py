"""
Unit tests for producer_awattar.py
"""
import pytest
import json
from unittest.mock import patch, MagicMock, call
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


# fetch_awattar_data
class TestFetchAwattarData:
    @patch("producer_awattar.requests.get")
    def test_successful_fetch_de(self, mock_get, awattar_api_response):
        """Should parse DE region API response correctly."""
        from producer_awattar import fetch_awattar_data
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = awattar_api_response
        mock_get.return_value = mock_response

        result = fetch_awattar_data("DE")
        assert len(result) == 3
        assert result[0]["marketprice"] == 85.50

    @patch("producer_awattar.requests.get")
    def test_successful_fetch_at(self, mock_get, awattar_api_response):
        """Should use AT region endpoint correctly."""
        from producer_awattar import fetch_awattar_data
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = awattar_api_response
        mock_get.return_value = mock_response

        result = fetch_awattar_data("AT")
        mock_get.assert_called_once_with(
            "https://api.awattar.at/v1/marketdata", timeout=30
        )
        assert len(result) == 3

    @patch("producer_awattar.requests.get")
    def test_api_error_returns_empty(self, mock_get):
        """Should return empty list on HTTP error."""
        from producer_awattar import fetch_awattar_data
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = fetch_awattar_data("DE")
        assert result == []

    @patch("producer_awattar.requests.get")
    def test_network_timeout_returns_empty(self, mock_get):
        """Should return empty list on network timeout."""
        from producer_awattar import fetch_awattar_data
        import requests
        mock_get.side_effect = requests.exceptions.Timeout("timeout")

        result = fetch_awattar_data("DE")
        assert result == []

    def test_unknown_region_returns_empty(self):
        """Should return empty list for unsupported regions."""
        from producer_awattar import fetch_awattar_data
        result = fetch_awattar_data("XX")
        assert result == []

    @patch("producer_awattar.requests.get")
    def test_empty_data_field(self, mock_get):
        """Should return empty list when API returns no data points."""
        from producer_awattar import fetch_awattar_data
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response

        result = fetch_awattar_data("DE")
        assert result == []


# Region Configuration
class TestRegionConfig:
    def test_regions_defined(self):
        """REGIONS dict should contain DE and AT endpoints."""
        from producer_awattar import REGIONS
        assert "DE" in REGIONS
        assert "AT" in REGIONS
        assert "awattar.de" in REGIONS["DE"]
        assert "awattar.at" in REGIONS["AT"]

    def test_region_urls_are_v1(self):
        """All region URLs should use the v1 API."""
        from producer_awattar import REGIONS
        for region, url in REGIONS.items():
            assert "/v1/marketdata" in url, (
                f"Region {region} URL missing /v1/marketdata"
            )


# Kafka Message Format
class TestKafkaMessageFormat:
    def test_message_schema(self, awattar_api_response):
        """Kafka messages should have the expected fields."""
        import time
        data_point = awattar_api_response["data"][0]
        # Simulate the message construction from producer
        message = {
            "timestamp_ms": data_point["start_timestamp"],
            "price_eur_mwh": data_point["marketprice"],
            "unit": data_point["unit"],
            "region": "DE",
            "ingested_at": time.time(),
        }
        required_keys = {
            "timestamp_ms", "price_eur_mwh", "unit",
            "region", "ingested_at"
        }
        assert required_keys == set(message.keys())

    def test_message_serializable(self, awattar_api_response):
        """Message should be JSON-serializable (Kafka requirement)."""
        import time
        data_point = awattar_api_response["data"][0]
        message = {
            "timestamp_ms": data_point["start_timestamp"],
            "price_eur_mwh": data_point["marketprice"],
            "unit": data_point["unit"],
            "region": "DE",
            "ingested_at": time.time(),
        }
        serialized = json.dumps(message)
        assert isinstance(serialized, str)
        deserialized = json.loads(serialized)
        assert deserialized["price_eur_mwh"] == 85.50

    def test_negative_price_handled(self):
        """Negative prices should serialize."""
        message = {
            "timestamp_ms": 1700000000000,
            "price_eur_mwh": -5.20,
            "unit": "Eur/MWh",
            "region": "DE",
            "ingested_at": 1700000000.0,
        }
        serialized = json.dumps(message)
        deserialized = json.loads(serialized)
        assert deserialized["price_eur_mwh"] == -5.20
