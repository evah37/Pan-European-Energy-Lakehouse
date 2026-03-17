"""
Unit tests for ingest_kaggle.py
"""
import pytest
import polars as pl
from unittest.mock import patch, MagicMock, call
from botocore.exceptions import ClientError
import sys
import os

# Add scripts to sys.path to import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


# get_target_countries
class TestGetTargetCountries:
    def test_detects_standard_countries(self, sample_csv_header):
        """Should detect DE_LU, AT, FR from sample header."""
        from ingest_kaggle import get_target_countries
        countries = get_target_countries(sample_csv_header)
        assert "DE_LU" in countries
        assert "AT" in countries
        assert "FR" in countries

    def test_returns_sorted_list(self, sample_csv_header):
        """Country list should be sorted alphabetically."""
        from ingest_kaggle import get_target_countries
        countries = get_target_countries(sample_csv_header)
        assert countries == sorted(countries)

    def test_ignores_non_price_columns(self):
        """Should not detect countries from load/solar/wind columns only."""
        from ingest_kaggle import get_target_countries
        columns = [
            "utc_timestamp",
            "DE_LU_load_actual",
            "DE_LU_solar_generation",
        ]
        countries = get_target_countries(columns)
        assert len(countries) == 0

    def test_handles_se_bidding_zones(self):
        """Should correctly detect SE_1, SE_2 etc. as separate countries."""
        from ingest_kaggle import get_target_countries
        columns = [
            "utc_timestamp",
            "SE_1_price_day_ahead",
            "SE_2_price_day_ahead",
            "SE_1_load_actual",
        ]
        countries = get_target_countries(columns)
        assert "SE_1" in countries
        assert "SE_2" in countries
        assert len(countries) == 2

    def test_empty_columns(self):
        """Should return empty list for empty column list."""
        from ingest_kaggle import get_target_countries
        countries = get_target_countries([])
        assert countries == []


# ensure_bucket_exists
class TestEnsureBucketExists:

    @patch("ingest_kaggle.boto3")
    def test_bucket_already_exists(self, mock_boto3):
        """If bucket exists, should not create."""
        from ingest_kaggle import ensure_bucket_exists
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_bucket.return_value = {}

        ensure_bucket_exists("test-bucket")

        mock_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
        mock_client.create_bucket.assert_not_called()

    @patch("ingest_kaggle.boto3")
    def test_bucket_does_not_exist(self, mock_boto3):
        """If bucket does not exist, should create it."""
        from ingest_kaggle import ensure_bucket_exists
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadBucket"
        )

        ensure_bucket_exists("test-bucket")

        mock_client.create_bucket.assert_called_once_with(
            Bucket="test-bucket"
        )


# parse_and_process
class TestParseAndProcess:
    def test_output_schema(self, sample_csv_file):
        """Processed DataFrame should have correct columns."""
        from ingest_kaggle import parse_and_process
        df = parse_and_process(sample_csv_file)
        expected_cols = {"timestamp", "country", "metric_name", "value",
                         "year", "month"}
        assert expected_cols.issubset(set(df.columns))

    def test_country_extraction(self, sample_csv_file):
        """Country column should be correctly extracted from column names."""
        from ingest_kaggle import parse_and_process
        df = parse_and_process(sample_csv_file)
        countries = df["country"].unique().sort().to_list()
        assert "DE_LU" in countries
        assert "AT" in countries
        assert "FR" in countries

    def test_no_null_critical_fields(self, sample_csv_file):
        """Critical fields (country, timestamp, value) should have no nulls."""
        from ingest_kaggle import parse_and_process
        df = parse_and_process(sample_csv_file)
        assert df["country"].null_count() == 0
        assert df["timestamp"].null_count() == 0
        assert df["value"].null_count() == 0

    def test_year_month_partition_columns(self, sample_csv_file):
        """Year and month partition columns should be string type."""
        from ingest_kaggle import parse_and_process
        df = parse_and_process(sample_csv_file)
        assert df["year"].dtype == pl.Utf8
        assert df["month"].dtype == pl.Utf8

    def test_month_zero_padded(self, sample_csv_file):
        """Month values should be zero-padded."""
        from ingest_kaggle import parse_and_process
        df = parse_and_process(sample_csv_file)
        months = df["month"].unique().to_list()
        for m in months:
            assert len(m) == 2, f"Month '{m}' is not zero-padded"

    def test_metric_name_extracted(self, sample_csv_file):
        """Metric names should have country prefix stripped."""
        from ingest_kaggle import parse_and_process
        df = parse_and_process(sample_csv_file)
        metrics = df["metric_name"].unique().to_list()
        # Should contain metrics WITHOUT country prefix
        assert any("price_day_ahead" in m for m in metrics)
        # Should NOT contain the country prefix
        assert not any(m.startswith("DE_LU_") for m in metrics)

    def test_raises_on_no_price_columns(self, sample_csv_empty_prices):
        """Should raise ValueError when no price_day_ahead columns found."""
        from ingest_kaggle import parse_and_process
        with pytest.raises(ValueError, match="No price columns"):
            parse_and_process(sample_csv_empty_prices)

    def test_wide_to_long_format(self, sample_csv_file):
        """Output should be in long format."""
        from ingest_kaggle import parse_and_process
        df = parse_and_process(sample_csv_file)
        # Value column should exist (long format indicator)
        assert "value" in df.columns
        # Each row should have exactly one country + metric + value
        assert df.height > 0
        # More rows than input
        assert df.height > 5
