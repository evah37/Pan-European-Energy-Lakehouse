"""
Shared pytest fixtures for the project.
Provides mock S3 clients, temporary paths, and sample data for unit tests.
"""
import pytest
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT / "spark_jobs"))


@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client for MinIO operations."""
    client = MagicMock()
    client.head_bucket.return_value = {}
    client.create_bucket.return_value = {}
    client.put_object.return_value = {}
    client.list_objects_v2.return_value = {"Contents": []}
    return client


@pytest.fixture
def sample_csv_header():
    """Sample CSV header mimicking ENTSOE data structure."""
    return [
        "utc_timestamp",
        "DE_LU_price_day_ahead",
        "DE_LU_load_actual_entsoe_transparency",
        "DE_LU_solar_generation_actual",
        "DE_LU_wind_generation_actual",
        "AT_price_day_ahead",
        "AT_load_actual_entsoe_transparency",
        "AT_solar_generation_actual",
        "AT_wind_generation_actual",
        "FR_price_day_ahead",
        "FR_load_actual_entsoe_transparency",
    ]


@pytest.fixture
def sample_csv_file(tmp_path, sample_csv_header):
    """
    Create a minimal sample CSV file for testing ingestion.
    """
    csv_content = ",".join(sample_csv_header) + "\n"
    # Add sample data rows
    rows = [
        "2020-01-01T00:00:00Z,28.5,55000,1200,8500,32.1,6000,300,2100,40.2,60000",
        "2020-01-01T01:00:00Z,25.3,52000,800,9000,29.8,5800,200,2300,38.5,58000",
        "2020-01-01T02:00:00Z,22.1,48000,500,9200,27.5,5500,100,2400,35.0,55000",
        "2020-01-01T03:00:00Z,20.0,45000,0,8800,25.0,5200,0,2200,33.0,52000",
        "2020-01-01T04:00:00Z,21.5,46000,0,8600,26.3,5300,0,2100,34.0,53000",
    ]
    csv_content += "\n".join(rows) + "\n"

    csv_path = tmp_path / "test_energy_data.csv"
    csv_path.write_text(csv_content)
    return str(csv_path)


@pytest.fixture
def sample_csv_empty_prices(tmp_path):
    """CSV with no price_day_ahead columns."""
    csv_content = "utc_timestamp,DE_LU_load_actual,DE_LU_solar_gen\n"
    csv_content += "2020-01-01T00:00:00Z,55000,1200\n"
    csv_path = tmp_path / "no_prices.csv"
    csv_path.write_text(csv_content)
    return str(csv_path)


@pytest.fixture
def awattar_api_response():
    """Sample Awattar API response for testing."""
    return {
        "data": [
            {
                "start_timestamp": 1700000000000,
                "end_timestamp": 1700003600000,
                "marketprice": 85.50,
                "unit": "Eur/MWh",
            },
            {
                "start_timestamp": 1700003600000,
                "end_timestamp": 1700007200000,
                "marketprice": 72.30,
                "unit": "Eur/MWh",
            },
            {
                "start_timestamp": 1700007200000,
                "end_timestamp": 1700010800000,
                "marketprice": -5.20,
                "unit": "Eur/MWh",
            },
        ]
    }
