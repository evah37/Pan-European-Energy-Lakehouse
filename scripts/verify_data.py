import os
import re
import polars as pl
import boto3
import tempfile
from pathlib import Path

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
BUCKET_NAME = "raw"
PREFIX = "entsoe/"
SOURCE_FILE_PATH = "/opt/airflow/data/time_series_60min_singleindex.csv"


def get_target_countries(columns):
    """
    Recover target countries logic from ingest script to ensure consistency
    """
    countries = set()
    pattern = re.compile(r"^(.+)_price_day_ahead$")
    for col in columns:
        match = pattern.match(col)
        if match:
            countries.add(match.group(1))
    return sorted(list(countries))


def verify_data():
    print("Starting Data Verification...")

    # Source data analysis
    print(f"Reading source schema from: {SOURCE_FILE_PATH}")
    try:
        # Read only header to get columns
        source_df = pl.read_csv(SOURCE_FILE_PATH, n_rows=0)
        source_cols = source_df.columns
        target_countries = get_target_countries(source_cols)
        print(f"Source: Detected {len(target_countries)} target countries: {target_countries}")

        # Calculate expected metrics (approximate)
    except Exception as e:
        print(f"Error reading source file: {e}")
        return False

    # Download and load parquet data
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "downloaded_parquet"
        local_path.mkdir()

        print(f"Downloading parquet files from s3://{BUCKET_NAME}/{PREFIX} to {local_path}...")

        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)

        download_count = 0
        for page in pages:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith(".parquet"):
                    # Create local directory structure
                    rel_path = key.replace(PREFIX, "")
                    dest_file = local_path / rel_path
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    s3.download_file(BUCKET_NAME, key, str(dest_file))
                    download_count += 1

        print(f"Downloaded {download_count} parquet files.")

        if download_count == 0:
            print("Error: No parquet files found in MinIO!")
            return False

        # Load all parquet files using Polars
        try:
            print("Loading parquet files into Polars DataFrame...")
            # Use glob to read all parquet files in the nested structure
            df = pl.read_parquet(str(local_path / "**/*.parquet"))
            print(f"Loaded DataFrame. Shape: {df.shape}")
        except Exception as e:
            print(f"Error loading parquet files: {e}")
            return False

    # Validation checks

    # Schema consistency
    expected_columns = {"timestamp", "country", "metric_name", "value", "year", "month"}
    missing_cols = expected_columns - set(df.columns)
    if missing_cols:
        print(f"FAILED: Schema check. Missing columns: {missing_cols}")
        return False
    print("PASSED: Schema check.")

    # Row count sanity check

    total_rows = df.height
    print(f"Total Rows: {total_rows}")
    if total_rows < 1000000:
        print("WARNING: Row count seems suspiciously low (< 1 million).")

    # Country consistency
    actual_countries = sorted(df["country"].unique().to_list())
    print(f"Actual Countries in Parquet: {actual_countries}")

    unexpected_countries = set(actual_countries) - set(target_countries)
    missing_countries = set(target_countries) - set(actual_countries)

    if unexpected_countries:
        print(f"FAILED: Unexpected countries found: {unexpected_countries}")
        return False
    if missing_countries:
        # unlikely in filtered list some countries might not have all metrics
        print(f"WARNING: Some target countries are missing data: {missing_countries}")
    else:
        print("PASSED: Country list matches source exactly.")

    # Metric consistency (sanity check for known suffixes)
    actual_metrics = df["metric_name"].unique().to_list()
    print(f"Actual Metrics Found: {actual_metrics[:10]} ... (Total {len(actual_metrics)})")

    required_metrics_keywords = ["price", "load", "generation"]
    found_keywords = [
        keyword for keyword in required_metrics_keywords
        if any(keyword in metric for metric in actual_metrics)
    ]
    if len(found_keywords) < len(required_metrics_keywords):
        print(f"WARNING: Missing some expected metric types. Found keywords: {found_keywords}")
    else:
        print("PASSED: Metric types look reasonable.")

    # Null values
    null_counts = df.null_count()
    print("Null Value Counts per Column:")
    print(null_counts)

    if df["country"].null_count() > 0 or df["timestamp"].null_count() > 0 or df["value"].null_count() > 0:
        print("FAILED: Critical columns (country, timestamp, value) contain NULLs.")
        return False
    print("PASSED: Critical columns have no NULLs.")

    # Specific user request
    # Check if we have partitioned country codes in 'country' column
    se_countries = [c for c in actual_countries if c.startswith("SE_")]
    print(f"SE Countries found: {se_countries}")
    if "SE_1" in se_countries:
        print("PASSED: SE_1 detected correctly.")
    else:
        print("WARNING: SE_1 not found (might be missing in data or regex issue).")

    print("\n=== Verification Summary ===")
    print("All checks completed.")
    return True


if __name__ == "__main__":
    success = verify_data()
    if not success:
        exit(1)
