import os
import re
import polars as pl
import boto3
from botocore.exceptions import ClientError


# set environment variables

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
BUCKET_NAME = "energy-lake"
SOURCE_FILE_PATH = "/opt/airflow/data/time_series_60min_singleindex.csv"
TARGET_PREFIX = "bronze/entsoe/"


def ensure_bucket_exists(bucket_name):
    """
    ensure the bucket exists
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except ClientError:
        print(f"Bucket '{bucket_name}' not found. Creating...")
        s3.create_bucket(Bucket=bucket_name)


def get_target_countries(columns):
    """
    dynamically scan the header to determine the list of countries by finding fields containing '_price_day_ahead'
    """
    countries = set()
    # match any prefix before _price_day_ahead
    pattern = re.compile(r"^(.+)_price_day_ahead$")

    for col in columns:
        match = pattern.match(col)
        if match:
            countries.add(match.group(1))

    sorted_countries = sorted(list(countries))
    print("--- dynamic detection result ---")
    print(f"auto-detected countries/regions based on price_day_ahead field: {sorted_countries}")
    return sorted_countries


def parse_and_process(file_path):
    print(f"Reading file header: {file_path}")

    # pre-read schema
    header_df = pl.read_csv(file_path, n_rows=0)
    all_columns = header_df.columns

    # dynamic country list
    target_countries = get_target_countries(all_columns)

    if not target_countries:
        raise ValueError("Critical Error: No price columns found in CSV, cannot determine target countries!")

    # build filter columns (Select)
    valid_cols = []
    for col in all_columns:
        if any(col.startswith(f"{country}_") for country in target_countries):
            valid_cols.append(col)

    print(f"Found {len(valid_cols)} relevant columns to process.")

    # lazy load data and clean
    q = pl.scan_csv(file_path, null_values=["NA", "nan", "-"])

    # select only the columns we need
    q = q.select(["utc_timestamp"] + valid_cols)

    # melt (wide to long)
    # schema becomes: timestamp | raw_col_name | value
    q = q.melt(
        id_vars=["utc_timestamp"],
        value_vars=valid_cols,
        variable_name="raw_col_name",
        value_name="value"
    )

    # feature extraction (Country, Metric, Year, Month)
    # sort country codes by length in descending order to ensure longer prefixes match first

    sorted_countries = sorted(target_countries, key=len, reverse=True)
    country_pattern = "|".join(re.escape(c) for c in sorted_countries)

    q = q.with_columns([
        # timestamp conversion
        # remove trailing Z, replace T with space, parse as timezone-naive datetime
        pl.col("utc_timestamp").str.replace("Z$", "").str.replace("T", " ").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("timestamp"),

        # use dynamic regex pattern to match known country codes
        pl.col("raw_col_name").str.extract(f"^({country_pattern})_", 1).alias("country"),

        # remove country prefix (including the following underscore)
        pl.col("raw_col_name").str.replace(f"^({country_pattern})_", "").alias("metric_name"),

        # convert to float64
        pl.col("value").cast(pl.Float64)
    ]).drop(["utc_timestamp", "raw_col_name"])

    # add partition columns
    q = q.with_columns([
        pl.col("timestamp").dt.year().cast(pl.Utf8).alias("year"),
        pl.col("timestamp").dt.month().cast(pl.Utf8).str.zfill(2).alias("month")
    ])

    # filter out null values
    q = q.filter(pl.col("value").is_not_null())

    print("Processing data (executing computation graph)...")
    df = q.collect()
    print(f"Processed dataframe shape: {df.shape}")

    return df


def upload_to_datalake(df):
    """
    write to MinIO (Parquet Partitioned)
    save to local temporary directory first, then upload to MinIO
    """
    import tempfile
    from pathlib import Path

    print(f"Uploading to MinIO bucket: {BUCKET_NAME}/{TARGET_PREFIX}...")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # clean existing data to ensure idempotency
    print(f"Cleaning existing data in {BUCKET_NAME}/{TARGET_PREFIX}...")
    try:
        objects_to_delete = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=TARGET_PREFIX)
        if 'Contents' in objects_to_delete:
            delete_keys = [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]
            # batch delete
            for i in range(0, len(delete_keys), 1000):
                batch = delete_keys[i:i + 1000]
                s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': batch})
            print(f"Deleted {len(delete_keys)} existing files.")
    except Exception as e:
        print(f"Warning during cleanup: {e}")

    # use temporary directory to save partitioned parquet files
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "entsoe"

        # use PyArrow to write partitioned parquet
        df.write_parquet(
            str(local_path),
            use_pyarrow=True,
            pyarrow_options={"partition_cols": ["year", "month"]}
        )

        print(f"Saved partitioned parquet to: {local_path}")

        # recursively upload all files to MinIO
        uploaded_count = 0
        for file_path in local_path.rglob("*.parquet"):
            relative_path = file_path.relative_to(local_path)
            s3_key = f"{TARGET_PREFIX}{relative_path}"

            s3.upload_file(str(file_path), BUCKET_NAME, s3_key)
            uploaded_count += 1

        print(f"Upload successful! Uploaded {uploaded_count} parquet files.")


if __name__ == "__main__":

    ensure_bucket_exists(BUCKET_NAME)

    if os.path.exists(SOURCE_FILE_PATH):
        try:
            df = parse_and_process(SOURCE_FILE_PATH)
            upload_to_datalake(df)
        except Exception as e:
            print(f"Pipeline Failed: {e}")
            exit(1)
    else:
        print(f"Error: Source file not found at {SOURCE_FILE_PATH}")
        print("Please ensure you downloaded the Kaggle CSV and placed it in the 'data/' folder.")
        exit(1)
