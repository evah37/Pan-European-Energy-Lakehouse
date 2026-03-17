from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import requests
import boto3
from io import StringIO
import pandas as pd

# Global configuration
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"
BUCKET_NAME = "energy-lake"

default_args = {
    'owner': 'energy_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Python function: API snapshot
def fetch_awattar_snapshot(**kwargs):

    # define API address for different regions
    endpoints = {
        "DE": "https://api.awattar.de/v1/marketdata",
        "AT": "https://api.awattar.at/v1/marketdata"
    }

    # initialize S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    # loop to fetch and upload
    for region, url in endpoints.items():
        try:
            print(f"Fetching data for region: {region} from {url}")
            response = requests.get(url, timeout=15)
            response.raise_for_status()  # check HTTP error
            data = response.json().get('data', [])

            if data:
                df = pd.DataFrame(data)
                # add region column
                df['region'] = region
                df['ingestion_time'] = datetime.now().isoformat()

                # convert to CSV buffer
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)

                # path partition by region
                filename = f"bronze/api_awattar_snapshot/{region}/{datetime.now().strftime('%Y%m%d')}_snapshot.csv"

                s3.put_object(Body=csv_buffer.getvalue(), Bucket=BUCKET_NAME, Key=filename)
                print(f"Successfully uploaded {region} snapshot to {filename}")
            else:
                print(f"No data points returned for {region}")

        except Exception as e:
            print(f"Failed to fetch {region} data: {str(e)}")


# DAG definition
with DAG(
    'energy_pipeline_final',
    default_args=default_args,
    description='Lambda Arch: Infra -> Batch (History) + Speed (Real-time)',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['energy', 'lakehouse', 'lambda-arch'],
) as dag:

    # basic infrastructure setup
    task_create_bucket = S3CreateBucketOperator(
        task_id='ensure_bucket_exists',
        bucket_name=BUCKET_NAME,
        aws_conn_id='aws_default'
    )

    # batch layer: process historical data
    with TaskGroup("batch_layer_history") as batch_group:

        # ingest historical data from kaggle
        task_ingest_history = BashOperator(
            task_id='ingest_kaggle_history',
            bash_command='python /opt/airflow/scripts/ingest_kaggle.py',
            env={
                **os.environ,
                "MINIO_ENDPOINT": S3_ENDPOINT,
                "MINIO_ACCESS_KEY": S3_ACCESS_KEY,
                "MINIO_SECRET_KEY": S3_SECRET_KEY
            }
        )

        # spark job for cleaning and feature engineering
        task_spark_batch = BashOperator(
            task_id='spark_batch_processing',
            bash_command="""
            spark-submit \\
            --master local[*] \\
            --packages org.apache.hadoop:hadoop-aws:3.3.4 \\
            /opt/airflow/spark_jobs/feature_engineering.py
            """,
            env={**os.environ, "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64"}
        )

        # dbt model for dimensional modeling
        task_dbt_seed = BashOperator(
            task_id='dbt_seed',
            bash_command='cd /opt/airflow/dbt_energy && dbt seed --profiles-dir .',
        )

        task_dbt_run = BashOperator(
            task_id='dbt_run_models',
            bash_command='cd /opt/airflow/dbt_energy && dbt run --profiles-dir .',
        )

        task_dbt_test = BashOperator(
            task_id='dbt_test_quality',
            bash_command='cd /opt/airflow/dbt_energy && dbt test --profiles-dir .',
        )

        # batch internal dependencies
        task_ingest_history >> task_spark_batch >> task_dbt_seed >> task_dbt_run >> task_dbt_test

    # speed layer: process real-time API
    with TaskGroup("speed_layer_realtime") as speed_group:

        # API snapshot ingestion
        task_ingest_api_snapshot = PythonOperator(
            task_id='ingest_api_snapshot',
            python_callable=fetch_awattar_snapshot
        )

        # spark streaming job for processing real-time API
        task_start_streaming = BashOperator(
            task_id='trigger_spark_streaming',
            bash_command="""
            # check if the job is already running
            if ! pgrep -f "stream_process_prices.py" > /dev/null; then
                echo "Starting Spark Streaming Job..."
                nohup spark-submit \\
                --master spark://spark-master:7077 \\
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \\
                /opt/airflow/spark_jobs/stream_process_prices.py > /opt/airflow/logs/streaming.log 2>&1 &
            else
                echo "Spark Streaming Job is already running."
            fi
            """,
            env={**os.environ, "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64"}
        )

        # speed internal dependencies
        task_ingest_api_snapshot
        task_start_streaming

    # serving layer

    # only notify when history data is updated and streaming is running
    task_notify_dashboard = BashOperator(
        task_id='dashboard_ready_notification',
        bash_command="echo 'Pipeline Finished. Streamlit Dashboard has access to fresh Batch Data & Active Stream.'",
    )

    # global dependencies

    # 1. infrastructure: bucket must exist before anything else
    task_create_bucket >> [task_ingest_history, task_ingest_api_snapshot]

    # 2. critical dependencies: streaming depends on batch

    task_dbt_run >> task_start_streaming

    # 3. final state: notify when history data is updated and streaming is running
    [task_dbt_test, task_start_streaming] >> task_notify_dashboard
