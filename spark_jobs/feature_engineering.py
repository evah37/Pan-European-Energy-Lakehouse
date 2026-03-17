from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")

# Path definitions
BUCKET = "energy-lake"
INPUT_PATH_BRONZE = f"s3a://{BUCKET}/bronze/entsoe/"
OUTPUT_PATH_SILVER_FEATURES = f"s3a://{BUCKET}/silver/features/"
OUTPUT_PATH_SILVER_STATS = f"s3a://{BUCKET}/silver/stats/price_profile/"


def create_spark_session():
    # Create SparkSession with MinIO/S3 dependencies
    return (SparkSession.builder
            .appName("Energy_Medallion_Bronze_to_Silver")
            .master("spark://spark-master:7077")
            .config("spark.executor.memory", "1g")
            # S3A dependencies
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())


def run_silver_transformation():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Silver Layer] Reading Bronze data from {INPUT_PATH_BRONZE}...")

    # read parquet files from bronze layer
    df = spark.read.parquet(INPUT_PATH_BRONZE)

    # data cleaning and pivoting
    print("Pivoting data (Long to Wide)...")

    # normalize metric names (handle Kaggle data inconsistencies)
    df_clean = df.withColumn(
        "metric_group",
        F.when(F.col("metric_name").contains("price_day_ahead"), "price")
        .when(F.col("metric_name").contains("load"), "load")
        .when(F.col("metric_name").contains("solar"), "solar")
        .when(F.col("metric_name").contains("wind"), "wind")
        .otherwise("other")
    ).filter(F.col("metric_group") != "other")

    # pivot: timestamp + country as primary key
    # result columns: price, load, solar, wind
    df_pivot = (df_clean.groupBy("timestamp", "country")
                .pivot("metric_group")
                .agg(F.avg("value"))
                .na.fill(0, subset=["solar", "wind"]))  # missing generation is treated as 0

    # Physical features (ramp rate, dunkelflaute, volatility)
    print("Calculating physical features (ramp rate, dunkelflaute, volatility)...")

    # Residual = Load - (Solar + Wind)
    df_features = (df_pivot
                   .withColumn("renewable_gen", F.col("solar") + F.col("wind"))
                   .withColumn("residual_load", F.col("load") - F.col("renewable_gen")))

    # define window: partition by country, order by time
    w_country_time = Window.partitionBy("country").orderBy("timestamp")

    # define window: past 24 hours (for volatility and dunkelflaute)
    w_rolling_24h = w_country_time.rowsBetween(-23, 0)

    df_features = (
        df_features
        # add row number to identify boundary cases
        .withColumn("row_num", F.row_number().over(w_country_time))
        .withColumn("is_window_valid", F.col("row_num") >= 24)

        .withColumn("prev_load", F.lag("load", 1).over(w_country_time))
        .withColumn("load_diff", F.col("load") - F.col("prev_load"))

        # Ramp rate, if change amount(abs) > 10% of total load, mark as extreme_ramp
        .withColumn("ramp_rate_pct", F.abs(F.col("load_diff") / F.col("load")))
        .withColumn("is_extreme_ramp", F.when(F.col("ramp_rate_pct") > 0.1, True).otherwise(False))

        # Volatility 24h
        # first 23 rows window is incomplete, set to null
        .withColumn("volatility_24h_raw", F.stddev("price").over(w_rolling_24h))
        .withColumn(
            "volatility_24h",
            F.when(F.col("is_window_valid"), F.col("volatility_24h_raw")).otherwise(F.lit(None)))

        # Dunkelflaute, continuous 24 hours (Wind+Solar)/Load < 10%
        # boundary processing: the first 23 rows window is incomplete, set to null
        .withColumn("renewable_share", F.col("renewable_gen") / F.col("load"))
        .withColumn("max_share_last_24h", F.max("renewable_share").over(w_rolling_24h))
        .withColumn(
            "is_dunkelflaute",
            F.when(~F.col("is_window_valid"), F.lit(None))
            .when(F.col("max_share_last_24h") < 0.1, True)
            .otherwise(False))

        # drop temporary columns
        .drop("volatility_24h_raw", "row_num")
    )

    # add time dimension columns (for partitioning)
    df_features_final = (
        df_features
        .withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("day", F.dayofmonth("timestamp"))
        .withColumn("weekday", F.dayofweek("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
    )

    # calculate statistical baselines
    print("Calculating Statistical Baselines...")

    # Country + Month + Weekday + Hour
    df_stats = (
        df_features_final.groupBy("country", "month", "weekday", "hour")
        .agg(
            F.avg("price").alias("avg_price_baseline"),
            F.stddev("price").alias("stddev_price_baseline"),
            F.count("price").alias("sample_size")
        )
    )

    # write feature data
    print(f"Writing Feature Data to {OUTPUT_PATH_SILVER_FEATURES}...")
    df_features_final.write \
        .mode("overwrite") \
        .partitionBy("country", "year") \
        .parquet(OUTPUT_PATH_SILVER_FEATURES)

    # write statistical baseline
    print(f"Writing Statistical Baseline to {OUTPUT_PATH_SILVER_STATS}...")
    df_stats.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .parquet(OUTPUT_PATH_SILVER_STATS)

    print("Silver Layer Transformation Completed Successfully!")
    spark.stop()


if __name__ == "__main__":
    run_silver_transformation()
