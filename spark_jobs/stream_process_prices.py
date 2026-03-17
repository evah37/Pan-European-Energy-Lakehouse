from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, month, hour, weekday, when
)
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, StringType, TimestampType
)

# Scale factors for baseline adjustment
SCALE_FACTOR_MEAN = 1.0
SCALE_FACTOR_STD = 1.0


def main():
    # initialize spark session
    spark = SparkSession.builder \
        .appName("AwattarRealTimeStream") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://energy-lake/checkpoints/awattar_stream") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # MinIO configuration
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio123")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    # load historical baselines from dbt Gold Layer
    print("Loading baselines from fct_price_baselines...")

    baselines_df = spark.read.parquet("s3a://energy-lake/gold/fct_price_baselines.parquet")

    # extract baseline lookup table
    baseline_lookup = baselines_df \
        .select(
            col("country_code"),
            col("month_of_year").alias("base_month"),
            col("day_of_week").alias("base_dow"),
            col("hour_of_day").alias("base_hour"),
            col("avg_price_baseline").alias("hist_mean"),
            col("stddev_price_baseline").alias("hist_std")
        ) \
        .distinct() \
        .cache()

    print(f"Loaded {baseline_lookup.count()} baseline patterns.")

    # read kafka stream
    print("Starting Stream from Redpanda...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "redpanda:9092") \
        .option("subscribe", "energy-market-live") \
        .option("startingOffsets", "earliest") \
        .load()

    # parse JSON
    schema = StructType([
        StructField("timestamp_ms", LongType()),
        StructField("price_eur_mwh", DoubleType()),
        StructField("region", StringType()),
        StructField("ingested_at", DoubleType())
    ])

    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # feature engineering, align dbt time logic
    # use weekday(col) + 1 to match dbt's 1-7 (Mon-Sun) logic

    # map producer region to historical data country_code
    processed_df = parsed_df \
        .withColumn("event_time", (col("timestamp_ms") / 1000).cast(TimestampType())) \
        .withColumn("base_month", month(col("event_time"))) \
        .withColumn("base_hour", hour(col("event_time"))) \
        .withColumn("base_dow", (weekday(col("event_time")) + 1)) \
        .withColumn("country_code",
                    when(col("region") == "DE", "DE_LU")
                    .when(col("region") == "AT", "AT")
                    .otherwise(col("region")))

    # join with dbt baselines
    # use four keys for join country_code + time pattern
    enriched_df = processed_df.join(
        baseline_lookup,
        on=["country_code", "base_month", "base_dow", "base_hour"],
        how="left"
    )

    # calculate Z-Score and anomaly level
    final_df = (
        enriched_df
        .withColumn("scaled_mean", col("hist_mean") * SCALE_FACTOR_MEAN)
        .withColumn("scaled_std", col("hist_std") * SCALE_FACTOR_STD)
        .withColumn(
            "z_score",
            # Z = (real value - adjusted mean) / adjusted standard deviation
            (col("price_eur_mwh") - col("scaled_mean")) / col("scaled_std")
        ).withColumn(
            "anomaly_level",
            when(col("z_score") > 3, "CRITICAL")  # > +3σ
            .when(col("z_score") > 2, "WARNING")  # > +2σ
            .when(col("z_score") < -2, "OPPORTUNITY")  # < -2σ (negative price or very low price)
            .otherwise("NORMAL")
        ).select(
            col("event_time"),
            col("region"),
            col("price_eur_mwh"),
            # save adjusted baseline into parquet
            col("scaled_mean").alias("expected_price"),
            col("z_score"),
            col("anomaly_level")
        )
    )

    # write to MinIO (Real-time Layer)
    query = final_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "s3a://energy-lake/realtime/market_monitor") \
        .option("checkpointLocation", "s3a://energy-lake/checkpoints/market_monitor_v2") \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
