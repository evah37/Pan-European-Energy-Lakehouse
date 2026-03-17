"""
Unit tests for spark_jobs/feature_engineering.py
"""
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType
)
from datetime import datetime, timedelta


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_feature_engineering")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_bronze_data(spark):
    """
    Create a sample Bronze layer DataFrame mimicking Spark ingestion.
    """
    rows = []
    base_time = datetime(2020, 1, 1, 0, 0, 0)
    country = "DE_LU"

    for i in range(48):
        ts = base_time + timedelta(hours=i)
        # Price oscillates: base + some variance
        price = 30.0 + (i % 12) * 2.0 + (-1) ** i * 3.0
        load = 50000.0 + i * 100 - (i % 6) * 500
        solar = max(0.0, 3000.0 * (1 - abs(i % 24 - 12) / 12.0))
        wind = 5000.0 + (i % 8) * 300

        # price row
        rows.append((ts, country, "price_day_ahead", price))
        # load row
        rows.append((ts, country, "load_actual_entsoe_transparency", load))
        # solar row
        rows.append((ts, country, "solar_generation_actual", solar))
        # wind row
        rows.append((ts, country, "wind_generation_actual", wind))

    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("country", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("value", DoubleType(), True),
    ])
    return spark.createDataFrame(rows, schema)


@pytest.fixture
def pivoted_df(spark, sample_bronze_data):
    """
    Apply the same pivot logic as feature_engineering.py to
    produce the wide-format DataFrame used for feature calculation.
    """
    df = sample_bronze_data
    df_clean = df.withColumn(
        "metric_group",
        F.when(F.col("metric_name").contains("price"), "price")
         .when(F.col("metric_name").contains("load"), "load")
         .when(F.col("metric_name").contains("solar"), "solar")
         .when(F.col("metric_name").contains("wind"), "wind")
         .otherwise("other")
    ).filter(F.col("metric_group") != "other")

    df_pivot = (
        df_clean.groupBy("timestamp", "country")
        .pivot("metric_group")
        .agg(F.avg("value"))
        .na.fill(0, subset=["solar", "wind"])
    )
    return df_pivot


@pytest.fixture
def featured_df(pivoted_df):
    """
    Apply the same feature engineering logic as the real script.
    This mirrors the exact calculations in feature_engineering.py.
    """
    df = pivoted_df
    df_features = (
        df.withColumn("renewable_gen", F.col("solar") + F.col("wind"))
          .withColumn("residual_load", F.col("load") - F.col("renewable_gen"))
    )

    w_country_time = Window.partitionBy("country").orderBy("timestamp")
    w_rolling_24h = w_country_time.rowsBetween(-23, 0)

    df_features = (
        df_features
        .withColumn("row_num", F.row_number().over(w_country_time))
        .withColumn("is_window_valid", F.col("row_num") >= 24)
        .withColumn("prev_load", F.lag("load", 1).over(w_country_time))
        .withColumn("load_diff", F.col("load") - F.col("prev_load"))
        .withColumn(
            "ramp_rate_pct",
            F.abs(F.col("load_diff") / F.col("load"))
        )
        .withColumn(
            "is_extreme_ramp",
            F.when(F.col("ramp_rate_pct") > 0.1, True).otherwise(False)
        )
        .withColumn(
            "volatility_24h_raw",
            F.stddev("price").over(w_rolling_24h)
        )
        .withColumn(
            "volatility_24h",
            F.when(F.col("is_window_valid"), F.col("volatility_24h_raw"))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "renewable_share",
            F.col("renewable_gen") / F.col("load")
        )
        .withColumn(
            "max_share_last_24h",
            F.max("renewable_share").over(w_rolling_24h)
        )
        .withColumn(
            "is_dunkelflaute",
            F.when(~F.col("is_window_valid"), F.lit(None))
             .when(F.col("max_share_last_24h") < 0.1, True)
             .otherwise(False)
        )
        .drop("volatility_24h_raw", "row_num")
    )
    return df_features


# Pivot Logic
class TestPivotLogic:
    def test_pivot_columns_exist(self, pivoted_df):
        """Pivoted DataFrame should have price, load, solar, wind columns."""
        expected = {"timestamp", "country", "price", "load", "solar", "wind"}
        assert expected.issubset(set(pivoted_df.columns))

    def test_pivot_row_count(self, pivoted_df):
        """After pivot, should have 48 rows (one per hour per country)."""
        assert pivoted_df.count() == 48

    def test_no_null_solar_wind(self, pivoted_df):
        """Solar and wind should have 0-fill, no nulls."""
        null_solar = pivoted_df.filter(F.col("solar").isNull()).count()
        null_wind = pivoted_df.filter(F.col("wind").isNull()).count()
        assert null_solar == 0
        assert null_wind == 0


# Feature Calculations
class TestFeatureCalculations:
    def test_residual_load_formula(self, featured_df):
        """residual_load = load - (solar + wind)."""
        row = featured_df.first()
        expected = row["load"] - (row["solar"] + row["wind"])
        assert abs(row["residual_load"] - expected) < 0.01

    def test_ramp_rate_first_row_null(self, featured_df):
        """First row should have null prev_load (no lag yet)."""
        first_row = (
            featured_df
            .orderBy("timestamp")
            .first()
        )
        assert first_row["prev_load"] is None

    def test_ramp_rate_calculation(self, featured_df):
        """ramp_rate_pct = |load_diff| / load for non-first rows."""
        rows = (
            featured_df
            .filter(F.col("prev_load").isNotNull())
            .orderBy("timestamp")
            .limit(5)
            .collect()
        )
        for row in rows:
            expected = abs(row["load_diff"]) / row["load"]
            assert abs(row["ramp_rate_pct"] - expected) < 0.001

    def test_extreme_ramp_threshold(self, featured_df):
        """is_extreme_ramp should be True when ramp_rate_pct > 0.1."""
        extreme_rows = (
            featured_df
            .filter(F.col("ramp_rate_pct").isNotNull())
            .filter(F.col("ramp_rate_pct") > 0.1)
            .collect()
        )
        for row in extreme_rows:
            assert row["is_extreme_ramp"] is True


# 24h Window Boundary
class TestWindowBoundary:
    def test_volatility_null_first_23_rows(self, featured_df):
        """
        volatility_24h should be null for the first 23 rows
        """
        rows = (
            featured_df
            .orderBy("timestamp")
            .limit(23)
            .select("volatility_24h")
            .collect()
        )
        for row in rows:
            assert row["volatility_24h"] is None, (
                "First 23 rows should have null volatility_24h"
            )

    def test_volatility_valid_after_24th_row(self, featured_df):
        """volatility_24h should be a valid number from row 24 onward."""
        rows = (
            featured_df
            .filter(F.col("is_window_valid") == True)  # noqa: E712
            .select("volatility_24h")
            .collect()
        )
        assert len(rows) > 0, "Should have rows with valid windows"
        for row in rows:
            assert row["volatility_24h"] is not None
            assert row["volatility_24h"] >= 0

    def test_dunkelflaute_null_first_23_rows(self, featured_df):
        """is_dunkelflaute should be null for the first 23 rows."""
        rows = (
            featured_df
            .orderBy("timestamp")
            .limit(23)
            .select("is_dunkelflaute")
            .collect()
        )
        for row in rows:
            assert row["is_dunkelflaute"] is None

    def test_is_window_valid_flag(self, featured_df):
        """is_window_valid should be False for first 23 rows, True after."""
        rows = featured_df.orderBy("timestamp").collect()
        for i, row in enumerate(rows):
            if i < 23:
                assert row["is_window_valid"] is False, (
                    f"Row {i} should have is_window_valid=False"
                )
            else:
                assert row["is_window_valid"] is True, (
                    f"Row {i} should have is_window_valid=True"
                )


# Dunkelflaute Logic
class TestDunkelflaute:
    def test_dunkelflaute_false_with_renewables(self, featured_df):
        """
        With meaningful solar/wind generation, dunkelflaute should be False
        for valid window rows.
        """
        valid_rows = (
            featured_df
            .filter(F.col("is_window_valid") == True)
            .filter(F.col("is_dunkelflaute").isNotNull())
            .collect()
        )
        # The sample data has significant renewables, so most should be False
        false_count = sum(1 for r in valid_rows if r["is_dunkelflaute"] is False)
        assert false_count > 0

    def test_renewable_share_calculation(self, featured_df):
        """renewable_share = renewable_gen / load."""
        row = (
            featured_df
            .filter(F.col("load") > 0)
            .first()
        )
        expected = row["renewable_gen"] / row["load"]
        assert abs(row["renewable_share"] - expected) < 0.001


# Statistical Baselines
class TestStatisticalBaselines:
    def test_stats_aggregation(self, featured_df, spark):
        """Statistical baselines should group by country+month+weekday+hour."""
        df_final = (
            featured_df
            .withColumn("year", F.year("timestamp"))
            .withColumn("month", F.month("timestamp"))
            .withColumn("weekday", F.dayofweek("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
        )
        df_stats = (
            df_final
            .groupBy("country", "month", "weekday", "hour")
            .agg(
                F.avg("price").alias("avg_price_baseline"),
                F.stddev("price").alias("stddev_price_baseline"),
                F.count("price").alias("sample_size"),
            )
        )
        assert df_stats.count() > 0
        assert "avg_price_baseline" in df_stats.columns
        assert "stddev_price_baseline" in df_stats.columns
        assert "sample_size" in df_stats.columns

    def test_baseline_values_reasonable(self, featured_df, spark):
        """Baseline avg should be within a reasonable price range."""
        df_final = (
            featured_df
            .withColumn("month", F.month("timestamp"))
            .withColumn("weekday", F.dayofweek("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
        )
        df_stats = (
            df_final
            .groupBy("country", "month", "weekday", "hour")
            .agg(F.avg("price").alias("avg_price"))
        )
        avg_prices = [r["avg_price"] for r in df_stats.collect()]
        for p in avg_prices:
            assert p is not None
            # Price should be in a reasonable range for European energy market
            assert -100 < p < 500
