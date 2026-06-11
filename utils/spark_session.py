"""
utils/spark_session.py

Shared Spark session used across the project.

Keeping Spark session creation in one place makes it easier to
manage configuration changes and avoids creating multiple sessions
across different pipeline layers.
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "OlistEcommercePlatform") -> SparkSession:
    """
    Create or return an existing Spark session.

    Databricks usually provides a Spark session automatically.
    getOrCreate() reuses the existing session if one is already available.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)

        # Handle legacy timestamp formats found in source files.
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

        # Compression used when writing Parquet files.
        .config("spark.sql.parquet.compression.codec", "snappy")

        # Let Spark optimize joins and shuffle operations at runtime.
        .config("spark.sql.adaptive.enabled", "true")

        # Reduce unnecessary small partitions after shuffles.
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )

        # Default number of partitions used during shuffle operations.
        .config("spark.sql.shuffle.partitions", "200")

        .getOrCreate()
    )

    return spark