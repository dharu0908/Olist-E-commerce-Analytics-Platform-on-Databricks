"""
ingestion/bronze_loader.py

Bronze layer for the Olist pipeline.

This file reads the raw CSV files, applies predefined schemas,
adds simple ingestion metadata, runs basic checks, and writes the
result as Parquet.

Bronze is kept close to the source data. Heavy business logic belongs
in Silver and Gold.
"""
import sys

sys.path.append("/Workspace/Users/dharmikpatel982003@gmail.com/project_sales")
import logging
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from Utils.config import (
    raw_path,
    csv_files,
    ALL_SCHEMAS,
    NOT_NULL_COLUMNS,
    MIN_ROW_COUNTS,
    bronze_path,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger("bronze_loader")

BATCH_ID = str(uuid.uuid4())[:8]


def read_csv(spark: SparkSession, table_name: str, file_name: str):
    """Read one raw CSV file using the schema defined in config.py."""

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(ALL_SCHEMAS[table_name])
        .load(f"{raw_path}/{file_name}")
    )


def add_metadata(df, source_file: str):
    """
    Add basic load metadata.

    These fields make it easier to trace when the data was loaded
    and which source file it came from.
    """

    return (
        df.withColumn("_ingestion_ts", F.current_timestamp())
          .withColumn("_source_file", F.lit(source_file))
          .withColumn("_batch_id", F.lit(BATCH_ID))
    )


def validate(df, table_name: str):
    """
    Run lightweight checks before writing Bronze.

    This is not meant to be a full data quality framework.
    It catches obvious issues like missing join keys or incomplete files.
    """

    key_columns = NOT_NULL_COLUMNS.get(table_name, [])

    if key_columns:
        null_exprs = [
            F.count(F.when(F.col(col_name).isNull(), col_name)).alias(col_name)
            for col_name in key_columns
        ]

        null_counts = df.select(null_exprs).collect()[0].asDict()

        for col_name, null_count in null_counts.items():
            if null_count > 0:
                log.warning(
                    f"{table_name}.{col_name} has {null_count:,} null values"
                )
    else:
        null_counts = {}

    row_count = df.count()
    min_required = MIN_ROW_COUNTS.get(table_name)

    if min_required and row_count < min_required:
        log.warning(
            f"{table_name} has only {row_count:,} rows. "
            f"Expected at least {min_required:,}."
        )

    return null_counts, row_count


def load_all_tables(spark: SparkSession):
    """Read, tag, and validate every raw dataset."""

    dataframes = {}

    for table_name, file_name in csv_files.items():
        log.info(f"Reading {table_name}")

        df = read_csv(spark, table_name, file_name)
        df = add_metadata(df, file_name)

        _, row_count = validate(df, table_name)

        dataframes[table_name] = df

        log.info(f"{table_name} loaded with {row_count:,} rows")

    return dataframes


def clean_dataframes(dataframes: dict):
    """
    Apply small Bronze-level cleanup.

    Bronze should stay mostly raw, so only remove records that are clearly
    unusable for the rest of the pipeline.
    """

    if "reviews" in dataframes:
        log.info("Removing review rows without order_id")

        dataframes["reviews"] = dataframes["reviews"].filter(
            F.col("order_id").isNotNull()
        )

    return dataframes


def write_bronze(dataframes: dict):
    """Write each Bronze table as Parquet."""

    for table_name, df in dataframes.items():
        output_path = f"{bronze_path}/{table_name}"

        (
            df.write
            .mode("overwrite")
            .format("delta")
            .save(output_path)
        )

        log.info(f"{table_name} written to {output_path}")


def print_summary(dataframes: dict):
    """Print a simple load summary for the pipeline logs."""

    log.info("=" * 60)
    log.info(f"Bronze batch complete | batch_id={BATCH_ID}")
    log.info("=" * 60)

    total_rows = 0

    for table_name, df in dataframes.items():
        row_count = df.count()
        total_rows += row_count
        log.info(f"{table_name}: {row_count:,} rows")

    log.info(f"Total Bronze rows: {total_rows:,}")


def run_bronze_ingestion(spark: SparkSession):
    """Run the full Bronze ingestion step."""

    start = datetime.now()

    log.info("=" * 60)
    log.info("Starting Bronze ingestion")
    log.info("=" * 60)

    dataframes = load_all_tables(spark)
    dataframes = clean_dataframes(dataframes)

    write_bronze(dataframes)
    print_summary(dataframes)

    elapsed = (datetime.now() - start).seconds
    print(f"Bronze ingestion completed in {elapsed}s")