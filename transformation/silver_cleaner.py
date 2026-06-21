"""
transformation/silver_cleaner.py

Builds the Silver layer of the Olist pipeline.

The Bronze layer keeps data close to the source files. This layer combines
those datasets into a business-ready table that can be used for reporting
and Gold aggregations.

Main responsibilities:
    - Read Bronze datasets
    - Aggregate payments to avoid duplicate orders
    - Join customer, product, seller, payment, and review data
    - Create delivery and revenue metrics
    - Add reporting-friendly date attributes
    - Write a partitioned Silver dataset

Output grain:
    One row per order item.
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys

sys.path.append("/Workspace/Users/dharmikpatel982003@gmail.com/project_sales")

from Utils.config import bronze_path, silver_path
from Utils.spark_session import get_spark


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger("silver_cleaner")


def read_bronze(spark: SparkSession) -> dict:
    """
    Read all Bronze datasets.

    Schemas were already enforced during ingestion, so the Parquet
    files can be loaded directly.
    """

    log.info("Reading Bronze tables")

    tables = {}

    for name in [
        "orders",
        "customers",
        "items",
        "payments",
        "reviews",
        "products",
        "sellers",
        "translations",
    ]:
        df = spark.read.format("delta").load(f"{bronze_path}/{name}")
        tables[name] = df
        log.info(f"{name} loaded")

    return tables


def aggregate_payments(payments: DataFrame) -> DataFrame:
    """
    Aggregate payments to one row per order.

    Orders can have multiple payment records, such as credit card plus
    voucher. Aggregating first prevents revenue from being overstated
    after joining payments with order items.
    """

    log.info("Aggregating payments to order level")

    payment_summary = (
        payments
        .groupBy("order_id")
        .agg(
            F.round(F.sum("payment_value"), 2).alias("total_payment_value"),
            F.max("payment_installments").alias("max_installments"),
            F.collect_set("payment_type").alias("payment_types"),
            F.count("*").alias("payment_rows_count"),
        )
    )

    log.info(f"Payment rows after aggregation: {payment_summary.count():,}")

    return payment_summary


def translate_categories(
    products: DataFrame,
    translations: DataFrame
) -> DataFrame:
    """
    Add English product category names.

    Most reporting is easier in English. If a translation is missing,
    the original category name is kept.
    """

    return (
        products
        .join(translations, on="product_category_name", how="left")
        .withColumn(
            "category_english",
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name")
            )
        )
        .drop("product_category_name_english")
    )


def drop_ingestion_columns(df: DataFrame) -> DataFrame:
    """
    Remove Bronze load metadata before building the Silver table.

    The metadata is useful for tracing raw loads, but keeping it from
    every joined table would add duplicate columns and noise.
    """

    return df.drop(
        "_ingestion_ts",
        "_source_file",
        "_batch_id"
    )


def build_wide_table(
    tables: dict,
    payments_agg: DataFrame
) -> DataFrame:
    """
    Join Bronze datasets into one wide Silver table.

    Orders act as the base table. The items join is the only join that
    increases row count because one order can contain multiple products.

    Left joins are used so missing dimension records do not remove orders
    from the dataset.
    """

    log.info("Building wide Silver table")

    order_metadata = (
        tables["orders"]
        .select(
            "order_id",
            "_ingestion_ts",
            "_source_file",
            "_batch_id"
        )
        .distinct()
    )

    orders = drop_ingestion_columns(tables["orders"])
    customers = drop_ingestion_columns(tables["customers"])
    items = drop_ingestion_columns(tables["items"])

    products_translated = translate_categories(
        tables["products"],
        tables["translations"]
    )
    products = drop_ingestion_columns(products_translated)

    sellers = drop_ingestion_columns(tables["sellers"])
    reviews = drop_ingestion_columns(tables["reviews"])

    silver = (
        orders
        .join(customers, on="customer_id", how="left")
        .join(items, on="order_id", how="left")
        .join(products, on="product_id", how="left")
        .join(sellers, on="seller_id", how="left")
        .join(payments_agg, on="order_id", how="left")
        .join(
            reviews.select(
                "order_id",
                "review_score",
                "review_comment_message",
                "review_creation_date"
            ),
            on="order_id",
            how="left"
        )
        .join(order_metadata, on="order_id", how="left")
    )

    log.info(
        f"Wide Silver table built with {silver.count():,} rows "
        f"and {len(silver.columns)} columns"
    )

    return silver


def add_business_columns(df: DataFrame) -> DataFrame:
    """
    Add business-friendly columns used by Gold models and reporting.
    """

    log.info("Adding delivery, revenue, date, and review metrics")

    return (
        df

        # Difference between actual and promised delivery date.
        # Positive values indicate late deliveries.
        .withColumn(
            "delivery_delay_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date")
            )
        )

        # Number of days from purchase until the customer receives the order.
        .withColumn(
            "fulfillment_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp")
            )
        )

        # Used later to calculate late delivery rates.
        .withColumn(
            "is_late_delivery",
            F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)
        )

        .withColumn(
            "total_item_revenue",
            F.round(
                F.coalesce(F.col("price"), F.lit(0)) +
                F.coalesce(F.col("freight_value"), F.lit(0)),
                2
            )
        )

        # Helps identify products where shipping makes up a large share
        # of the total purchase cost.
        .withColumn(
            "freight_pct_of_price",
            F.round(
                F.when(
                    F.col("price") > 0,
                    F.col("freight_value") / F.col("price") * 100
                ).otherwise(None),
                2
            )
        )

        # Date fields used for partitioning and trend analysis.
        .withColumn("order_year", F.year("order_purchase_timestamp"))
        .withColumn("order_month", F.month("order_purchase_timestamp"))
        .withColumn("order_dayofweek", F.dayofweek("order_purchase_timestamp"))

        # Simple sentiment bucket based on review score.
        .withColumn(
            "review_sentiment",
            F.when(F.col("review_score") >= 4, "positive")
             .when(F.col("review_score") == 3, "neutral")
             .when(F.col("review_score") <= 2, "negative")
             .otherwise("no_review")
        )
    )


def write_silver(df: DataFrame) -> None:
    """
    Write the final Silver dataset.

    Partitioning by year and month keeps reads efficient for most
    reporting workloads.
    """

    log.info("Writing Silver dataset")

    (
        df
        .write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .format("delta")
        .save(silver_path)
    )

    log.info(f"Silver written to {silver_path}")


def sanity_check(silver: DataFrame) -> None:
    """
    Print a few quick checks after the Silver layer is built.

    This is useful when running the file directly during development.
    """

    print("\nSample delivered orders")
    (
        silver
        .filter(F.col("order_status") == "delivered")
        .select(
            "order_id",
            "delivery_delay_days",
            "fulfillment_days",
            "is_late_delivery",
            "total_item_revenue",
            "review_sentiment"
        )
        .show(5, truncate=False)
    )

    print("\nLate delivery rate")
    (
        silver
        .filter(F.col("order_status") == "delivered")
        .agg(
            F.countDistinct("order_id").alias("delivered_orders"),
            F.sum("is_late_delivery").alias("late_order_items"),
            F.round(F.avg("is_late_delivery") * 100, 2).alias("late_pct")
        )
        .show()
    )

    print("\nRevenue by review sentiment")
    (
        silver
        .groupBy("review_sentiment")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.avg("total_item_revenue"), 2).alias("avg_item_revenue"),
            F.round(F.sum("total_item_revenue"), 2).alias("total_revenue")
        )
        .orderBy("review_sentiment")
        .show()
    )


def run_silver_cleaning(spark: SparkSession = None) -> DataFrame:
    """
    Run the full Silver transformation.

    Pipeline flow:
        Bronze tables
            -> payment aggregation
            -> wide order-item table
            -> business metrics
            -> partitioned Silver dataset
    """

    if spark is None:
        spark = get_spark("OlistSilverCleaner")

    log.info("=" * 60)
    log.info("Starting Silver transformation")
    log.info("=" * 60)

    tables = read_bronze(spark)

    payments_agg = aggregate_payments(tables["payments"])

    silver = build_wide_table(tables, payments_agg)
    silver = add_business_columns(silver)

    silver_rows = silver.count()
    distinct_orders = silver.select("order_id").distinct().count()

    log.info(f"Silver order item rows: {silver_rows:,}")
    log.info(f"Distinct orders in Silver: {distinct_orders:,}")
    log.info(f"Silver columns: {len(silver.columns)}")

    write_silver(silver)

    log.info("=" * 60)
    log.info("Silver transformation complete")
    log.info("=" * 60)

    return silver


if __name__ == "__main__":
    spark_session = get_spark("OlistSilverCleaner")
    silver_df = run_silver_cleaning(spark_session)
    sanity_check(silver_df)