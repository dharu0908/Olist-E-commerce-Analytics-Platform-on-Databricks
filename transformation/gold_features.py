"""
transformation/gold_features.py

Builds the Gold layer of the Olist pipeline.

Gold tables are business-ready datasets created from Silver.
These outputs are used by the analytics/reporting layer.
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

sys.path.append("/Workspace/Users/dharmikpatel982003@gmail.com/project_sales")
from Utils.config import silver_path, gold_path
from Utils.spark_session import get_spark


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger("gold_features")


def read_silver(spark: SparkSession) -> DataFrame:
    """Read the cleaned Silver dataset."""

    log.info("Reading Silver dataset")
    return spark.read.parquet(silver_path)


def build_rfm_customers(silver: DataFrame) -> DataFrame:
    """
    Customer level summary used for segmentation.

    Recency = days since last purchase
    Frequency = number of orders
    Monetary = total spend
    """

    log.info("Building customer RFM table")

    max_order_date = silver.agg(
        F.max("order_purchase_timestamp")
    ).collect()[0][0]

    return (
        silver
        .filter(F.col("order_status") == "delivered")
        .groupBy("customer_unique_id", "customer_state")
        .agg(
            F.datediff(
                F.lit(max_order_date),
                F.max("order_purchase_timestamp")
            ).alias("recency_days"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("total_item_revenue"), 2).alias("monetary"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score")
        )
        .withColumn(
            "customer_segment",
            F.when(
                (F.col("frequency") >= 3) & (F.col("monetary") >= 500),
                "champion"
            )
            .when(F.col("frequency") >= 2, "loyal")
            .when(F.col("monetary") >= 300, "high_value")
            .when(F.col("recency_days") <= 60, "recent")
            .otherwise("regular")
        )
    )


def build_seller_scores(silver: DataFrame) -> DataFrame:
    """
    Seller performance table.

    Combines revenue, order volume, customer rating, and delivery quality
    into one dataset.
    """

    log.info("Building seller performance table")

    sellers = (
        silver
        .filter(F.col("seller_id").isNotNull())
        .groupBy("seller_id", "seller_state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_item_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
            F.round(F.avg("is_late_delivery") * 100, 2).alias("late_delivery_pct")
        )
    )

    revenue_window = Window.orderBy(F.desc("total_revenue"))

    return (
        sellers
        .withColumn("revenue_rank", F.rank().over(revenue_window))
        .withColumn(
            "seller_tier",
            F.when(F.col("revenue_rank") <= 100, "top_seller")
             .when(F.col("revenue_rank") <= 500, "mid_seller")
             .otherwise("long_tail")
        )
    )


def build_category_stats(silver: DataFrame) -> DataFrame:
    """
    Category level performance table.

    Used to compare product categories by revenue, review score,
    and delivery performance.
    """

    log.info("Building category performance table")

    categories = (
        silver
        .groupBy("category_english")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_item_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("total_item_revenue"), 2).alias("avg_order_value"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
            F.round(F.avg("is_late_delivery") * 100, 2).alias("late_delivery_pct")
        )
        .withColumnRenamed(
            "category_english",
            "product_category_name_english"
        )
    )

    revenue_window = Window.orderBy(F.desc("total_revenue"))

    return categories.withColumn(
        "revenue_rank",
        F.rank().over(revenue_window)
    )


def build_monthly_trends(silver: DataFrame) -> DataFrame:
    """
    Monthly revenue and delivery trend table.

    This is useful for dashboarding and high-level business reporting.
    """

    log.info("Building monthly trend table")

    monthly = (
        silver
        .groupBy("order_year", "order_month")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_item_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("total_item_revenue"), 2).alias("avg_order_value"),
            F.round(F.avg("is_late_delivery") * 100, 2).alias("late_pct")
        )
    )

    month_window = Window.orderBy("order_year", "order_month")

    return (
        monthly
        .withColumn(
            "previous_month_revenue",
            F.lag("total_revenue").over(month_window)
        )
        .withColumn(
            "revenue_mom_growth_pct",
            F.round(
                (
                    (F.col("total_revenue") - F.col("previous_month_revenue"))
                    / F.col("previous_month_revenue")
                ) * 100,
                2
            )
        )
        .drop("previous_month_revenue")
    )


def write_gold_table(df: DataFrame, table_name: str) -> None:
    """Write one Gold table to Parquet."""

    output_path = f"{gold_path}/{table_name}"

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .save(output_path)
    )

    log.info(f"{table_name} written to {output_path}")


def run_gold_features(spark: SparkSession = None) -> None:
    """Run all Gold transformations."""

    if spark is None:
        spark = get_spark("OlistGoldFeatures")

    log.info("=" * 60)
    log.info("Starting Gold layer")
    log.info("=" * 60)

    silver = read_silver(spark)

    rfm_customers = build_rfm_customers(silver)
    seller_scores = build_seller_scores(silver)
    category_stats = build_category_stats(silver)
    monthly_trends = build_monthly_trends(silver)

    write_gold_table(rfm_customers, "rfm_customers")
    write_gold_table(seller_scores, "seller_scores")
    write_gold_table(category_stats, "category_stats")
    write_gold_table(monthly_trends, "monthly_trends")

    log.info(f"RFM customers: {rfm_customers.count():,}")
    log.info(f"Seller scores: {seller_scores.count():,}")
    log.info(f"Category stats: {category_stats.count():,}")
    log.info(f"Monthly trends: {monthly_trends.count():,}")

    log.info("=" * 60)
    log.info("Gold layer complete")
    log.info("=" * 60)


if __name__ == "__main__":
    spark_session = get_spark("OlistGoldFeatures")
    run_gold_features(spark_session)