"""
serving/analytics.py

Runs business-facing analytics on top of the Gold layer.

The Gold tables are already prepared for reporting. This file registers
them as temporary SQL views and runs a few example queries that could feed
a dashboard or be used for business analysis.
"""

import logging

from pyspark.sql import SparkSession, DataFrame

import sys

sys.path.append("/Workspace/Users/dharmikpatel982003@gmail.com/project_sales")

from Utils.config import gold_path
from Utils.spark_session import get_spark


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger("analytics")


def register_views(spark: SparkSession) -> None:
    """
    Register Gold datasets as Spark SQL views.

    This keeps the reporting layer simple and makes it easy to write
    analytics using SQL instead of PySpark transformations.
    """

    log.info("Registering Gold tables as SQL views")

    spark.read.parquet(f"{gold_path}/rfm_customers") \
        .createOrReplaceTempView("rfm_customers")

    spark.read.parquet(f"{gold_path}/seller_scores") \
        .createOrReplaceTempView("seller_scores")

    spark.read.parquet(f"{gold_path}/category_stats") \
        .createOrReplaceTempView("category_stats")

    spark.read.parquet(f"{gold_path}/monthly_trends") \
        .createOrReplaceTempView("monthly_trends")

    log.info("Gold SQL views registered")


def q_customer_segments(spark: SparkSession) -> DataFrame:
    """Customer segments with spend, recency, and review behaviour."""

    return spark.sql("""
        SELECT
            customer_segment,
            COUNT(*) AS customer_count,
            ROUND(AVG(monetary), 2) AS avg_spend,
            ROUND(AVG(recency_days), 1) AS avg_recency_days,
            ROUND(AVG(avg_review_score), 2) AS avg_review_score
        FROM rfm_customers
        GROUP BY customer_segment
        ORDER BY avg_spend DESC
    """)


def q_top_sellers(spark: SparkSession, limit_count: int = 10) -> DataFrame:
    """Highest revenue sellers with delivery and review metrics."""

    return spark.sql(f"""
        SELECT
            seller_id,
            seller_state,
            seller_tier,
            total_orders,
            total_revenue,
            avg_review_score,
            late_delivery_pct,
            revenue_rank
        FROM seller_scores
        ORDER BY revenue_rank
        LIMIT {limit_count}
    """)


def q_top_categories(spark: SparkSession, limit_count: int = 15) -> DataFrame:
    """Top product categories by revenue."""

    return spark.sql(f"""
        SELECT
            product_category_name_english,
            total_orders,
            total_revenue,
            avg_order_value,
            avg_review_score,
            late_delivery_pct,
            revenue_rank
        FROM category_stats
        ORDER BY revenue_rank
        LIMIT {limit_count}
    """)


def q_monthly_growth(spark: SparkSession) -> DataFrame:
    """Monthly order, revenue, AOV, delivery, and growth trend."""

    return spark.sql("""
        SELECT
            order_year,
            order_month,
            total_orders,
            total_revenue,
            avg_order_value,
            late_pct,
            revenue_mom_growth_pct
        FROM monthly_trends
        ORDER BY order_year, order_month
    """)


def q_late_delivery_by_state(spark: SparkSession) -> DataFrame:
    """Seller states ranked by average late delivery rate."""

    return spark.sql("""
        SELECT
            seller_state,
            COUNT(*) AS total_sellers,
            ROUND(AVG(late_delivery_pct), 2) AS avg_late_pct,
            ROUND(AVG(avg_review_score), 2) AS avg_review_score,
            ROUND(SUM(total_revenue), 2) AS total_revenue
        FROM seller_scores
        GROUP BY seller_state
        ORDER BY avg_late_pct DESC
    """)


def q_revenue_by_customer_segment(spark: SparkSession) -> DataFrame:
    """Revenue contribution by customer segment."""

    return spark.sql("""
        SELECT
            customer_segment,
            COUNT(*) AS customer_count,
            ROUND(SUM(monetary), 2) AS total_revenue,
            ROUND(AVG(monetary), 2) AS avg_customer_value
        FROM rfm_customers
        GROUP BY customer_segment
        ORDER BY total_revenue DESC
    """)


def show_report(title: str, df: DataFrame, rows: int = 20) -> None:
    """Print a report title and display the query result."""

    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

    df.show(rows, truncate=False)


def run_analytics(spark: SparkSession = None) -> None:
    """Run all reporting queries."""

    if spark is None:
        spark = get_spark("OlistAnalytics")

    log.info("=" * 60)
    log.info("Starting analytics layer")
    log.info("=" * 60)

    register_views(spark)

    show_report(
        "Customer Segments",
        q_customer_segments(spark)
    )

    show_report(
        "Revenue by Customer Segment",
        q_revenue_by_customer_segment(spark)
    )

    show_report(
        "Top 10 Sellers by Revenue",
        q_top_sellers(spark, 10)
    )

    show_report(
        "Top 15 Product Categories",
        q_top_categories(spark, 15)
    )

    show_report(
        "Monthly Revenue Trends",
        q_monthly_growth(spark),
        rows=50
    )

    show_report(
        "Late Delivery Rate by Seller State",
        q_late_delivery_by_state(spark)
    )

    log.info("Analytics layer complete")


if __name__ == "__main__":
    spark_session = get_spark("OlistAnalytics")
    run_analytics(spark_session)