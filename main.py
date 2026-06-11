"""
main.py

Main runner for the Olist E-commerce Data Platform.

The pipeline can run end-to-end or one layer at a time:

    python main.py
    python main.py --layer bronze
    python main.py --layer silver
    python main.py --layer gold
    python main.py --layer serve
"""

import argparse
import logging
from datetime import datetime
import sys

sys.path.append("/Workspace/Users/dharmikpatel982003@gmail.com/project_sales")

from Utils.spark_session import get_spark
from Ingestion.bronze_loader import run_bronze_ingestion
from Transformation.silver_cleaner import run_silver_cleaning
from Transformation.gold_features import run_gold_features
from Serving.analytics import run_analytics


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger("pipeline")


def main(layer: str = "all") -> None:
    """Run the selected pipeline layer."""

    spark = get_spark("OlistEcommercePipeline")
    start_time = datetime.now()

    log.info("=" * 60)
    log.info(f"Starting Olist pipeline | layer={layer}")
    log.info("=" * 60)

    if layer in ("all", "bronze"):
        log.info("Running Bronze layer")
        run_bronze_ingestion(spark)

    if layer in ("all", "silver"):
        log.info("Running Silver layer")
        run_silver_cleaning(spark)

    if layer in ("all", "gold"):
        log.info("Running Gold layer")
        run_gold_features(spark)

    if layer in ("all", "serve"):
        log.info("Running Analytics layer")
        run_analytics(spark)

    elapsed_seconds = (datetime.now() - start_time).seconds

    log.info("=" * 60)
    log.info(f"Pipeline completed in {elapsed_seconds}s")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the Olist E-commerce data pipeline"
    )

    parser.add_argument(
        "--layer",
        choices=["all", "bronze", "silver", "gold", "serve"],
        default="all",
        help="Pipeline layer to run"
    )

    args, unknown = parser.parse_known_args()

    main(args.layer)