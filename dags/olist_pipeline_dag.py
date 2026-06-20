from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PROJECT_PATH = "/opt/airflow/olist-project"

default_args = {
    "owner": "dharmik",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="olist_ecommerce_medallion_pipeline",
    default_args=default_args,
    description="Orchestrates Olist Bronze, Silver, Gold and Serving layers",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["olist", "pyspark", "databricks", "medallion"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"cd {PROJECT_PATH} && python main.py --layer bronze",
    )

    silver = BashOperator(
        task_id="silver_transformation",
        bash_command=f"cd {PROJECT_PATH} && python main.py --layer silver",
    )

    gold = BashOperator(
        task_id="gold_marts",
        bash_command=f"cd {PROJECT_PATH} && python main.py --layer gold",
    )

    serving = BashOperator(
        task_id="serving_queries",
        bash_command=f"cd {PROJECT_PATH} && python main.py --layer serve",
    )

    bronze >> silver >> gold >> serving