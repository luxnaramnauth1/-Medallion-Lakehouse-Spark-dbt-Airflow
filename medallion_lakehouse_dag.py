"""Orchestrates the medallion pipeline: raw -> Bronze -> Silver (Spark) -> Gold (dbt)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

ENTITIES = ["customers", "products", "orders", "order_items"]
JOBS = "/opt/airflow/spark/jobs"
DBT = "/home/airflow/dbt_venv/bin/dbt"
# Work on a writable copy of the dbt project (the mount is read-only)
DBT_PREP = "rm -rf /tmp/dbt && cp -r /opt/airflow/dbt_src/lakehouse /tmp/dbt && cd /tmp/dbt"
DS = "{{ ds }}"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="medallion_lakehouse",
    description="Spark (Bronze/Silver) + dbt (Gold) medallion pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "spark", "dbt", "medallion"],
) as dag:

    generate_raw = BashOperator(
        task_id="generate_raw_data",
        bash_command=f"cd /tmp && python {JOBS}/generate_data.py --ds {DS}",
    )

    with TaskGroup("bronze") as bronze:
        for e in ENTITIES:
            BashOperator(
                task_id=f"ingest_{e}",
                bash_command=f"cd /tmp && python {JOBS}/bronze_ingest.py --entity {e} --ds {DS}",
            )

    with TaskGroup("silver") as silver:
        for e in ENTITIES:
            BashOperator(
                task_id=f"transform_{e}",
                bash_command=f"cd /tmp && python {JOBS}/silver_transform.py --entity {e} --ds {DS}",
            )

    gold_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_PREP} && {DBT} run --profiles-dir .",
    )

    gold_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_PREP} && {DBT} test --profiles-dir .",
    )

    generate_raw >> bronze >> silver >> gold_run >> gold_test
