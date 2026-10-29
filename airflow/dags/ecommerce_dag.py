"""E-commerce clickstream pipeline.

init_schema -> load_products -> load_clickstream (Spark clean + load) -> dbt_run -> dbt_test

Everything runs inside the Airflow container. Paths come from compose.yml:
  /opt/airflow/project       src/, clickstream_events.jsonl, product_catalog.csv (read-only)
  /opt/airflow/ecommerce_dbt dbt project (read-only; output goes to DBT_TARGET_PATH)
  /opt/airflow/dbt_profiles  profiles.yml
The pipeline's Python dependencies are in their own venv (see Dockerfile.airflow).
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

VENV = os.environ.get("PIPELINE_VENV", "/home/airflow/pipeline_venv")
PYTHON = f"{VENV}/bin/python"
DBT = f"{VENV}/bin/dbt"

# The scripts read clickstream_events.jsonl / product_catalog.csv / src/*.sql by relative
# path, so every Python task runs from the project directory.
PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = "/opt/airflow/ecommerce_dbt"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt_profiles")
DBT_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

# Make PySpark use the venv's interpreter for its Python workers too.
SPARK_ENV = {"PYSPARK_PYTHON": PYTHON, "PYSPARK_DRIVER_PYTHON": PYTHON}

default_args = {
    "owner": "ayoisegun",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    description="Clean clickstream with Spark, load to Postgres, build and test dbt models",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,  # trigger manually from the UI; change to "@daily" once it is stable
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "spark", "dbt"],
) as dag:

    # Creates cleaned_clickstream and products if they do not exist (CREATE TABLE IF NOT EXISTS)
    init_schema = BashOperator(
        task_id="init_schema",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} src/load_postgres.py",
    )

    # Truncates and reloads the product catalogue, so reruns do not duplicate rows
    load_products = BashOperator(
        task_id="load_products",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} src/load_products.py",
    )

    # Spark: clean the raw events and overwrite public.cleaned_clickstream
    load_clickstream = BashOperator(
        task_id="load_clickstream",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} src/load_clickstream.py",
        env=SPARK_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=60),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run {DBT_FLAGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_FLAGS}",
    )

    init_schema >> load_products >> load_clickstream >> dbt_run >> dbt_test
