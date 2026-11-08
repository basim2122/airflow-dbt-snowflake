
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Project paths: resolved from this file's location (dags/ -> project root),
# so the DAG works wherever the repo is cloned.
PROJECT_DIR = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_DIR / "dbt" / "snowflake_data_project"

# dbt executable from the separate dbt environment
DBT_EXECUTABLE = PROJECT_DIR / ".dbt_venv" / "bin" / "dbt"

# dbt profile configuration directory
DBT_PROFILES_DIR = PROJECT_DIR / "config"

default_args = {
    "owner": "basem",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_snowflake_workflow",
    default_args=default_args,
    description="Run dbt models and tests on Snowflake",
    start_date=datetime(2026, 11, 5),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "snowflake"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=(
            f'"{DBT_EXECUTABLE}" debug '
            f'--project-dir "{DBT_PROJECT_DIR}" '
            f'--profiles-dir "{DBT_PROFILES_DIR}"'
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f'"{DBT_EXECUTABLE}" run '
            f'--project-dir "{DBT_PROJECT_DIR}" '
            f'--profiles-dir "{DBT_PROFILES_DIR}"'
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f'"{DBT_EXECUTABLE}" test '
            f'--project-dir "{DBT_PROJECT_DIR}" '
            f'--profiles-dir "{DBT_PROFILES_DIR}"'
        ),
    )

    dbt_debug >> dbt_run >> dbt_test
