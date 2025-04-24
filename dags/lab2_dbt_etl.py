from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

# Retrieve Snowflake connection information from Airflow
conn = BaseHook.get_connection("snowflake_conn")

dbt_env_vars = {
    "DBT_USER": conn.login,
    "DBT_PASSWORD": conn.password,
    "DBT_ACCOUNT": conn.extra_dejson.get("account"),
    "DBT_SCHEMA": conn.schema,
    "DBT_DATABASE": conn.extra_dejson.get("database"),
    "DBT_ROLE": conn.extra_dejson.get("role"),
    "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
    "DBT_TYPE": "snowflake"
}

# Path to the dbt project directory
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

# Define the DAG configuration
with DAG(
    dag_id="lab2_dbt",
    start_date=datetime(2025, 4, 21),
    description="Run dbt transformations on Snowflake after ETL",
    schedule="0 4 * * *",  # Runs daily at 4:00 AM (after ETL is completed)
    catchup=False
) as dag:

    # Wait for the ETL process to complete before running dbt
    etl_task = BashOperator(
        task_id="Stock_YFinance_Snowflake",
        bash_command="echo 'Waiting for ETL DAG Stock_YFinance_Snowflake to complete...'",
    )

    # Execute dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=dbt_env_vars,
    )

    # Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=dbt_env_vars,
    )

    # Execute snapshot to track historical changes
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=dbt_env_vars,
    )

    # Define task dependencies
    etl_task >> dbt_run >> dbt_test >> dbt_snapshot