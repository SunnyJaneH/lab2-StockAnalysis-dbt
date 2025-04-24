from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

@task
def set_stage():
    return SnowflakeOperator(
        task_id="set_stage",
        sql="""
            CREATE OR REPLACE STAGE dev.raw.blob_stage
            URL = 's3://s3-geospatial/readonly/'
            FILE_FORMAT = (TYPE = csv, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """,
        snowflake_conn_id="snowflake_conn",
    ).execute({})

@task
def load():
    return SnowflakeOperator(
        task_id="load",
        sql="""
            COPY INTO dev.raw.user_session_channel FROM @dev.raw.blob_stage/user_session_channel.csv;
            COPY INTO dev.raw.session_timestamp FROM @dev.raw.blob_stage/session_timestamp.csv;
        """,
        snowflake_conn_id="snowflake_conn",
    ).execute({})

with DAG(
    dag_id="SessionToSnowflake",
    start_date=days_ago(1),  
    schedule_interval="30 14 * * *", 
    catchup=False,
    tags=['snowflake']
) as dag:
    
    # Define task dependencies
    set_stage_task = set_stage()
    load_task = load()

    set_stage_task >> load_task  # `load` runs after `set_stage`