from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='meetup_medallion_ingestion_15min',
    default_args=default_args,
    description='Pipeline Medallion para ingesta de datos Meetup en Snowflake',
    schedule_interval='*/15 * * * *',
    catchup=False,
) as dag:

    test_conn = SQLExecuteQueryOperator(
        task_id='test_snowflake_connection',
        conn_id='snowflake_default',
        sql="SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_SCHEMA();",
    )

    create_bronze_table = SQLExecuteQueryOperator(
        task_id='create_bronze_layer',
        conn_id='snowflake_default',
        sql="""
        CREATE TABLE IF NOT EXISTS PRUEBA_TECNICA_RAPPI.BRONZE.MEETUP_RAW (
            raw_data VARIANT,
            ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """,
    )

    test_conn >> create_bronze_table