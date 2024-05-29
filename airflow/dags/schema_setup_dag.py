from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'schema_creation_dag',
    default_args=default_args,
    description='A DAG to create schema by executing schema-creation.sql',
    schedule_interval=None,
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

create_database_task = PostgresOperator(
    task_id='create_database',
    postgres_conn_id='postgresql-data-platform',
    sql='CREATE DATABASE data_platform;',
    trigger_rule='all_done',
    autocommit=True,
    dag=dag,
)

schema_creation_task = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id='postgresql-data-platform',
    sql='./data-pipelines/src/main/sql/setup-script/schema_creation_script.sql',
    trigger_rule='all_done',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_task >> create_database_task >> schema_creation_task >> end_task
