import os
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from jinja2 import Environment, FileSystemLoader

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_iot_temp_files',
    default_args=default_args,
    description='A DAG to load CSV to staging, validate, and move to golden table',
    schedule_interval=None,
)
"""
    Note:
        In actual production production environment, these Variables(Global) should be moved to 
        configuration files, which will allow easy management and switching environment  
"""
INPUT_DIR = './data/input'
PROCESSED_DIR = './data/processed'
REJECT_DIR = './data/rejected'
EXPECTED_COLUMNS = ['id', 'room_id', 'noted_date', 'temp', 'out_or_in']
CSV_COLUMNS = ['id', 'room_id/id', 'noted_date', 'temp', 'out/in']
STAGING_TABLE = 'temperature_readings_stg'
PROCESSED_TABLE = 'temperature_readings'
GOLDEN_TABLE = 'temperature_readings'
LOG_TABLE = 'rejected_files_log'
SCHEMA_STAGING = 'staging'
SCHEMA_PROCESSED = 'processed'
SCHEMA_GOLDEN = 'golden'
SCHEMA_LOG = 'dwh_audit'
CONN_ID = 'postgresql-data-platform'
SQL_BASE_DIR = '/opt/airflow/dags/data-pipelines/src/main/sql'


def get_postgres_engine():
    conn = BaseHook.get_connection(CONN_ID)
    return create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')


def preprocess_and_load_csv():
    engine = get_postgres_engine()
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]

    # Truncate the staging table
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_STAGING}.{STAGING_TABLE};"))

    for file in files:
        file_path = os.path.join(INPUT_DIR, file)
        try:
            df = pd.read_csv(file_path)
            df.columns = [col.replace('room_id/id', 'room_id').replace('out/in', 'out_or_in') for col in df.columns]
            if list(df.columns) == EXPECTED_COLUMNS:
                df['load_timestamp'] = datetime.now()
                df['source_file_name'] = file
                # df['load_status'] = 'pending'
                df.to_sql(STAGING_TABLE, engine, schema=SCHEMA_STAGING, if_exists='append', index=False)
                os.rename(file_path, os.path.join(PROCESSED_DIR, file))
                logging.info(f"File {file} loaded and moved to processed directory successfully.")
            else:
                reason = "Incorrect format"
                log_rejected_file(engine, file, reason)
                os.rename(file_path, os.path.join(REJECT_DIR, file))
                logging.info(f"File {file} rejected due to incorrect format.")
        except Exception as e:
            reason = f"Error processing file: {e}"
            log_rejected_file(engine, file, reason)
            os.rename(file_path, os.path.join(REJECT_DIR, file))
            logging.info(f"Error processing file {file}: {e}")


def log_rejected_file(engine, file_name, reason):
    with engine.connect() as conn:
        conn.execute(
            text(f"INSERT INTO {SCHEMA_LOG}.{LOG_TABLE} (file_name, reason) VALUES (:file_name, :reason)"),
            {"file_name": file_name, "reason": reason}
        )


def get_query_from_template(file_name, schema_name=SCHEMA_PROCESSED, table_name=PROCESSED_TABLE):

    environment = Environment(loader=FileSystemLoader(SQL_BASE_DIR + '/log_summary/'))
    template = environment.get_template(file_name)
    query = template.render(SCHEMA_PROCESSED=schema_name,
                            PROCESSED_TABLE=table_name)

    return query


def log_processed_record_counts():

    engine = get_postgres_engine()

    logging.info('\n'*2 + '#'*50 + '\t'*3 + 'Current Run Summary' + '\t'*3 + '#'*50 + '\n'*2)
    with engine.connect() as connection:

        load_summary_query = get_query_from_template('total_record_processed.sql' )
        result = connection.execute(load_summary_query)
        for row in result:
            logging.info(f"Total Records Processed in Current Run , Count: {row['count']}")

        logging.info('\n')

        load_summary_query = get_query_from_template('load_summary.sql' )
        result = connection.execute(load_summary_query)
        for row in result:
            logging.info(f"Load Status: {row['load_status']}, Count: {row['count']}")

        logging.info('\n')

        load_message_query = get_query_from_template('load_message.sql')
        result = connection.execute(load_message_query)
        for row in result:
            logging.info(f"Validation Message: {row['validation_message']}, Count: {row['count']}")

        update_current_run_flag = get_query_from_template('update_current_run.sql')
        result = connection.execute(load_message_query)
        connection.execute(update_current_run_flag)

    logging.info('\n'*2 + '#'*163 + '\n'*2)


start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

preprocess_and_load_task = PythonOperator(
    task_id='preprocess_and_load_csv',
    python_callable=preprocess_and_load_csv,
    dag=dag,
)

validate_and_load_task = PostgresOperator(
    task_id='validate_and_load_to_processed',
    postgres_conn_id=CONN_ID,
    sql='./data-pipelines/src/main/sql/process_iot_temp_files/validate_and_load_to_processed.sql',
    params={"SCHEMA_STAGING": SCHEMA_STAGING, "STAGING_TABLE": STAGING_TABLE,
            "SCHEMA_PROCESSED": SCHEMA_PROCESSED, "PROCESSED_TABLE": PROCESSED_TABLE,
            "SCHEMA_GOLDEN": SCHEMA_GOLDEN, "GOLDEN_TABLE": GOLDEN_TABLE},
    dag=dag
)

move_to_golden_task = PostgresOperator(
    task_id='move_valid_records_to_golden',
    postgres_conn_id=CONN_ID,
    sql='./data-pipelines/src/main/sql/process_iot_temp_files/move_valid_records_to_golden.sql',
    params={"SCHEMA_GOLDEN": SCHEMA_GOLDEN, "GOLDEN_TABLE": GOLDEN_TABLE,
            "SCHEMA_PROCESSED": SCHEMA_PROCESSED, "PROCESSED_TABLE": PROCESSED_TABLE},
    dag=dag,
)

log_task = PythonOperator(
    task_id='log_processed_record_counts',
    python_callable=log_processed_record_counts,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


start_task >> preprocess_and_load_task \
           >> validate_and_load_task \
           >> move_to_golden_task \
           >> log_task \
    >> end_task
