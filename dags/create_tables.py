import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.operators import PostgresOperator

dag = DAG(
    'Create_Redshift_Tables',
    start_date=datetime.datetime.now(),
    tags=['sparkify']
)

create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)