from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # operator params (with defaults)
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Mapped operator params
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        
        self.log.info("Connecting to Redshift database")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            table_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(table_records) < 1 or len(table_records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = table_records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {table_records[0][0]} records")