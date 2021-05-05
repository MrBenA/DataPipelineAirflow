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
                 table_names_expected_records=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Mapped operator params
        self.redshift_conn_id = redshift_conn_id
        self.table_names_expected_records = table_names_expected_records

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')

        self.log.info("Connecting to Redshift database")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # for dic in self.table_names_expected_records:
        #     for key in dic.values():
        #         record_count = redshift_hook.get_records(f"SELECT COUNT(*) FROM {key}")
        #         expected_count = 0
        #         if len(record_count[0]) == expected_count:
        #             raise ValueError(f"Record count advisory. {key} does not match expected results")
        #         logging.info(f"Record count advisory. {key} does not match expected results")

        for dic in self.table_names_expected_records:
            table_name = dic['table']
            expected_count = dic['expected']
            record_count = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if record_count[0][0] != expected_count:
                raise ValueError(f"WARNING, {table_name} record count advisory."
                             f"{record_count[0][0]} does not match expected result {expected_count}!")
            else:
                logging.info(f"SUCCESS, {table_name} record count."
                             f"{record_count[0][0]} matches expected result {expected_count}!")
