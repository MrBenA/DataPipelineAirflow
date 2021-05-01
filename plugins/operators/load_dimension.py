from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {table}
        {table_insert_sql}
    """

    
    @apply_defaults
    def __init__(self,
                 # operator params (with defaults)
                 redshift_conn_id="",
                 table = "",
                 table_insert_sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Mapped operator params
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_insert_sql = table_insert_sql

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        
        self.log.info("Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from dimension table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info("Loading dimension table data")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            table = self.table,
            table_insert_sql = self.table_insert_sql
        )
        redshift.run(formatted_sql)
