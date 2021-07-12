from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Class DataQualityOperator
      Operator to verify that loads were successful from s3 to staging tables then to dimension and fact tables.
"""
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id,
        self.tables = tables

    """
        Operator execute validates that data was actually loaded.
    """
    def execute(self, context):
        # Redshift Hook
        #redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift = PostgresHook(postgres_conn_id="redshift")
        
        if len(self.tables) < 1:
            self.log.error("No tables listed for validation")
            raise ValueError(f"No tables listed for validation")
        self.log.info("Completed validation that non empty list of tables {}".format(self.tables))
        
        for table in self.tables:
            query = f"SELECT COUNT(*) FROM {table}"
            self.log.info("Validation Query is: {}".format(query))
            records = redshift.get_records(query)
            if records is None or len(records[0]) < 1:
                self.log.error(f"No records present in destination table {table}")
                raise ValueError(f"No records present in destination table {table}")
                
        self.log.info('DataQualityOperator completed successfully')