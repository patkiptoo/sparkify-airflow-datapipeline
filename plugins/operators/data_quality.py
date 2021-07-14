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
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id,
        self.dq_checks = dq_checks

    """
        Operator execute validates that data was actually loaded.
    """
    def execute(self, context):
        # Redshift Hook
        #redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift = PostgresHook(postgres_conn_id="redshift")
        
        if len(self.dq_checks) < 1:
            self.log.error("Data Quality checks action list is empty")
            raise ValueError(f"Data Quality checks action list is empty")
        
        for dq_check in self.dq_checks:
            dq_check_query=dq_check.get("check_sql")
            dq_check_expected=dq_check.get("expected_result")
            
            records = redshift.get_records(dq_check_query)

            if records is None or len(records[0]) < 1:
                self.log.error(f"Data Quality check failed. Query : {dq_check_query}")
                raise ValueError(f"Data Quality check failed. Query : {dq_check_query}")

            if dq_check_expected == "greater_than_zero":
                self.log.info(f"The records is {records[0]}")
                if records[0][0] < 1:
                    self.log.error(f"Data Quality check failed. Query : {dq_check_query}")
                    raise ValueError(f"Data Quality check failed. Query : {dq_check_query}")
                else:
                    self.log.info(f"Data Quality check passed. Query : {dq_check_query} ; Check : {dq_check_expected}")
            else:
                self.log.error(f"Data Quality check failed. Unimplemented expected results passed in. Query : {dq_check_query} ; Check : {dq_check_expected}")
                raise ValueError(f"Data Quality check failed. Unimplemented expected results passed in. Query : {dq_check_query} ; Check : {dq_check_expected}")          

                
        self.log.info('DataQualityOperator completed successfully')