from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Class LoadFactOperator
      Operator to load fact tables
"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 table="songplays",
                 insert_sql="",
                 truncate_on=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.truncate_on=truncate_on
        self.insert_sql=insert_sql

    """
        Operator execute load fact table.
                   Default mode is append only for fact tables, unless truncate_on is True
    """
    def execute(self, context):
        # Redshift Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_on:
            # Clear data
            self.log.info("Clearing data from destination Redshift dimension table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        # Build Query
        insert_query="INSERT INTO {} ({})".format(self.table,self.insert_sql)
        self.log.info("BUILT QUERY: {}".format(insert_query))
        
        # Execute Query
        self.log.info("Running insert for fact table {}".format(self.table))
        redshift.run(insert_query)
        #self.log.info('LoadFactOperator not implemented yet')
