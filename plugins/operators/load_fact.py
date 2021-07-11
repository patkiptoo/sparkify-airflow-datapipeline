from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

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
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.insert_sql=insert_sql

     """
        Operator execute to  clear data, and reload
    """
    def execute(self, context):
        # Redshift Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Clear data
        self.log.info("Clearing data from destination Redshift table {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        
        # Build Query
        insert_query="INSERT INTO {} ({})".format(self.table,self.insert_sql)
        self.log.info("BUILT QUERY: {}".format(insert_query))
        
        # Execute Query
        self.log.info("Running insert for fact table {}".format(self.table))
        redshift.run(insert_query)
        #self.log.info('LoadFactOperator not implemented yet')
