from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


"""
Default DAG args
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
"""
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 7, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='30 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

"""
   stage_events_to_redshift task. Uses StageToRedshiftOperator to load events data from s3 to the staging tables.
"""
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11"
)

"""
   stage_songs_to_redshift task. Uses StageToRedshiftOperator to load song data from s3 to the staging tables.
"""
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend", 
    s3_key="song_data/A/A"
)


"""
   load_songplays_table task. Uses LoadFactOperator to load fact table songplays from the staging tables.
"""
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="songplays",
    insert_sql=SqlQueries.songplay_table_insert
)

"""
   load_user_dimension_table task. Uses LoadDimensionOperator to load dimension table user from the staging tables.
"""
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="users",
    insert_sql=SqlQueries.user_table_insert
)

"""
   load_song_dimension_table task. Uses LoadDimensionOperator to load dimension table songs from the staging tables.
"""
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="songs",
    insert_sql=SqlQueries.song_table_insert
)

"""
   load_artist_dimension_table task. Uses LoadDimensionOperator to load dimension table artists from the staging tables.
"""
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="artists",
    insert_sql=SqlQueries.artist_table_insert    
)

"""
   load_time_dimension_table task. Uses LoadDimensionOperator to load dimension table time from the staging tables.
"""
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="time",
    insert_sql=SqlQueries.time_table_insert
)

"""
   run_quality_checks task. Uses DataQualityOperator to validate the data loaded to the fact and dimension tables.
"""
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["time","artists","songplays","songs","users"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
