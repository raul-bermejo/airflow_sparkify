from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'raulbv',
    'start_date': datetime(2021, 8, 11),
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    
}

dag = DAG('sparkify_main_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    aws_credentials='aws_credentials',
    table='staging_events',
    s3_key='log_data',
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    aws_credentials='aws_credentials',
    table='staging_songs',
    s3_key='song_data',
    task_id='Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    redshift_conn_id="redshift",
    task_id='Load_songplays_fact_table',
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    redshift_conn_id="redshift",
    table="users",
    sql_query=SqlQueries.users_table_insert,
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    redshift_conn_id="redshift",
    table="song",
    sql_query=SqlQueries.song_table_insert,
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    redshift_conn_id="redshift",
    table="artist",
    sql_query=SqlQueries.artist_table_insert,
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert,
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    redshift_conn_id="redshift",
    table=["users", "song", "artist", "time"],
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Configure DAG order
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
