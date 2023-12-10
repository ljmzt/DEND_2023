from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from project.create_table_queries import CreateTable
from project.sql_statements import SqlQueries
from project.operators import *

default_args = {
    'owner': 'ljmzt',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup_by_default': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule='@hourly'
)
def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')

    drop_table_operator = DropTableOperator(
        tables = ['public.staging_events', 
                  'public.staging_songs',
                  'public.fact_song_plays',
                  'public.dim_users',
                  'public.dim_songs',
                  'public.dim_artists',
                  'public.dim_times'],
        conn_id = 'redshift',
        task_id = 'Drop_all_tables'
    )

    create_schema_operator = CreateSchemaOperator(
        sql = CreateTable.create,
        conn_id = 'redshift',
        task_id = 'Create_schema'
    )

    # the log_json_path is necessary, otherwise it gets a lot of NaNs
    # useful to generate a test.json ~20 lines for test first
    stage_events_to_redshift = StageToRedshiftOperator(
        table = 'staging_events',
        s3_bucket = 'ljmzt-airflow',
        # s3_key = 'log-data/test.json',
        s3_key = 'log-data/2018/11/2018-11-17-events.json',
        s3_format = "FORMAT AS JSON 's3://ljmzt-airflow/log_json_path.json'",
        task_id = 'Stage_events',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        table = 'staging_songs',
        s3_bucket = 'ljmzt-airflow',
        s3_key = 'song-data/big_A.json',
        s3_format = "FORMAT AS JSON 'auto'",
        task_id = 'Stage_songs',
    )

    load_songplays_table = LoadFactOperator(
        sql = SqlQueries.songplay_table_insert,
        table = 'public.fact_songplays',
        task_id = 'Load_fact_songplays_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        sql = SqlQueries.user_table_insert,
        table = 'public.dim_users',
        task_id = 'Load_dim_users_table',
        truncate = True
    )

    load_song_dimension_table = LoadDimensionOperator(
        sql = SqlQueries.song_table_insert,
        table = 'public.dim_songs',
        task_id = 'Load_dim_songs_table',
        truncate = True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        sql = SqlQueries.artist_table_insert,
        table = 'public.dim_artists',
        task_id = 'Load_dim_artists_table',
        truncate = True
    )

    load_time_dimension_table = LoadDimensionOperator(
        sql = SqlQueries.time_table_insert,
        table = 'public.dim_times',
        task_id = 'Load_dim_times_table',
        truncate = True
    )

    run_quality_checks = DataQualityOperator(
        checks = [
            ('SELECT * FROM public.fact_songplays LIMIT 10', None),
            ('SELECT COUNT(*) FROM public.dim_users WHERE user_id IS NULL', 0),
            ('SELECT COUNT(*) FROM public.dim_songs WHERE song_id IS NULL', 0),
            ('SELECT COUNT(*) FROM public.dim_artists WHERE artist_id IS NULL', 0),
            ('SELECT COUNT(*) FROM public.dim_times WHERE hour is NULL', 0)
        ],
        task_id='Run_data_quality_checks',
    )

    # # create schemea
    start_operator >> drop_table_operator >> create_schema_operator

    # # staging
    create_schema_operator >> stage_events_to_redshift
    create_schema_operator >> stage_songs_to_redshift

    # load song plays table
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    # dim tables
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    # data quality checks
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

final_project_dag = final_project()
