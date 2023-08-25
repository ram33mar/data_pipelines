from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,

    'data_checks': [
        {'checkSql': "select count(*) from factsongplays", 'operator': '>', 'expectedResult':0},
        {'checkSql': "select count(*) from dimusers", 'operator': '>', 'expectedResult':0},
        {'checkSql': "select count(*) from dimsongs", 'operator': '>', 'expectedResult':0},
        {'checkSql': "select count(*) from dimartists", 'operator': '>', 'expectedResult':0},
        {'checkSql': "select count(*) from dimtime", 'operator': '>', 'expectedResult':0}
    ]
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket_path="s3://awsairflow20230820",
        s3_sub_path="log-data",
        format_json="s3://awsairflow20230820/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket_path="s3://awsairflow20230820",
        s3_sub_path="song-data",
        format_json=""
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="factSongPlays",
        sql=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="dimUsers",
        sql=final_project_sql_statements.SqlQueries.user_table_insert,
        truncate_insert=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="dimSongs",
        sql=final_project_sql_statements.SqlQueries.song_table_insert,
        truncate_insert=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="dimArtists",
        sql=final_project_sql_statements.SqlQueries.artist_table_insert,
        truncate_insert=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="dimTime",
        sql=final_project_sql_statements.SqlQueries.time_table_insert,
        truncate_insert=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        dqChecks=default_args['data_checks']
    )

    end_operator = DummyOperator(task_id='End_execution')


    start_operator >>  stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator
    

final_project_dag = final_project()