from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_create_sql_statements
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'catchUp': False
}

@dag(
    default_args=default_args,
    description='Create Tables in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project_create_table():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_table_staging_events_task=PostgresOperator(
            task_id="create_table_staging_events",
            postgres_conn_id="redshift",
            sql=final_project_create_sql_statements.CreateSqlQueries.staging_events_table_create
    )

    create_table_staging_songs_task=PostgresOperator(
            task_id="create_table_staging_songs",
            postgres_conn_id="redshift",
            sql=final_project_create_sql_statements.CreateSqlQueries.staging_songs_table_create
    )

    create_table_user_table_task=PostgresOperator(
            task_id="create_table_user_table",
            postgres_conn_id="redshift",
            sql=final_project_create_sql_statements.CreateSqlQueries.user_table_create
    )

    create_table_song_table_task=PostgresOperator(
            task_id="create_table_song_table",
            postgres_conn_id="redshift",
            sql=final_project_create_sql_statements.CreateSqlQueries.song_table_create
    )

    create_table_artist_table_task=PostgresOperator(
            task_id="create_table_artist_table",
            postgres_conn_id="redshift",
            sql=final_project_create_sql_statements.CreateSqlQueries.artist_table_create
    )

    create_table_time_table_task=PostgresOperator(
            task_id="create_table_time_table",
            postgres_conn_id="redshift",
            sql=final_project_create_sql_statements.CreateSqlQueries.time_table_create
    )

    create_table_songplay_table_task=PostgresOperator(
            task_id="create_table_songplay_table",
            postgres_conn_id="redshift",
            sql=final_project_create_sql_statements.CreateSqlQueries.songplay_table_create
    )


    end_operator = DummyOperator(task_id='End_execution')


    start_operator >> create_table_staging_events_task
    create_table_staging_events_task >> create_table_staging_songs_task
    create_table_staging_songs_task >> create_table_user_table_task
    create_table_user_table_task >> create_table_song_table_task
    create_table_song_table_task >> create_table_artist_table_task
    create_table_artist_table_task >> create_table_time_table_task
    create_table_time_table_task >> create_table_songplay_table_task
    create_table_songplay_table_task >> end_operator

final_project_create_table = final_project_create_table()