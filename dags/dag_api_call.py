import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from nba_api_calls import fetch_player_team_data, fetch_and_save_team_game_data, fetch_and_save_player_game_data

DAG_ID = "api_to_cloud"
BUCKET_NAME = "nba_bucket_1_200724"
DATASET_NAME = "nba_dataset_1"
STG_TABLE_NAME = ["stg_playersgames", "stg_team_games"]


default_args = {
    'owner': 'pragnesh',
    'start_date': airflow.utils.dates.days_ago(0),
    'schedule_interval': '@once',
    'retries': 0
}

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Calling API and loading files to GCS",
    tags=["API"]
) as dag:
    # Define the task to fetch and save NBA games
    player_team_data_task = PythonOperator(
        task_id="fetch_and_save_player_team_data",
        python_callable=fetch_player_team_data,
        dag=dag,
    )

    team_game_data_task = PythonOperator(
        task_id="fetch_and_save_team_game_data",
        python_callable=fetch_and_save_team_game_data,
        dag=dag,
    )

    player_game_data_task = PythonOperator(
        task_id="fetch_and_save_player_game_data",
        python_callable=fetch_and_save_player_game_data,
        dag=dag
    )

    players_to_bigquery_task = GCSToBigQueryOperator(
        task_id="players_to_bigquery_load",
        bucket=BUCKET_NAME,
        source_objects=["nba_player_stats_{{ ds }}.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{STG_TABLE_NAME[0]}",
        source_format="CSV",
        field_delimiter=",",
        write_dispostion="WRITE_TRUNCATE",
    )

    team_to_bigquery_task = GCSToBigQueryOperator(
        task_id="teams_to_bigquery_load",
        bucket=BUCKET_NAME,
        source_objects=["nba_team_stats_{{ ds }}.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{STG_TABLE_NAME[1]}",
        source_format="CSV",
        field_delimiter=",",
        write_disposition="WRITE_TRUNCATE",
    )

    player_team_data_task >> team_game_data_task >> player_game_data_task