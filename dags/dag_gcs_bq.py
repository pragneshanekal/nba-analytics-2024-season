import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DAG_ID = "bucket_to_bigquery"
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
    description="Using Operators to load data from files to BigQuery",
    tags=["GCS"]
) as dag:

    players_to_bigquery_task = GCSToBigQueryOperator(
        task_id="players_to_bigquery_load",
        bucket=BUCKET_NAME,
        source_objects=["players_games/nba_player_stats_*"],
        destination_project_dataset_table=f"{DATASET_NAME}.{STG_TABLE_NAME[0]}",
        source_format="CSV",
        field_delimiter=",",
        write_disposition="WRITE_APPEND",
    )

    team_to_bigquery_task = GCSToBigQueryOperator(
        task_id="teams_to_bigquery_load",
        bucket=BUCKET_NAME,
        source_objects=["teams_games/nba_teams_stats_*"],
        destination_project_dataset_table=f"{DATASET_NAME}.{STG_TABLE_NAME[1]}",
        source_format="CSV",
        field_delimiter=",",
        write_disposition="WRITE_APPEND",
    )

    players_to_bigquery_task >> team_to_bigquery_task

