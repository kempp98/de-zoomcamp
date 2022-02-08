import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

DATASET = "tripdata"
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_to_bq_fhv_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:


    gcs_to_gcs_task = GCSToGCSOperator(
        task_id = "gcs_to_gcs_task",
        source_bucket = BUCKET,
        source_object = 'raw/fhv_tripdata*.parquet',
        destination_bucket = BUCKET,
        destination_object='fhv/',
        move_object = True
    )


    gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
        task_id="gcs_to_bq_ext_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId" : BIGQUERY_DATASET,
                "tableId" : "fhv_table"
            },
            "externalDataConfiguration" : {
                "autodetect" : True,
                "sourceFormat": "PARQUET",
                "sourceUris" : [f"gs://{BUCKET}/fhv/*"]
            }
        }
    )

    CREATE_PART_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.fhv_data_partitioned\
                        PARTITION BY DATE(pickup_datetime) AS \
                        SELECT * FROM {BIGQUERY_DATASET}.fhv_table"

    bg_ext_to_part_task = BigQueryInsertJobOperator(
        task_id="bg_ext_to_part_task",
        configuration = {
            "query": {
                "query" : CREATE_PART_QUERY,
                "useLegacySql" : False
            }
        }
    )

    gcs_to_gcs_task >> gcs_to_bq_ext_task >> bg_ext_to_part_task