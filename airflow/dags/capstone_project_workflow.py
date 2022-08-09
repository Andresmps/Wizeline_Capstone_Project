from airflow.models import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator

from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

from airflow.utils.dates import days_ago
# from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

# General constants
ENV = "dev"
PROJECT_ID = "dataengbootcamp"

CLUSTER_NAME = f"cluster-dataproc-pyspark-{ENV}"
REGION = "us-central1"
ZONE = "us-central1-a"

DAG_ID = f"gcp_capstone_project_{ENV}_workflow"
CLOUD_PROVIDER = "gcp"
API_VERSION = "V0.1"


# GCS constants

GCS_BUCKET_NAME = f"capstone-project-{ENV}"
GCS_USER_PURCHASE_KEY = "GDrive/user_purchase.csv"
GCS_MOVIE_REVIEW_KEY = "GDrive/movie_review.csv"
GCS_LOG_REVIEW_KEY = "GDrive/log_reviews.csv"
GCS_RAW_ZONE = F"gs://{GCS_BUCKET_NAME}/Raw/"

GCS_INIT_FILE_KEY = "Data_proc_scripts/pip-install.sh"
GCS_PYSPARK_CLEANING_KEY = "Data_proc_scripts/clean_data.py"
GCS_PYSPARK_AGG_KEY = "Data_proc_scripts/agg_data.py"

# Connections
GCP_CONN_ID = "gcp_conn"

# Postgres constants
POSTGRES_CONN_ID = "postgres_conn"
POSTGRES_TABLE_NAME = "user_purchase"


def ingest_data_from_gcs(
    gcs_bucket: str, gcs_object: str, postgres_table: str,
    gcp_conn_id: str = "google_cloud_default",
    postgres_conn_id: str = "postgres_default",
):
    import tempfile
    import pandas as pd

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    psql_hook = PostgresHook(postgres_conn_id)

    with tempfile.NamedTemporaryFile() as tmp:

        gcs_hook.download(
            bucket_name=gcs_bucket, object_name=gcs_object, filename=tmp.name
        )

        user_purchase_df = pd.read_csv(tmp.name, sep=',')
        user_purchase_df.CustomerID = user_purchase_df.CustomerID.astype("Int64").fillna(-1)
        user_purchase_df.to_csv(tmp.name, header=False, sep='\t', index=False)

        psql_hook.bulk_load(table=postgres_table, tmp_file=tmp.name)


with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, ENV, API_VERSION],
) as dag:

    start_workflow = DummyOperator(task_id="start_workflow")

    verify_user_purchases_existence = GCSObjectExistenceSensor(
        task_id="verify_existence_user_purchase_file",
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=GCS_USER_PURCHASE_KEY
    )

    verify_movie_review_existence = GCSObjectExistenceSensor(
        task_id="verify_existence_movie_review_file",
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=GCS_MOVIE_REVIEW_KEY
    )

    verify_log_reviews_existence = GCSObjectExistenceSensor(
        task_id="verify_existence_log_review_file",
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=GCS_LOG_REVIEW_KEY
    )

    create_table_user_purchases = PostgresOperator(
        task_id="create_table_user_purchases",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_NAME} (
                invoice_number varchar(20),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
            )
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    copy_gcs_movie_review_to_gcs = GCSToGCSOperator(
        task_id="Copy_GDrive_Files_to_GCS",
        source_bucket=GCS_BUCKET_NAME,
        source_objects=[
            GCS_MOVIE_REVIEW_KEY, GCS_LOG_REVIEW_KEY
        ],
        destination_object=GCS_RAW_ZONE,
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    continue_process = DummyOperator(
        task_id="continue_process"
    )
    
    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"DELETE FROM {POSTGRES_TABLE_NAME}",
    )

    validate_data = BranchSQLOperator(
        task_id="validate_no_data_in_the_db",
        conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT COUNT(*) AS total_rows FROM {POSTGRES_TABLE_NAME}",
        follow_task_ids_if_false=[continue_process.task_id],
        follow_task_ids_if_true=[clear_table.task_id]
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    ingest_user_purchase_data = PythonOperator(
        task_id="ingest_user_purchase_data",
        python_callable=ingest_data_from_gcs,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "gcs_bucket": GCS_BUCKET_NAME,
            "gcs_object": GCS_USER_PURCHASE_KEY,
            "postgres_table": POSTGRES_TABLE_NAME,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    (
        start_workflow
        >> [
            verify_user_purchases_existence
        ]
        >> create_table_user_purchases
        >> validate_data
        >> [
            clear_table,
            continue_process
        ]
        >> ingest_user_purchase_data
        >> end_workflow
    )

    (
        start_workflow
        >> [
            verify_movie_review_existence,
            verify_log_reviews_existence
        ]
        >> copy_gcs_movie_review_to_gcs
    )
