import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "dev"
DAG_ID = "dataproc_creation"
PROJECT_ID = "dataengbootcamp"
CLUSTER_NAME = f"cluster-dataproc-pyspark-{ENV_ID}"
REGION = "us-central1"
ZONE = "us-central1-a"

BUCKET_NAME = "data-bootcamp-test-1-dev-data"
INIT_FILE = "Data_proc_scripts/pip-install.sh"
PYSPARK_FILE = "Data_proc_scripts/clean_data.py"
GCP_CONN_ID = "gcp_conn"

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},

    },
    "gceClusterConfig":{
        "metadata": {
            "PIP_PACKAGES": "pg8000 joblib sqlalchemy nltk "
        }
    },
    "softwareConfig": {
        "properties": {
            "spark": "spark:spark.jars.packages=com.databricks:spark-xml_2.11:0.4.1"
        }
    },
    "initializationActions": {
        "executableFile": f"gs://{BUCKET_NAME}/{INIT_FILE}"
    }
}

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/{PYSPARK_FILE}"},
}


with models.DAG(
    DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id=GCP_CONN_ID

    )
    # [END how_to_cloud_dataproc_create_cluster_operator]

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
         project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID
    )

    # [START how_to_cloud_dataproc_delete_cluster_operator]
    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id="delete_cluster",
    #     project_id=PROJECT_ID,
    #     cluster_name=CLUSTER_NAME,
    #     region=REGION,
    # )
    # [END how_to_cloud_dataproc_delete_cluster_operator]
    # delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    create_cluster >> pyspark_task
    #  >> delete_cluster







