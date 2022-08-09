project_id = "dataengbootcamp"
region     = "us-central1"
location   = "us-central1-a"
env        = "dev"

#GKE
gke_num_nodes = 2
machine_type  = "n1-standard-1"

#CloudSQL airflow metadata
instance_name      = "airflow-metadata-1"
database_version   = "POSTGRES_12"
instance_tier      = "db-f1-micro"
disk_space         = 10
database_name      = "dbadmin"
db_username        = "postgres"
