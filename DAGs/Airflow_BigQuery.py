from datetime import datetime, timedelta
from deleting_extra_GPC_resources import move_file, delete_buckets_with_prefix, delete_all_dataproc_jobs
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator,DataprocSubmitJobOperator,DataprocDeleteClusterOperator
from airflow.operators.python_operator import PythonOperator


# Replace these with your own values
GCP_CONN_ID = 'google_cloud_default'
PROJECT_ID = 'academic-pier-405912'
BUCKET_NAME = 'afzal03082821k407711123'
FILE_NAME = 'raw_data/uber_data.csv' # You can set the prefix based on your requirements
DESTINATION_FILE_NAME = 'transformed_data/uber_data.csv'
CLUSTER_NAME = 'afzalcluster17243569'
REGION = 'us-central1'
JOB_FILE_URI = 'gs://afzal03082821k407711123/Scripts/Uber_Data_Pipeline(DataProc).py'
bucket_prefix = 'dataproc'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Airflow_DataProc_BigQuery',
    default_args=default_args,
    description='DAG to create a Google Cloud Storage bucket',
    schedule_interval='12 07 * * *',  # Set to None to disable automatic scheduling
    catchup = False
)


# Checking if the file exists at the specified path
gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
    bucket=BUCKET_NAME,
    prefix=FILE_NAME,
    task_id="gcs_object_with_prefix_exists_task",
    timeout=40,  # Set the timeout to a reasonable value
    poke_interval=10,  # Set the poke interval to 3 seconds
    dag=dag,
)

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        },
    "secondary_worker_config": {},
    }

#Creating a cluster
create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag
)

#PySpark job config
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_FILE_URI},
}

#Submiting the job to cluster
pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task",
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag
)

# Deleting the Created Cluster
delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag
)   

# Move the file from raw folder to processed folder
moving_file_to_processed = PythonOperator(
    task_id='moving_file_to_processed',
    python_callable= move_file,
    op_args=[BUCKET_NAME, FILE_NAME, DESTINATION_FILE_NAME],  # Pass the argument as a list
    dag=dag,
    provide_context=True,
)

# Delete the temp and staging buckets for Dataproc
deleting_dataproc_buckets = PythonOperator(
    task_id='delete_dataproc_buckets',
    python_callable=delete_buckets_with_prefix,
    op_args=[bucket_prefix],  # Pass the argument as a list
    provide_context=True,
    dag=dag,
)

# Delete the created job after processing is done
deleting_dataproc_jobs = PythonOperator(
    task_id='delete_dataproc_jobs',
    python_callable=delete_all_dataproc_jobs,
    op_args=[PROJECT_ID,REGION],  # Pass the argument as a list
    provide_context=True,
    dag=dag,
)

# Flow of the execution
gcs_object_with_prefix_exists >> create_cluster >> pyspark_task >> [delete_cluster, moving_file_to_processed, deleting_dataproc_buckets, deleting_dataproc_jobs]
