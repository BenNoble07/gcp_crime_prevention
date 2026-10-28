from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

from datetime import datetime 

#---------------------------------------------------------------------------------------------------------
#GCP config
#--------------------------------------------------------------------------------------------------------
project_id = 'project-e7d60bb2-e6b7-41d1-b28'
region = 'us-central1'
cluster_name = 'banking-crimeflow-cluster'
composer_bucket = 'us-central1-bankcrimeflow-c57f7f77-bucket'


cluster_config = {
        "master_config": {
          'num_instances': 1,
          'machine_type_uri':'e2-standard-2',
          'disk_config':{
            'boot_disk_type': "pd-standard",
            'boot_disk_size_gb': 50,
            },
        },

       "worker_config" :{
         'num_instances': 2, 
         'machine_type_uri' : 'e2-standard-2',
         'disk_config':{
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 50},
        },
    }

PYSPARK_JOB = {
    "reference": {
        "project_id": project_id
    },
    "placement": {
        "cluster_name": cluster_name
    },
    "pyspark_job": {
        "main_python_file_uri": "gs://keystone_26/scripts/CrimePreventionSpark.py"
    }
}

with DAG(
    dag_id ='crimeflow_pipeline_DAG',
    start_date = datetime(2026, 1 ,1),
    schedule = None,
    catchup = False,
    tags=['dataproc', 'crimeflow','pyspark']
)as dag:

    start_task = EmptyOperator(
    task_id = 'start_pipeline'
    )
    create_cluster = DataprocCreateClusterOperator(
    task_id = 'create_cluster',
    project_id = project_id,
    cluster_name = cluster_name,
    region = region,
    cluster_config =cluster_config,
    )

    crimeflow_pipeline = DataprocSubmitJobOperator(
    task_id = 'run_crimeflow_pipeline',
    job = PYSPARK_JOB,
    project_id =project_id,
    region = region,
    )

    delete_cluster = DataprocDeleteClusterOperator(
    task_id = 'delete_cluster',
    project_id =project_id,
    cluster_name = cluster_name,
    region = region,
    trigger_rule ='all_done',
    )

    end_task = EmptyOperator(
    task_id = 'end_pipeline'
    )

    start_task >> create_cluster >> crimeflow_pipeline >> delete_cluster  >> end_task