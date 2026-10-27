from airflow import DAG
from airflow.operators.empty import EmptyOperator

from arirflow.provider.google.cloud.operator.datarproc import(
    DataprocCreateClusterOperator
    DataprocDeleteClusterOperator
    DataprocSumbitJobOperator
)

from datatime import datetime 

#---------------------------------------------------------------------------------------------------------
 GCP config
#--------------------------------------------------------------------------------------------------------
project_id = 'project-e7d60bb2-e6b7-41d1-b28'
region = 'us-central1'
cluster_name = 'banking_crimeflow_cluster'
composer_bucket = ' us-central1-bankproj-b76a42cd-bucke'



Cluster_config = {
 
        "master_config": {
        num_instances = 1,
        'machine_type' = 'e2-standard-2',
        'disk_config':{
            'boot_disk_type': "pd-standard",
            'boot_disk_size_GB': 50,
            },
        },

       "worker_config" = {
        num_instances = 2 
        machine_type = 'e2-standard-2'
        disk_config:{
            boot_disk_type: 'pd-standard'
            boot_size: 50},
        }
    }

with DAG(
    dag_id ='crimeflow_pipeline_DAG'
    start_date =datetime(2026 1 1)
    schedule = None
    catchup =False
    tag =('dataproc', 'crimeflow','pyspark')
)

start_task = EmptyOperator(
 task_id = 'start_pipeline'
)
create_cluster = DataprocCreateClusterOperator(
 task_id = 'create_cluster',
 project_id ='project_id'
 cluster_name = 'cluster_name',
 region = 'region'
 cluster_config ='cluster_config',
)

crimeflow_pipleline = DataprocSumbitJobOperator(
 task_id = 'run_crimeflow_pipleline',
 job ='PYSPARK_JOB'
 project_id ='project_id'
 region = 'region'
)

delete_cluster = DataprocDeleteClusterOperator(
 task_id = 'create_cluster',
 project_id ='project_id'
 cluster_name = 'cluster_name',
 region = 'region'
 cluster_config ='cluster_config',
)

end_task = EmptyOperator(
 task_id = 'end_pipeline'
)

start_task >> create_cluster >> crimeflow_pipleline >> delete_cluster  >> end_task