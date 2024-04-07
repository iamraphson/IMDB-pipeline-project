import logging
import os
from airflow import DAG
import datetime as dt
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    'owner': 'iamraphson',
    'depends_on_past': False,
    'retries': 2
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
GCP_IMDB_BUCKET = os.environ.get('GCP_IMDB_BUCKET')
GCP_IMDB_WH_DATASET = os.environ.get('GCP_IMDB_WH_DATASET')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_PROJECT_REGION = os.environ.get('GCP_PROJECT_REGION', 'us-west1')
GCP_IMDB_DATAPROC_TEMP_BUCKET = os.environ.get('GCP_IMDB_DATAPROC_TEMP_BUCKET', 'dataproc-temp-us-west1-475254441817-u7kv6jeo')
download_path = f'{AIRFLOW_HOME}/raws/'
scripts_path = f'{AIRFLOW_HOME}/dags/scripts/'
INIT_DATA_XFORM_URI=f'gs://{GCP_IMDB_BUCKET}/scripts/init-data-transformation.py'
XFORM_STEP_ONE_URI=f'gs://{GCP_IMDB_BUCKET}/scripts/transformation_step_one.py'

imdb_datasets = {
    'name.basics': 'name_basics',
    'title.akas': 'title_akas',
    'title.basics': 'title_basics',
    'title.crew': 'title_crew',
    'title.episode': 'title_episode',
    'title.principals': 'title_principals',
    'title.ratings': 'title_ratings',
}
with DAG(
    'IMDB-DAG',
    default_args=default_args,
    tags=['IMDB-DAG'],
    schedule_interval='0 13 * * *',
    start_date=dt.datetime.today()
) as dag:
    start_task = DummyOperator(task_id='start_task', dag=dag)

    with TaskGroup('download-dataset-tasks') as download_dataset_task:
        for imdb_dataset in imdb_datasets.keys():
            BashOperator(
                dag=dag,
                task_id=f'download_{imdb_dataset}_task',
                bash_command=f'curl --create-dirs -o {download_path}{imdb_dataset}.tsv.gz https://datasets.imdbws.com/{imdb_dataset}.tsv.gz'
            )


    upload_dataset_to_gcs_task = LocalFilesystemToGCSOperator(
        dag=dag,
        task_id='upload_dataset_to_gcs_task',
        src=f'{download_path}*.tsv.gz',
        dst='raws/',
        bucket=GCP_IMDB_BUCKET
    )

    upload_scripts_to_gcs_task = LocalFilesystemToGCSOperator(
        dag=dag,
        task_id='upload_scripts_to_gcs_task',
        src=f'{scripts_path}*.py',
        dst='scripts/',
        bucket=GCP_IMDB_BUCKET
    )

    with TaskGroup('init-data-xform-tasks') as init_data_xform_task:
        for key, value  in imdb_datasets.items():
            init_data_xform_job = {
                'reference': {'project_id': GCP_PROJECT_ID},
                'placement': {'cluster_name': f'imdb-spark-cluster-{GCP_PROJECT_ID}'},
                'pyspark_job': {
                    'main_python_file_uri': INIT_DATA_XFORM_URI,
                    'jar_file_uris': [
                        'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar',
                    ],
                    'args': [
                        f'--src_input=gs://{GCP_IMDB_BUCKET}/raws/{key}.tsv.gz',
                        f'--selected_data={value}',
                        f'--dest_output=gs://{GCP_IMDB_BUCKET}/pq/{value}',
                        f'--spark_tmp_bucket={GCP_IMDB_DATAPROC_TEMP_BUCKET}',
                    ]
                }
            }

            DataprocSubmitJobOperator(
                task_id=f'process_{key}_init_data_xform_task',
                dag=dag,
                job=init_data_xform_job,
                region=GCP_PROJECT_REGION,
                project_id=GCP_PROJECT_ID,
                trigger_rule=TriggerRule.ALL_DONE
            )

    facts_dim_step_1_creations_tasks = DataprocSubmitJobOperator(
        task_id='facts_dim_step_1_creations_tasks',
        dag=dag,
        region=GCP_PROJECT_REGION,
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
        job={
            'reference': {'project_id': GCP_PROJECT_ID},
            'placement': {'cluster_name': f'imdb-spark-cluster-{GCP_PROJECT_ID}'},
            'pyspark_job': {
                'main_python_file_uri': XFORM_STEP_ONE_URI,
                'jar_file_uris': [
                    'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar',
                    'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
                ],
                'args': [
                    f'--title_basics_src=gs://{GCP_IMDB_BUCKET}/pq/title_basics/*',
                    f'--title_ratings_src=gs://{GCP_IMDB_BUCKET}/pq/title_ratings/*',
                    f'--fact_movies_pq_dest=gs://{GCP_IMDB_BUCKET}/pq/fact_movies',
                    f'--fact_genres_pq_dest=gs://{GCP_IMDB_BUCKET}/pq/fact_genres',
                    f'--fact_movies_table_dest={GCP_IMDB_WH_DATASET}.facts_movies',
                    f'--dim_genres_table_dest={GCP_IMDB_WH_DATASET}.dim_genres',
                    f'--spark_tmp_bucket={GCP_IMDB_DATAPROC_TEMP_BUCKET}',
                ]
            }
        }
    )

    clean_up_dataset_temp_store_task = BashOperator(
        task_id='clean_up_dataset_temp_store_task',
        dag=dag,
        bash_command=f'rm -rf {download_path}'
    )

    start_task.set_downstream(download_dataset_task)
    download_dataset_task.set_downstream(upload_dataset_to_gcs_task)
    upload_dataset_to_gcs_task.set_downstream(upload_scripts_to_gcs_task)
    upload_scripts_to_gcs_task.set_downstream(init_data_xform_task)
    init_data_xform_task.set_downstream(facts_dim_step_1_creations_tasks)
    facts_dim_step_1_creations_tasks.set_downstream(clean_up_dataset_temp_store_task)
