import os
import logging
import requests 

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

#dataset_file = "fhv_tripdata_2019-01.csv.gz"
#dataset_files = ["fhv_tripdata_2019-01.csv.gz","fhv_tripdata_2019-02.csv.gz","fhv_tripdata_2019-03.csv.gz","fhv_tripdata_2019-04.csv.gz","fhv_tripdata_2019-05.csv.gz","fhv_tripdata_2019-06.csv.gz","fhv_tripdata_2019-07.csv.gz","fhv_tripdata_2019-08.csv.gz","fhv_tripdata_2019-09.csv.gz","fhv_tripdata_2019-10.csv.gz","fhv_tripdata_2019-11.csv.gz","fhv_tripdata_2019-12.csv.gz"]
months = list(range(1,13))
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'fhv_data_all')
print("Initialization happening")

def download_dataset(dataset_url, months, path_to_local_home):
    
    for month in months:
        filename=f"fhv_tripdata_2019-{month:02}.csv.gz"
        full_url=dataset_url+filename
        print (f"Downloading file from URL {full_url}")
        #response = requests.get(full_url)
        
        #target_file_path=
       # with open(f"{path_to_local_home}/{datafile}", "wb") as f:
        #    f.write(response.content)
        
        os.system(f"wget {full_url} -P {path_to_local_home}")
        print(f"{filename} downloadedd")
        


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, months, local_filepath):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)
    print("Bucket name is ",bucket)

    for month in months:
        filename=f"fhv_tripdata_2019-{month:02}.csv.gz"
        object_name=f"Week3_HW/{filename}"
        blob = bucket.blob(object_name)
        local_file=f"{local_filepath}/{filename}"
        blob.upload_from_filename(local_file)
        print(f"{filename} uploaded to GCS bucket")


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="Week3_HW",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_dataset,
        op_kwargs={
            "dataset_url": dataset_url,
            "months": months,
            "path_to_local_home": path_to_local_home,
        },
    )


    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "months": months,
            "local_filepath": path_to_local_home,
        },
    )



    download_dataset_task >> local_to_gcs_task