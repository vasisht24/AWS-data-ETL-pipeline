import gzip
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.http.hooks.http import HttpHook

data_cfg = [
    {
        "url": "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "s3_key": "imdb-input/rating/imdb_rating.tsv",
        "dataset": "rating",
    },
    {
        "url": "https://datasets.imdbws.com/title.episode.tsv.gz",
        "s3_key": "imdb-input/episode/imdb_episode.tsv",
        "dataset": "episode",
    },
]


def download_decompress_upload_to_s3(url, s3_key, **kwargs):
    S3_CONN_ID = "s3_conn_id"

    # download compressed imdb dataset
    http_hook = HttpHook(method="GET", http_conn_id=None)
    response = http_hook.run(url)

    # decompress data
    decompress_data = gzip.decompress(response.content)

    # upload decompress data to s3
    s3_hook = S3Hook(S3_CONN_ID)
    s3_hook.load_string(
        string_data=decompress_data.decode("utf-8"),
        key=s3_key,
        bucket_name="imdb-data-folder",
        replace=True,
    )


# Define default_args and other DAG configurations
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    "imdb_data_ingestion",
    start_date=datetime(2023, 1, 1),
    description="downloading and uploading data",
    catchup=False,
)

# Create a starting task (dummy_task) before the loop
empty_task = EmptyOperator(task_id="start_task", dag=dag)

fetch_tasks = []
for d in data_cfg:
    task_id = f"ingesting_{d['dataset']}"
    fetch_task = PythonOperator(
        task_id=task_id,
        python_callable=download_decompress_upload_to_s3,
        provide_context=True,
        op_kwargs={
            "url": d["url"],
            "s3_key": d["s3_key"],
        },
        dag=dag,
    )
    fetch_tasks.append(fetch_task)

run_glue_job = GlueJobOperator(
    task_id="run_glue_job",
    job_name="demo-copy",
    script_location="s3://aws-glue-assets-643548402900-ap-south-1/scripts/demo-copy.py",
    aws_conn_id="aws_conn_id",
    s3_bucket="aws-glue-assets-643548402900-ap-south-1",
    dag=dag,
    region_name="ap-south-1",
)

# Set up Dependencies
empty_task >> fetch_tasks[0]
for i in range(len(fetch_tasks) - 1):
    fetch_tasks[i] >> fetch_tasks[i + 1]

fetch_tasks[-1] >> run_glue_job
