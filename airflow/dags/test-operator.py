import pendulum

from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.decorators import dag, task

import sys
print(sys.path)

with DAG(
  dag_id='test-operator',
  schedule_interval='0 1 * * *', # */5 * * * *
  start_date=pendulum.datetime(2022, 5, 23, tz="Asia/Seoul"),
  catchup=False,
  tags=['catcher'],
) as dag:
  @task(task_id="preprocess_data")
  def transform():
    print('test')

  transform()