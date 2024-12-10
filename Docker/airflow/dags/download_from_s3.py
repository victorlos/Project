from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import logging
import boto3
from botocore.exceptions import ClientError
import pendulum
import configparser
import os

config = configparser.ConfigParser()
config.read('/opt/airflow/config/credentials')
aws_access_key = config['default']['aws_access_key_id']
aws_secret_key = config['default']['aws_secret_access_key']



def download_file(key, bucket, file_name):
    """Download a file from an S3 bucket"""


    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key,
                             aws_secret_access_key=aws_secret_key)

    try:
        response = s3_client.download_file(Key=key, Bucket=bucket,Filename=file_name)
        # s3_client.list_objects(Bucket=bucket,Prefix=file_name)
        print(response)
    except ClientError as e:
        logging.error(e)
        print(e)
        return False
    return True

dag = DAG(
    dag_id="download_from_s3",
    catchup=False,
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
    schedule="@daily"
)


download = PythonOperator(
    task_id="download",
    python_callable=download_file,
    op_kwargs = {
            'key': 'test.txt',
            'bucket': 'my-private-bucket2345',
            'file_name': '/opt/airflow/config/test_downloaded.txt',
            },
    dag=dag,
)

download