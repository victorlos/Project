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
config.read('/opt/airflow/dags/credentials')
aws_access_key = config['default']['aws_access_key_id']
aws_secret_key = config['default']['aws_secret_access_key']



def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket"""

    if object_name is None:
            object_name = os.path.basename('test.txt') 

    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key,
                             aws_secret_access_key=aws_secret_key)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

dag = DAG(
    dag_id="upload_to_s3",
    catchup=False,
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
    schedule="@daily"
)

dummy = DummyOperator(
    task_id="test",
    dag=dag
)

upload= PythonOperator(
    task_id="upload",
    python_callable=upload_file,
    op_kwargs = {
            'file_name': '/opt/airflow/dags/test.txt',
            'bucket': 'my-private-bucket2345',
            'object_name': None,
            },
    dag=dag,
)

dummy >> upload