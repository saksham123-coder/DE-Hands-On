from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime,timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'trigger_glue_job_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['aws', 'glue'],
) as dag:

    trigger_glue_job = GlueJobOperator(
        task_id='trigger_glue_job',
        job_name='de101_workplace',
        script_location='s3://aws-glue-assets-083252236158-ap-south-1/scripts/de101_workplace.py',
        region_name='ap-south-1',
        aws_conn_id='aws_default',
        iam_role_name='admin_access',
        script_args={'--key': 'value'},
        wait_for_completion=True,
        verbose=True
    )
    
    trigger_glue_job
