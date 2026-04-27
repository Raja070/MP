from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
    'retries': 2,
    'retry_dealy': timedelta(minutes=5)
}

with DAG(
    dag_id = 'spark_dag',
    default_args = default_args,
    schedule_interval = '*/10 * * * *',
    catch_up = False,
) as dag:
    dag_start = EmptyOperator(
        task_id = 'dag_start'
    )

    trigger_spark = SparkSubmitOperator(
        task_id = 'submit_spark_job',
        application = '/path/to/your/script.py',
        conn_id = 'spark_default',
        executor_cores=2,
        executor_memory='2g',
        driver_memory = '1g',
        name = 'airflow_spark_job',
        config={'spark.master': 'yarn'},
        verbose  = True
    )

    dag_end = EmptyOperator(task_id = 'dag_end')

    dag_start >> trigger_spark >> dag_end