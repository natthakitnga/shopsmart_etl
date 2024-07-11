from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.func_utils import get_file_path
import pendulum
import logging

# define
customer_dir = "./data/product_catalog/input"
file_key_word = "^product_catalog.*"
spark_path = "/opt/airflow/include/spark_process_product.py"

@dag(schedule=None, start_date=pendulum.datetime(2024, 7, 9, tz="UTC"), catchup=False)
def process_product():
    @task(task_id="get_path")
    def get_path():
        file_path = get_file_path(customer_dir, file_key_word)
        if not file_path:
            logging.info('expected file not found.')
            raise AirflowSkipException
        return file_path
        
    submit_job = SparkSubmitOperator(
        task_id="submit_spark_process",
        name="spark_process",
        application=spark_path,
        conn_id="spark_default",
        application_args=["{{ ti.xcom_pull(task_ids='get_path', key='return_value') }}"],
        jars="/opt/airflow/include/jars/postgresql-42.7.3.jar",
        )
    
    get_path = get_path()
    get_path >> submit_job

process_product()