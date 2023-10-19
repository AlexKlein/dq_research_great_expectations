import sys
import importlib.util
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DB_GEN_ROOT_DIR = '/opt/airflow/db_generator'
MODULE_PATH = '/opt/airflow/db_generator/generator.py'
MODULE_NAME = 'generator'

DEFAULT_ARGS = {
    'owner': 'aleksandr.klein',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


def execute_db_generation():
    """Import and execute the start_app function from custom_expectations.py."""
    sys.path.insert(0, DB_GEN_ROOT_DIR)

    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
    db_generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(db_generator)

    db_generator.start_generation()


dag_config = {
    'dag_id': 'db_generation',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run DB generator',
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2023, 10, 9)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    db_generation = PythonOperator(
        task_id='db_generation',
        python_callable=execute_db_generation,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    start >> db_generation >> end
