import sys
import importlib.util
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


GE_PLUGINS_DIR = '/opt/airflow/plugins'
OPERATOR_MODULE_PATH = f'{GE_PLUGINS_DIR}/gx_run_comparison_operator.py'
OPERATOR_MODULE_NAME = 'gx_run_comparison_operator'

DEFAULT_ARGS = {
    'owner': 'aleksandr.klein',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


def get_gx_run_comparison_operator():
    """Dynamically import and return the GXRunComparisonOperator class."""
    sys.path.insert(0, GE_PLUGINS_DIR)

    spec = importlib.util.spec_from_file_location(OPERATOR_MODULE_NAME, OPERATOR_MODULE_PATH)
    custom_operator_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(custom_operator_module)

    return getattr(custom_operator_module, "GXRunComparisonOperator")


dag_config = {
    'dag_id': 'gx_run_comparison',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run GX comparisons',
    'schedule_interval': '30 8 * * MON',
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2023, 10, 9)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    GXRunComparisonOperator = get_gx_run_comparison_operator()

    run_comparison = GXRunComparisonOperator(
        task_id='run_comparison',
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    start >> run_comparison >> end
