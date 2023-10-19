from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from run_data_profiler_plugin import profile_data


class GXDataProfilerOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(GXDataProfilerOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        profile_data()
