from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import great_expectations as gx
import os

import run_expectations_plugin as expectation_plugin
import process_validations_plugins as validations_plugin
import process_files_plugin as file_plugin


class GXRunExpectationsOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        ge_root_dir='/opt/airflow/great_expectations',
        suite_name="data_validation_suite",
        *args, **kwargs
    ):
        super(GXRunExpectationsOperator, self).__init__(*args, **kwargs)
        self.ge_root_dir = ge_root_dir
        self.suite_name = suite_name

    def execute(self, context):
        """Main execution method for the Airflow operator."""
        os.chdir(self.ge_root_dir)

        conn = validations_plugin.create_database_connection()
        cur = conn.cursor()
        context = gx.get_context()

        for schema, table, expectations_path in file_plugin.get_schema_table_and_expectation_paths():
            custom_expectations = file_plugin.load_custom_expectations(expectations_path)

            validation_results = self._process_and_run_expectations(context, custom_expectations, schema, table)
            validations_plugin.ingest_results_into_database(cur, validation_results, schema, table)

        conn.commit()
        cur.close()
        conn.close()

    def _process_and_run_expectations(self, context, custom_expectations, schema, table):
        """Process the custom expectations and run them using Great Expectations."""
        custom_query = custom_expectations.get("custom_query")
        custom_filter = custom_expectations.get("custom_filter")

        suite = expectation_plugin.get_or_create_expectation_suite(context, self.suite_name)
        expectation_plugin.update_suite_expectations(suite, custom_expectations['expectations'])
        context.save_expectation_suite(suite)

        batch_request = expectation_plugin.create_pg_batch_request(
            schema=schema,
            table=table,
            custom_query=custom_query,
            custom_filter=custom_filter
        )
        expectation_plugin.setup_pg_datasource(context)

        return expectation_plugin.create_and_run_checkpoint(
            context=context,
            batch_request=batch_request,
            suite_name=self.suite_name,
        )
