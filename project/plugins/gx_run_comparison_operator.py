from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import great_expectations as gx
import os

import run_expectations_plugin as expectation_plugin
import process_validations_plugins as validations_plugin
import process_files_plugin as file_plugin


class GXRunComparisonOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        ge_root_dir='/opt/airflow/great_expectations',
        suite_name="data_comparison_suite",
        *args, **kwargs
    ):
        super(GXRunComparisonOperator, self).__init__(*args, **kwargs)
        self.ge_root_dir = ge_root_dir
        self.suite_name = suite_name

    def execute(self, context):
        """Main execution method for the Airflow operator."""
        os.chdir(self.ge_root_dir)

        conn = validations_plugin.create_database_connection()
        cur = conn.cursor()
        context = gx.get_context()

        for instance_id, source_schema, source_table, source_custom_filter, \
                target_schema, target_table, target_custom_filter, source, target in file_plugin.load_comparison_config():

            validation_results = self._process_and_run_expectations(
                context,
                source_schema,
                source_table,
                source_custom_filter,
                target_schema,
                target_table,
                target_custom_filter,
                source,
                target
            )

            validations_plugin.ingest_results_into_database(cur, validation_results, source_schema, source_table)

        conn.commit()
        cur.close()
        conn.close()

    def _process_and_run_expectations(self, context, source_schema, source_table, source_custom_filter,
                                      target_schema, target_table, target_custom_filter, source, target):
        """Create custom expectations and run them using Great Expectations."""
        if source == "postgres" and target == "postgres":
            expectation_plugin.setup_pg_datasource(context)

            source_batch_request = expectation_plugin.create_pg_batch_request(
                schema=source_schema,
                table=source_table,
                custom_filter=source_custom_filter,
            )
            target_batch_request = expectation_plugin.create_pg_batch_request(
                schema=target_schema,
                table=target_table,
                custom_filter=target_custom_filter,
            )
        else:
            self.log.error(f"Error: Set correctly source and target platforms in config file")
            raise f"Error: Set correctly source and target platforms in config file"

        expectation_plugin.profile_initial_data(context, source_batch_request, self.suite_name)

        return expectation_plugin.create_and_run_checkpoint(
            context=context,
            batch_request=target_batch_request,
            suite_name=self.suite_name
        )
