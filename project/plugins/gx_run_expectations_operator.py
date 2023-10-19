from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import great_expectations as gx
import os

from run_expectations_plugin import (
    create_database_connection,
    get_schema_table_and_expectation_paths_from_expectation_folder,
    load_custom_expectations,
    fetch_data_from_database,
    update_suite_expectations,
    run_great_expectations_checks,
    generate_sql_inserts,
    execute_sql_statements,
    get_or_create_expectation_suite,
    parse_gx_result
)


class GXRunExpectationsOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        ge_root_dir='/opt/airflow/great_expectations',
        suite_name="my_dq_suite",
        *args, **kwargs
    ):
        super(GXRunExpectationsOperator, self).__init__(*args, **kwargs)
        self.ge_root_dir = ge_root_dir
        self.suite_name = suite_name

    def execute(self, context):

        os.chdir(self.ge_root_dir)

        conn = create_database_connection()
        cur = conn.cursor()
        context = gx.get_context()
        suite = get_or_create_expectation_suite(context, self.suite_name)

        for schema, table, expectations_path in get_schema_table_and_expectation_paths_from_expectation_folder():

            custom_expectations = load_custom_expectations(expectations_path)
            custom_query = custom_expectations.get("custom_query")
            filter = custom_expectations.get("filter")

            df_data = fetch_data_from_database(cur, schema, table, custom_query=custom_query, filter=filter)
            df = gx.dataset.PandasDataset(df_data)

            update_suite_expectations(suite, custom_expectations['expectations'])
            results = run_great_expectations_checks(df, suite)

            parsed_results = parse_gx_result(results)
            insert_statements = generate_sql_inserts(parsed_results)

            execution_results = execute_sql_statements(cur, insert_statements)

            for result in execution_results:
                if result["status"] == "SUCCESS":
                    self.log.info(f"Executed: {result['statement'][:100]}... SUCCESS")
                else:
                    self.log.error(f"Failed: {result['statement'][:100]}... {result['status']}")

            conn.commit()
            cur.close()
            conn.close()
