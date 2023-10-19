import fnmatch
import os

import pandas as pd
import psycopg2
from ruamel import yaml
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# Constants
CONTEXT_ROOT_DIR = "/opt/airflow/great_expectations"
SUITE_NAME = "my_profiler_suite"
STORED_PROFILES_DIR = "/opt/airflow/profilers/stored_profiles"
PROFILER_CONFIG = "/opt/airflow/profilers/profiling_config.yml"

EXCLUDE_EXPECTATION_TYPES = [
    "expect_column_values_to_be_between",
    "expect_column_quantile_values_to_be_between",
    "expect_column_median_to_be_between",
    "expect_column_mean_to_be_between"
]


def load_profiling_config(file_path=PROFILER_CONFIG):
    with open(file_path, 'r') as stream:
        return yaml.safe_load(stream)


def create_database_connection():
    """Create a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='my_small_dwh',
        port=5432
    )
    return conn


def get_partition_filter(cur, table_name):
    """Fetch the latest partition for a given table."""
    schema_name, actual_table_name = table_name.split('.')

    sql_query = f"""
        SELECT 
            CASE
            WHEN data_type IN ('date', 'timestamp without time zone', 'timestamp with time zone')
            THEN 'WHERE CAST('||column_name||' AS DATE) = DATE_TRUNC(''MONTH'', CURRENT_DATE)'
            ELSE 'WHERE FALSE'
            END AS table_filter

        FROM 
            information_schema.columns

        WHERE
            table_schema = %s
            AND table_name = %s
            AND column_name LIKE 'partition%%'
    """

    cur.execute(sql_query, (schema_name, actual_table_name))
    row = cur.fetchone()
    if row:
        return row[0]
    return None


def fetch_data_from_database(cur, table_name):
    """Execute SQL query and return results as Pandas DataFrame."""
    partition_clause = get_partition_filter(cur, table_name) or ""

    sql_query = f"SELECT * FROM {table_name} {partition_clause} LIMIT 1000"
    cur.execute(sql_query)

    results = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    return pd.DataFrame(data=results, columns=columns)


def create_new_suite(context, suite_name=SUITE_NAME):
    """Create a new expectation suite."""
    context.add_or_update_expectation_suite(suite_name)
    print(f"Suite {suite_name} created.")


def ensure_directory_exists(directory):
    """Ensure the directory exists. If not, create it."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def save_expectation_to_file(expectation_suite, file_path):
    """Save the expectation suite (plain text) to a file."""
    ensure_directory_exists(os.path.dirname(file_path))
    with open(file_path, 'w') as file:
        file.write(str(expectation_suite))


def generate_expectations_using_assistant(context, batch_request, full_path, exclude_column_names):
    data_assistant_result = context.assistants.onboarding.run(
        batch_request=batch_request,
        exclude_column_names=exclude_column_names
    )

    expectation_suite = data_assistant_result.get_expectation_suite(
        expectation_suite_name=SUITE_NAME
    )

    expectations = expectation_suite.expectations
    filtered_expectations = [
        exp for exp in expectations if exp.expectation_type not in EXCLUDE_EXPECTATION_TYPES
    ]
    expectation_suite.expectations = filtered_expectations

    context.add_or_update_expectation_suite(expectation_suite=expectation_suite)
    save_expectation_to_file(expectation_suite, full_path)


def setup_datasource(context):
    """Setup the Great Expectations datasource."""
    datasource_config = {
        "name": "data_profiler_datasource",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "data_profiler_connector": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["default_identifier_name"],
            },
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))
    context.add_datasource(**datasource_config)


def fetch_data_and_create_batch_request(cur, table_name):
    """Fetch data from PostgreSQL and create a batch request."""
    df_data = fetch_data_from_database(cur, table_name)

    batch_request = RuntimeBatchRequest(
        datasource_name="data_profiler_datasource",
        data_connector_name="data_profiler_connector",
        data_asset_name="data_profiler_asset",
        runtime_parameters={"batch_data": df_data},
        batch_identifiers={"default_identifier_name": "data_assistant_profiler_identifier"}
    )

    return batch_request


def profile_data():
    """Run the profiler, generate an expectation suite, and save it."""
    config = load_profiling_config()
    conn = create_database_connection()
    cur = conn.cursor()

    context = gx.get_context()
    setup_datasource(context)

    for expectations_config in config["my_small_dwh"]:
        database_name = expectations_config["database"]

        for schema_config in expectations_config["schema_name"]:
            schema_name = schema_config.get('schema_name')

            for table_config in expectations_config["tables"]:
                table_name_or_mask = table_config.get('table_name_mask')
                exclude_columns = table_config.get('exclude_columns', [])

                if '*' in table_name_or_mask:
                    cur.execute(f"""
                        SELECT table_name 
                        FROM   information_schema.tables 
                        WHERE  table_schema = %s
                    """, (schema_name,))

                    tables_in_schema = [row[0] for row in cur.fetchall()]
                    matching_tables = [t for t in tables_in_schema if fnmatch.fnmatch(t, table_name_or_mask)]
                else:
                    matching_tables = [table_name_or_mask]

                for table_name in matching_tables:
                    full_table_name = f"{schema_name}.{table_name}"
                    full_path = os.path.join(STORED_PROFILES_DIR, schema_name, f"{table_name}.json")

                    if os.path.exists(full_path):
                        print(f"You already profiled {full_table_name} table. Check the file {full_path}")
                    else:
                        create_new_suite(context)
                        batch_request = fetch_data_and_create_batch_request(
                            cur=cur,
                            table_name=full_table_name
                        )
                        generate_expectations_using_assistant(context, batch_request, full_path, exclude_columns)
