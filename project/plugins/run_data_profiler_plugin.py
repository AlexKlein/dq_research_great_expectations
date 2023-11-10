import fnmatch
import json
import os

import pandas as pd
import psycopg2
from ruamel import yaml
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest


# Constants
STORED_PROFILES_DIR = os.environ.get("STORED_PROFILES_DIR")
PROFILER_CONFIG = os.environ.get("PROFILER_CONFIG")

SUITE_NAME = os.environ.get("PROFILER_SUITE_NAME")
LIMIT_OF_PROFILED_ROWS = os.environ.get("LIMIT_OF_PROFILED_ROWS")


EXCLUDE_EXPECTATION_TYPES = [
    "expect_column_values_to_be_between",
    "expect_column_quantile_values_to_be_between",
    "expect_column_median_to_be_between",
    "expect_column_mean_to_be_between",
    "expect_column_stdev_to_be_between",
    "expect_column_proportion_of_unique_values_to_be_between",
    "expect_column_values_to_be_in_set"
]


# Utility Functions
def load_profiling_config(file_path=PROFILER_CONFIG):
    with open(file_path, 'r') as stream:
        return yaml.safe_load(stream)


def ensure_directory_exists(directory):
    """Ensure the directory exists. If not, create it."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def save_expectation_to_file(expectation_suite, file_path):
    """Save the expectation suite (plain text) to a file."""
    ensure_directory_exists(os.path.dirname(file_path))
    with open(file_path, 'w') as file:
        file.write(str(expectation_suite))


def remove_meta_from_expectation_file(full_path):
    """Read the expectations from a file, remove the 'meta' key, and save it back."""
    with open(full_path, 'r') as file:
        expectation_suite_dict = json.load(file)

    for expectation in expectation_suite_dict.get('expectations', []):
        expectation.pop('meta', None)

    with open(full_path, 'w') as file:
        json.dump(expectation_suite_dict, file, indent=2)


# Database functions
def create_database_connection(database_name):
    """Create a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname=database_name,
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
            THEN 'WHERE CAST('||column_name||' AS DATE) = CAST(DATE_TRUNC(''MONTH'', CURRENT_DATE) AS DATE)'
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


def fetch_data_from_database(cur, full_table_name):
    """Execute SQL query and return results as Pandas DataFrame."""
    partition_clause = get_partition_filter(cur, full_table_name) or ""

    sql_query = f"SELECT * FROM {full_table_name} {partition_clause} LIMIT {LIMIT_OF_PROFILED_ROWS}"
    cur.execute(sql_query)

    results = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    return pd.DataFrame(data=results, columns=columns)


# Great Expectations Functions
def create_new_suite(context, suite_name=SUITE_NAME):
    """Create a new expectation suite."""
    context.add_or_update_expectation_suite(suite_name)
    print(f"Suite {suite_name} created.")


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
    remove_meta_from_expectation_file(full_path)


# Main Processing Functions
def profile_data():
    """Main function to profile data."""
    config = load_profiling_config()
    context = gx.get_context()
    setup_datasource(context)

    for db_config in config:
        process_database(db_config, context)


def process_database(db_config, context):
    """Process each database specified in the configuration."""
    database_name = db_config["database"]
    conn = create_database_connection(database_name)
    cur = conn.cursor()

    for schema_config in db_config["schemas"]:
        process_schema(schema_config, cur, context)


def process_schema(schema_config, cur, context):
    """Process each schema in the database."""
    schema_name = schema_config.get('schema_name')

    for table_config in schema_config["tables"]:
        process_table(table_config, schema_name, cur, context)


def process_table(table_config, schema_name, cur, context):
    """Process each table in the schema."""
    table_name_or_mask = table_config.get('table_name_mask')
    exclude_columns = table_config.get('exclude_columns', [])
    matching_tables = get_matching_tables(table_name_or_mask, schema_name, cur)

    for table_name in matching_tables:
        process_individual_table(schema_name, table_name, exclude_columns, cur, context)


def get_matching_tables(table_name_or_mask, schema_name, cur):
    """Return a list of matching tables based on the mask."""
    if '*' in table_name_or_mask:
        cur.execute("""
            SELECT table_name 
            FROM   information_schema.tables 
            WHERE  table_schema = %s
        """, (schema_name,))

        tables_in_schema = [row[0] for row in cur.fetchall()]
        return [t for t in tables_in_schema if fnmatch.fnmatch(t, table_name_or_mask)]
    else:
        return [table_name_or_mask]


def process_individual_table(schema_name, table_name, exclude_columns, cur, context):
    """Process an individual table."""
    full_table_name = f"{schema_name}.{table_name}"
    full_path = os.path.join(STORED_PROFILES_DIR, schema_name, f"{table_name}.json")

    if os.path.exists(full_path):
        print(f"You already profiled {full_table_name} table. Check the file {full_path}")
    else:
        create_new_suite(context)
        batch_request = fetch_data_and_create_batch_request(cur, full_table_name)
        generate_expectations_using_assistant(context, batch_request, full_path, exclude_columns)
