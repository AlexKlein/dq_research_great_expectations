import os
import json
import pandas as pd
import psycopg2 as psycopg2
from great_expectations.core import ExpectationConfiguration

# Configuration constants
SUITE_NAME = "my_dq_suite"
VALIDATION_RESULTS_TABLE_NAME = "data_quality.gx_validation_results"
ONLY_RETURN_FAILURES = False

RUN_ENV = os.environ.get('RUN_ENV', 'local')


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


def fetch_data_from_database(cur, schema, table, custom_query=None, filter=None):
    """Execute SQL query and return results as Pandas DataFrame."""
    if custom_query:
        sql_query = custom_query
    else:
        sql_query = f"SELECT * FROM {schema}.{table} AS d"
        if filter:
            sql_query += f" WHERE {filter}"

    cur.execute(sql_query)

    results = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    return pd.DataFrame(data=results, columns=columns)


def compress_numbers_to_ranges(nums):
    """Convert a list of numbers to a string representation of ranges."""
    if not nums:
        return ""

    nums = sorted(nums)

    range_start = nums[0]
    range_end = nums[0]

    ranges = []
    for n in nums[1:]:
        if n == range_end + 1:
            range_end = n
        else:
            if range_start == range_end:
                ranges.append(str(range_start))
            else:
                ranges.append(f"{range_start}-{range_end}")
            range_start = range_end = n

    if range_start == range_end:
        ranges.append(str(range_start))
    else:
        ranges.append(f"{range_start}-{range_end}")

    return ", ".join(ranges)


def parse_gx_result(result):
    parsed_data = []
    results = result.get('results', [])
    for res in results:
        unexpected_list = list(
            set(res.get('result', {}).get('unexpected_list', res.get('result', {}).get('partial_unexpected_list', []))))
        unexpected_index_list = list(set(res.get('result', {}).get('unexpected_index_list', res.get('result', {}).get(
            'partial_unexpected_index_list', []))))
        status = "Success" if res.get('success', False) else "Failed"
        parsed_record = {
            'status': status,
            'check_type': res.get('expectation_config', {}).get('expectation_type', None),
            'check_column': res.get('expectation_config', {}).get('kwargs', {}).get('column', None),
            'check_condition': res.get('expectation_config', {}).get('kwargs', {}).get('value_set', None),
            'element_count': res.get('result', {}).get('element_count', None),
            'missing_count': res.get('result', {}).get('missing_count', None),
            'missing_percent': res.get('result', {}).get('missing_percent', None),
            'unexpected_count': res.get('result', {}).get('unexpected_count', None),
            'unexpected_percent': res.get('result', {}).get('unexpected_percent', None),
            'unexpected_percent_total': res.get('result', {}).get('unexpected_percent_total', None),
            'unexpected_percent_nonmissing': res.get('result', {}).get('unexpected_percent_nonmissing', None),
            'unexpected_list': unexpected_list,
            'unexpected_index_list': unexpected_index_list,
            'raised_exception': res.get('exception_info', {}).get('raised_exception', None),
            'exception_message': res.get('exception_info', {}).get('exception_message', None),
            'exception_traceback': res.get('exception_info', {}).get('exception_traceback', None)
        }
        parsed_data.append(parsed_record)

    return parsed_data


def generate_sql_inserts(parsed_data):
    """Generate SQL INSERT statements based on parsed data."""
    create_table = f"""
    CREATE TABLE IF NOT EXISTS {VALIDATION_RESULTS_TABLE_NAME} (
        status                        TEXT,
        check_type                    TEXT,
        check_column                  TEXT,
        check_condition               TEXT,
        element_count                 BIGINT,
        missing_count                 BIGINT,
        missing_percent               DOUBLE PRECISION,
        unexpected_count              BIGINT,
        unexpected_percent            DOUBLE PRECISION,
        unexpected_percent_total      DOUBLE PRECISION,
        unexpected_percent_nonmissing DOUBLE PRECISION,
        unexpected_list               TEXT,
        unexpected_index_list         TEXT,
        raised_exception              TEXT,
        exception_message             TEXT,
        exception_traceback           TEXT
    );
    """

    sql_statements = [create_table]

    numeric_fields = [
        "element_count", "missing_count", "missing_percent",
        "unexpected_count", "unexpected_percent", "unexpected_percent_total",
        "unexpected_percent_nonmissing"
    ]

    for record in parsed_data:
        formatted_record = {}
        for key, value in record.items():
            if value == "NaT" or value is pd.NaT:
                value = 'NULL'
            elif key == 'unexpected_list' and isinstance(value, list):
                if value:
                    value = f"'{', '.join(map(str, value))}'"
                else:
                    value = 'NULL'
            elif key == 'unexpected_index_list' and isinstance(value, list):
                if value:
                    value = f"'{compress_numbers_to_ranges(value)}'"
                else:
                    value = 'NULL'
            elif key == 'check_condition' and isinstance(value, list):
                value = f"'{', '.join(map(str, value))}'"
            elif value is None:
                value = 'NULL'
            elif key in numeric_fields:
                value = str(value)
            else:
                value = f"'{value}'"
            formatted_record[key] = value
        columns = ', '.join(formatted_record.keys())
        values = ', '.join(formatted_record.values())
        insert_statement = f"INSERT INTO {VALIDATION_RESULTS_TABLE_NAME} ({columns}) VALUES ({values});"
        sql_statements.append(insert_statement)

    return sql_statements


def get_schema_table_and_expectation_paths_from_expectation_folder(root_dir="expectations"):
    """Extract schema, table names and the path to the expectation .json file from the directory structure."""
    os.chdir('/opt/airflow/great_expectations/')
    schema_table_expectation_paths = []

    schemas = [d for d in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, d))]

    for schema in schemas:
        schema_path = os.path.join(root_dir, schema)

        tables = [t for t in os.listdir(schema_path) if os.path.isdir(os.path.join(schema_path, t))]

        for table in tables:
            table_path = os.path.join(schema_path, table)

            expectations = [f for f in os.listdir(table_path) if f.endswith(".json")]

            for expectation in expectations:
                expectation_path = os.path.join(table_path, expectation)
                schema_table_expectation_paths.append((schema, table, expectation_path))

    return schema_table_expectation_paths


def load_custom_expectations(file_path):
    """Load custom expectations from a given file."""
    os.chdir('/opt/airflow/great_expectations/')
    with open(file_path, 'r') as f:
        return json.load(f)


def get_or_create_expectation_suite(context, suite_name):
    """Retrieve an existing expectation suite from the context or create a new one if it doesn't exist."""
    if suite_name not in context.list_expectation_suite_names():
        return context.create_expectation_suite(suite_name, overwrite_existing=True)
    return context.get_expectation_suite(suite_name)


def update_suite_expectations(suite, expectations):
    """Update an expectation suite with a given list of expectations."""
    suite.expectations = [ExpectationConfiguration(**expectation) for expectation in expectations]


def run_great_expectations_checks(df, suite):
    """Validate data against the specified expectations and print results."""
    return df.validate(expectation_suite=suite, only_return_failures=ONLY_RETURN_FAILURES, result_format='COMPLETE')


def execute_sql_statements(cur, sql_statements):
    """Execute the provided SQL statements using PostgreSQL cursor."""
    results = []

    for statement in sql_statements:
        try:
            cur.execute(statement)
            results.append({"statement": statement, "status": "SUCCESS"})
        except Exception as e:
            results.append({"statement": statement, "status": str(e)})

    return results
