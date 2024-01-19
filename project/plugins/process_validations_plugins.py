import os
from datetime import datetime

import pandas as pd
import psycopg2

VALIDATION_RESULTS_TABLE_NAME = os.environ.get("VALIDATION_RESULTS_TABLE_NAME")


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


def compress_numbers_to_ranges(nums):
    """Convert a list of numbers to a string representation of ranges."""
    if not nums:
        return ""

    nums.sort()

    ranges = []
    range_start = range_end = nums[0]

    for n in nums[1:]:
        if n == range_end + 1:
            range_end = n
        else:
            ranges.append(str(range_start) if range_start == range_end else f"{range_start}-{range_end}")
            range_start = range_end = n

    ranges.append(str(range_start) if range_start == range_end else f"{range_start}-{range_end}")

    return ", ".join(ranges)


def extract_value(d, *keys, default=None):
    """Extract value from a nested dictionary."""
    for key in keys:
        d = d.get(key, {})
    return d or default


def parse_gx_result(result):
    results = extract_value(result, 'run_results', next(iter(result.get('run_results', {}))), 'validation_result', 'results', default=[])
    parsed_data = []

    for res in results:
        unexpected_list = list(set(extract_value(res, 'result', 'partial_unexpected_list', default=[])))
        partial_unexpected_counts = str(extract_value(res, 'result', 'partial_unexpected_counts')).replace("'", '"')

        parsed_data.append({
            'status': "Success" if res.get('success', False) else "Failed",
            'check_type': extract_value(res, 'expectation_config', 'expectation_type'),
            'check_column': extract_value(res, 'expectation_config', 'kwargs', 'column'),
            'check_condition': extract_value(res, 'expectation_config', 'kwargs', 'value_set'),
            'element_count': extract_value(res, 'result', 'element_count'),
            'missing_count': extract_value(res, 'result', 'missing_count'),
            'missing_percent': extract_value(res, 'result', 'missing_percent'),
            'unexpected_count': extract_value(res, 'result', 'unexpected_count'),
            'unexpected_percent': extract_value(res, 'result', 'unexpected_percent'),
            'unexpected_percent_total': extract_value(res, 'result', 'unexpected_percent_total'),
            'unexpected_percent_nonmissing': extract_value(res, 'result', 'unexpected_percent_nonmissing'),
            'unexpected_list': unexpected_list,
            'partial_unexpected_counts': partial_unexpected_counts,
            'raised_exception': extract_value(res, 'exception_info', 'raised_exception'),
            'exception_message': extract_value(res, 'exception_info', 'exception_message'),
            'exception_traceback': extract_value(res, 'exception_info', 'exception_traceback')
        })

    return parsed_data


def format_value(value, key):
    """Format values for SQL insertion."""
    if value == "NaT" or value is pd.NaT or value is None:
        return 'NULL'

    if key == 'unexpected_list' and value:
        return f"'{', '.join(map(str, value))}'"

    if key == 'check_condition' and isinstance(value, list):
        return f"'{', '.join(map(str, value))}'"

    if isinstance(value, (int, float)):
        return str(value)

    return f"'{value}'"


def generate_sql_inserts(parsed_data, schema, table):
    """Generate SQL INSERT statements based on parsed data."""
    datetime_value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    create_table = f"""
    CREATE TABLE IF NOT EXISTS {VALIDATION_RESULTS_TABLE_NAME} (
        datetime_value                TIMESTAMP,
        schema_name                   TEXT, 
        table_name                    TEXT,
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
        partial_unexpected_counts     TEXT,
        raised_exception              TEXT,
        exception_message             TEXT,
        exception_traceback           TEXT
    );
    """

    sql_statements = [create_table]

    for record in parsed_data:
        formatted_values = [format_value(val, key) for key, val in record.items()]

        columns_from_dataframe = ', '.join(record.keys())
        all_columns = f'datetime_value, schema_name, table_name, {columns_from_dataframe}'

        values = ', '.join(formatted_values)
        all_values = f"CAST('{datetime_value}' AS TIMESTAMP), '{schema}', '{table}', {values}"

        insert_statement = f"INSERT INTO {VALIDATION_RESULTS_TABLE_NAME} ({all_columns}) VALUES ({all_values});"
        sql_statements.append(insert_statement)

    return sql_statements


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


def ingest_results_into_database(cur, validation_results, schema, table):
    """Ingest the results into the database."""
    parsed_results = parse_gx_result(validation_results)
    insert_statements = generate_sql_inserts(parsed_results, schema, table)

    execution_results = execute_sql_statements(cur, insert_statements)

    for result in execution_results:
        if result["status"] == "SUCCESS":
            print(f"Executed: {result['statement'][:100]}... SUCCESS")
        else:
            print(f"Failed: {result['statement'][:100]}... {result['status']}")
