import json
import os


def load_custom_expectations(file_path):
    """Load custom expectations from a given file."""
    os.chdir('/opt/airflow/great_expectations/')
    with open(file_path, 'r') as f:
        return json.load(f)


def get_schema_table_and_expectation_paths(root_dir="expectations"):
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
