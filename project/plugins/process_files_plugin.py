import json
import os

import yaml


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


def load_comparison_config(root_dir="comparisons"):
    """Load comparison configuration from a given file dir."""
    os.chdir('/opt/airflow/great_expectations/')
    with open(os.path.join(root_dir, "comparison_config.yml"), 'r') as stream:
        config = yaml.safe_load(stream)

    list_of_tables = []

    for instance in config:
        instance_id = instance.get('instance_id', '')

        for storage in instance.get('storages', []):
            storage_type = storage.get('type', {})
            source = storage_type.get('source', '')
            target = storage_type.get('target', '')

            compared_pairs = storage.get('compared_pairs', [])
            for pair in compared_pairs:
                source_schema = pair.get('source_schema', '')
                source_table = pair.get('source_table', '')
                source_custom_filter = pair.get('source_custom_filter', '')
                target_schema = pair.get('target_schema', '')
                target_table = pair.get('target_table', '')
                target_custom_filter = pair.get('target_custom_filter', '')
                list_of_tables.append(
                    (instance_id, source_schema, source_table, source_custom_filter,
                     target_schema, target_table, target_custom_filter, source, target)
                )

    return list_of_tables
