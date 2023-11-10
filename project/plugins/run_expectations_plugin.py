import os
import yaml

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest


# Configuration constants
ONLY_RETURN_FAILURES = os.environ.get("ONLY_RETURN_FAILURES")
LIMIT_OF_VALIDATED_ROWS = os.environ.get("LIMIT_OF_VALIDATED_ROWS")

MY_SMALL_DWH_SQL_ALCHEMY_CONN = os.environ.get("MY_SMALL_DWH_SQL_ALCHEMY_CONN")


def setup_datasource(context):
    """Setup the Great Expectations datasource."""
    datasource_config = {
        "name": "expectations_launcher_datasource",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": MY_SMALL_DWH_SQL_ALCHEMY_CONN,
        },
        "data_connectors": {
            "expectations_launcher_data_connector": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["default_identifier_name"],
            },
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))
    context.add_datasource(**datasource_config)


def get_or_create_expectation_suite(context, suite_name):
    """Retrieve an existing expectation suite from the context or create a new one if it doesn't exist."""
    return context.add_or_update_expectation_suite(suite_name)


def update_suite_expectations(suite, expectations):
    """Update an expectation suite with a given list of expectations."""
    suite.expectations = [ExpectationConfiguration(**expectation) for expectation in expectations]


def generate_sql_query(schema, table, custom_query=None, custom_filter=None):
    """Generate an SQL query given certain parameters."""
    if custom_query:
        sql_query = custom_query
    else:
        sql_query = f"SELECT * FROM {schema}.{table} AS d"
        if custom_filter:
            sql_query += f" WHERE {custom_filter}"
        sql_query += f" LIMIT {LIMIT_OF_VALIDATED_ROWS}"

    return sql_query


def create_batch_request(schema, table, custom_query=None, custom_filter=None):
    """Convert an SQL query to a batch request."""
    sql_query = generate_sql_query(schema, table, custom_query, custom_filter)

    return RuntimeBatchRequest(
        datasource_name="expectations_launcher_datasource",
        data_connector_name="expectations_launcher_data_connector",
        data_asset_name=f"{schema}.{table}",
        runtime_parameters={"query": sql_query},
        batch_identifiers={"default_identifier_name": "expectations_launcher_identifier"},
        batch_spec_passthrough={"create_temp_table": False}
    )


def create_and_run_checkpoint(context, batch_request, suite_name):
    """Create a checkpoint in Great Expectations and then run it to validate the data."""
    context.add_or_update_checkpoint(
        name="expectations_launcher_checkpoint",
        config_version=1.0,
        class_name="Checkpoint",
        module_name="great_expectations.checkpoint",
        expectation_suite_name=suite_name,
        run_name_template="%Y%m%d-%H%M%S-expectations_launcher",
        action_list=[],
    )

    return context.run_checkpoint(
        checkpoint_name="expectations_launcher_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
                "result_format": "COMPLETE",
                "only_return_failures": ONLY_RETURN_FAILURES,
            },
        ],
    )
