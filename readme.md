# Research of Data Quality tools. Great Expectations

In this project I wanted to dive into the world of data quality with a deep exploration of the Great Expectations tool.

## Airflow

As the scheduler I choose Airflow v2. You can get local access there using Access it locally at [this link](http://localhost:8080/home/). For login credentials, refer to [entrypoint.sh](./project/entrypoint.sh).

## Build and Run

When you need to start the app with all infrastructure, you have to make this steps:
1. Modify the environment variables in [env-file](./project/local.env) and [entrypoint.sh](./project/entrypoint.sh) (now there are default values) 
2. Correct your credential files in the [YML-file](./project/docker-compose.yml).
3. Run the following: `docker-compose up -d --build` command. Give it some time. Your app, tables, and Airflow will soon be ready.

## Data Processing

This document provides an overview of the data processing workflows.

### Data Generating (`db_generation`)

This DAG is responsible for data generating in `MY_SMALL_DWH` database. Should be launched as the first step if you use current build.

- **DAG ID:** `db_generation`
- **Schedule:** Only manual trigger.


### Data Profiling (`gx_data_profiler`)

This DAG is responsible for profiling datasets and generating expectations using the Great Expectations library.

- **DAG ID:** `gx_data_profiler`
- **Schedule:** Only manual trigger.

#### Key Components

1. **GXDataProfilerOperator**: This is a custom [operator](./project/plugins/gx_data_profiler_operator.py) which triggers the data profiling process. 
2. **Utility Functions**: The utility [functions](./project/plugins/run_data_profiler_plugin.py) serve various purposes like loading the profiling configuration, ensuring directories exist, and saving the generated expectation suite to a file.
3. **Database Functions**: These [functions](./project/plugins/run_data_profiler_plugin.py) are designed to interact with PostgreSQL database. They can initialize a PostgreSQL connection, fetch the latest partition for a table, and retrieve data to be profiled.
4. **Great Expectations Functions**: These [functions](./project/plugins/run_data_profiler_plugin.py) utilize the Great Expectations library to set up data sources, create new expectation suites, and generate expectations using data assistants.
5. **Main Processing Functions**: [They](./project/plugins/run_data_profiler_plugin.py) orchestrate the profiling workflow for individual tables or entire datasets.

The profiling process involves:
- Fetching data from the database.
- Generating expectations using the Great Expectations library.
- Saving the expectations in a structured format.

#### Output Files and Conditions

After running the DAG, the resulting profile files will be located at `./project/profilers/stored_profiles/<schema_name>/<table_name>.json`. The root directory for the profiles can be adjusted by modifying the [local.env](./project/local.env) file. If it is need then it is possible to adjust the code and make additional loop and profile tables from different databases.

The data profiling is subject to the following conditions:

- Tables without sufficient permissions will be skipped.
- If a profile already exists for a table, the table will be skipped to avoid extra costs. To re-profile a table, the existing profile file must be manually deleted.
- Non-existent tables will be skipped.
  
Tables are specified in the [profiling_config.yml](./project/profilers/profiling_config.yml) file, where the list of tables can be edited or extended by users. Profiling can be tailored using masks to include datasets or tables as needed. Also, it is possible to exclude unnecessary table columns in the config. Here's an example of how the `profiling_config.yml` is structured to define profiling rules:

This flexible configuration allows for robust and selective data profiling across various databases and schemas in PostgreSQL.

### Data Validating and Result Ingesting (`gx_run_expectation`)

This DAG is responsible for validating data using Great Expectations, and after validations, it pushes the results to PostgreSQL.

- **DAG ID:** `gx_run_expectation`
- **Schedule:** On Monday at 8:30 AM.

#### Key Components

1. **GXRunExpectationsOperator**: A custom [operator](./project/plugins/gx_run_expectations_operator.py) that launches checks and ingests results into PostgreSQL.
2. **File Handling Functions**: Utility [functions](./project/plugins/process_files_plugin.py) for:
   - Loading custom expectations.
   - Parsing directories for datasets and expectation files.
3. **Ingestion to PostgreSQL**: The [plugin](./project/plugins/process_validations_plugins.py) provides functions to:
   - Parse validation results.
   - Generate SQL inserts.
   - Push the results to the database.
4. **Expectation Processing & Validation**: The [plugin](./project/plugins/run_expectations_plugin.py) streamlines:
   - Data source setup for Great Expectations with PostgreSQL integration.
   - Suite creation or retrieval, and expectation updates.
   - SQL query generation with customization.
   - Conversion of SQL queries to batch requests.
   - Checkpoint creation and validations running.

#### Prerequisites for Data Validation

Before running the `gx_run_expectation` DAG for data validation, ensure you have the following files prepared:
**Expectation JSON Files:** The validator uses pre-defined expectation configurations. You have to create these expectations in the form of `.json` files and place them in the directory structured as `./project/great_expectations/expectations/<schema_name>/<table_name>/<expectation_file_name>.json`. While the `expectation_file_name` can be arbitrary, it's recommended to use the table name with an appropriate prefix or suffix for ease of navigation since there can be multiple expectation files for each table.
For a streamlined setup process, consider utilizing the data profiler outlined in [Data Profiling (`gx_data_profiler`)](#data-profiling-gx_data_profiler). This will generate a baseline expectations file which can then be manually transferred to the appropriate directory, reviewed, and edited if necessary.

#### Custom Query and Filter Handling

The validation process is flexible, allowing for either standard queries as or custom ones defined by the user. If a `custom_query` is specified within the expectation file, it will be used as is. This allows for almost any SQL query that the user wishes to run against the dataset.
In cases where a `custom_query` is not provided, a default query is constructed to `select all` from the respective PostgreSQL table, with an optional `custom_filter` applied if specified. This filter can be used to narrow down the data for validation (e.g., to specific countries). The resulting query will include a `LIMIT` clause to cap the number of rows processed, which is controlled by the `LIMIT_OF_VALIDATED_ROWS` setting in [local.env](./project/local.env). This limit is crucial for managing memory consumption when the data is loaded into a pandas DataFrame.
Please note that the `custom_query` and `custom_filter` in the example file are prefixed with a `!`, indicating that these fields should be uncommented (remove the `!`) to activate them. If left as is, they serve as placeholders or comments within the JSON structure, since JSON does not support standard comment lines.

#### Result Ingestion to PostgreSQL

Upon completing the validation, the results are automatically inserted into a PostgreSQL table, as specified in the [local.env](./project/local.env) file.

## List of tables

```sql
-- Marine Data
select * from marine.dim_cities;
select * from marine.dim_ship_types;
select * from marine.dim_companies;
select * from marine.fact_ship_travels;

-- Financial Data
select * from financial.dim_banks;
select * from financial.dim_currencies;
select * from financial.dim_clients;
select * from financial.dim_cargo_types;
select * from financial.fact_bank_transactions;
select * from financial.fact_cargo_amounts;

-- Data Quality Results
select * from data_quality.gx_validation_results;
```

## Manual setup

1. If Python isn't already installed, get it [here](https://www.python.org/downloads/).
2. Configure your environment:
```bash
mkdir dq_great_expectations
cd dq_great_expectations

python --version
pip install virtualenv
virtualenv venv
.\ge_venv\Scripts\activate | source ge_venv/bin/activate


pip install great_expectations psycopg2 sqlalchemy jupyterlab
great_expectations init
great_expectations datasource new
great_expectations suite new
great_expectations docs build
```
3. Further Reading
   1. Craft your own tutorial with [this guide](https://www.digitalocean.com/community/tutorials/how-to-test-your-data-with-great-expectations). 
   2. Check out [Great Expectations' official documentation](https://greatexpectations.io/).

## Troubleshooting

### standard_init_linux.go:228

If you encounter the error message **standard_init_linux.go:228: exec user process caused: no such file or directory** when running your Docker container, it may be due to an issue with the encoding of your [entrypoint.sh](./project/entrypoint.sh) file.

To resolve this issue, follow these steps:

1. **Check File Encoding:** Ensure that the [entrypoint.sh](./project/entrypoint.sh) file is encoded correctly. It should be in UTF-8.
2. **Correct Encoding Issues:** If you suspect encoding issues, you can use a text editor that allows you to change the file's encoding to UTF-8. Save the file with UTF-8 encoding, and then try running your Docker container again.
3. **Line Endings:** Verify that the line endings (Unix vs. Windows) in your [entrypoint.sh](./project/entrypoint.sh) file are appropriate for your platform. Use a text editor that allows you to convert line endings if necessary.
By following these steps, you can resolve the "no such file or directory" error related to the [entrypoint.sh](./project/entrypoint.sh) file encoding.
