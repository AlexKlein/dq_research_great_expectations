# Research of Data Quality tools. Great Expectations

In this project I wanted to dive into the world of data quality with a deep exploration of the Great Expectations tool.

## Description

This repository consists of:

```
- a database with:
    - generic tables;
    - a table showcasing Great Expectations' results;
- a Great Expectations setup with:
    - data profiling functionalities;
    - data expectations validation;
- an Airflow setup: 
    - a DAG for creating DB objects;
    - a DAG for running Great Expectations data profiling;
    - a DAG for running Great Expectations validation;
- Docker files to containerize our setup for smooth deployment.
```

## Airflow
As the scheduler I choose Airflow v2. You can get local access there using Access it locally at [this link](http://localhost:8080/home/). For login credentials, refer to [entrypoint.sh](./project/entrypoint.sh).

## Build and Run

When you need to start the app with all infrastructure, you have to make this steps:
1. Modify the environment variables in [YML-file](./project/docker-compose.yml) and [entrypoint.sh](./project/entrypoint.sh) (now there are default values) 
2. Run the following: `docker-compose up -d --build` command. Give it some time. Your app, tables, and Airflow will soon be ready.

### Note

The database and Airflow webserver need a moment. Once up, proceed with the application.

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
