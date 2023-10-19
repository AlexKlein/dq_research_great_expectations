# Research of Data Quality tools. Great Expectations

In this project I wanted to explore features of Great Expectations tool.

## Description

This repository consists of:

```
- a database with:
    - a generic tables;
    - a table with Great Expectations results;
- a Great Expectations setup with:
    - data profiling;
    - checking expectations;
- an Airflow setup with a DAG for running Great Expectations;
- Docker files for wrapping this tool.
```

## Airflow
As the scheduler I choose Airflow v2. You can get local access there using http://localhost:8080/home/ (credentials you can find in `entrypoint.sh`).

## Build

When you need to start the app with all infrastructure, you have to make this steps:
1. Change environment variables in [YML-file](./project/docker-compose.yml) and [entrypoint.sh](./project/entrypoint.sh) (now there are default values) 
2. Execute the `docker-compose up -d --build` command - in this step the app will be built, tables will be created and Airflow app will be ready in some time.

### Note

You should wait a couple of minutes for the database and Airflow webserver start. After that you may run the application and check logs as the next step.
