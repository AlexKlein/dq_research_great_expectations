#!/bin/bash
set -e

# Initialize the database
airflow db migrate

# Create default connections
airflow connections create-default-connections

# Create an admin user
airflow users create \
     -r Admin \
     -u admin \
     -e admin@example.com \
     -f admin \
     -l user \
     -p admin

# Execute the command passed from docker-compose
exec airflow "$@"
