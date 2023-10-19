# Use Apache Airflow as the base image
FROM apache/airflow:2.7.1

# Developer and maintainer of the project
LABEL maintainer="Aleksandr Klein"

# Set environment varialbes
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Switch to ROOT user for installing mandatory packages
USER root

# Install mandatory packages
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        vim \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && apt-get install -y libpq-dev gcc \
 && rm -rf /var/lib/apt/lists/*

# Copy your great_expectations directory to the container
COPY great_expectations /opt/airflow/great_expectations

# Grant everyone permissions on GX folder
RUN chmod 777 /opt/airflow/great_expectations

# Switch back to the default Airflow user
USER airflow

# Install needed Python packages
RUN pip install --upgrade pip \
 && pip install great_expectations psycopg2 sqlalchemy

# Copy your dags folder to the container
COPY dags /opt/airflow/dags

# Copy entrypoint script to the container
COPY entrypoint.sh /entrypoint.sh

# Run the ini script
ENTRYPOINT ["/entrypoint.sh"]
