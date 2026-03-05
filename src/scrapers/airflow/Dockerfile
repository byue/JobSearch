FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/jobsearch

# Install DAG runtime dependencies.
COPY src/scrapers/airflow/requirements.txt /opt/jobsearch/src/scrapers/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/jobsearch/src/scrapers/airflow/requirements.txt
