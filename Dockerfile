# Airflow image with the pipeline's dependencies.
# The project itself is NOT copied: docker-compose mounts the repository at /opt/airflow/project,
# so editing code or DAGs does not require rebuilding the image.
FROM apache/airflow:3.1.8-python3.12

ARG AIRFLOW_VERSION=3.1.8
ARG PYTHON_VERSION=3.12

COPY --chown=airflow:root requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt