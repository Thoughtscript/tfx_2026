FROM apache/airflow:2.9.3-python3.10

USER airflow
RUN pip install --no-cache-dir docker==7.1.0

COPY --chown=airflow:0 services/airflow/init.sh /opt/airflow/init.sh
COPY --chown=airflow:0 services/airflow/dags/ /opt/airflow/dags/
