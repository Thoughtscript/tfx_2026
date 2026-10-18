import logging
import os
from datetime import datetime

import docker
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator


LOG = logging.getLogger(__name__)


def log_persisted_predictions():
    database_url = URL.create(
        "postgresql+psycopg2",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="postgres",
        port=5432,
        database=os.environ["POSTGRES_DB"],
    )
    engine = create_engine(database_url)
    try:
        with engine.connect() as connection:
            predictions = connection.execute(
                text(
                    """
                    SELECT record_id, feature, prediction
                    FROM beam_predictions
                    ORDER BY record_id
                    """
                )
            ).mappings().all()
    finally:
        engine.dispose()

    if not predictions:
        raise AirflowException("Beam completed without persisted predictions")
    LOG.info("Verified %d persisted prediction rows in Postgres", len(predictions))
    for prediction in predictions:
        LOG.info(
            "Persisted prediction: record_id=%s feature=%s prediction=%s",
            prediction["record_id"],
            prediction["feature"],
            prediction["prediction"],
        )


def run_beam_pipeline():
    context = get_current_context()
    dag_run = context["dag_run"]
    LOG.info(
        "Starting Beam submission for dag_id=%s run_id=%s task_id=%s",
        context["dag"].dag_id,
        dag_run.run_id,
        context["task"].task_id,
    )
    command = [
        "python",
        "-m",
        "src.submit",
        "--runner=FlinkRunner",
        "--flink_master=flink-jobmanager:8081",
        "--flink_version=2.2",
        "--environment_type=EXTERNAL",
        "--environment_config=localhost:50000",
    ]
    client = None
    try:
        client = docker.from_env()
        beam_container = client.containers.get("beam")
        LOG.info(
            "Executing Beam pipeline in container=%s command=%s",
            beam_container.name,
            " ".join(command),
        )
        execution = client.api.exec_create(
            beam_container.id,
            cmd=command,
            environment={
                "TF_SERVING_URL": os.environ.get(
                    "TF_SERVING_URL",
                    "http://tensorflow-serving:8501/v1/models/example:predict",
                ),
                "POSTGRES_HOST": "postgres",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": os.environ["POSTGRES_DB"],
                "POSTGRES_USER": os.environ["POSTGRES_USER"],
                "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
            },
            workdir="/app",
            stdout=True,
            stderr=True,
            tty=False,
        )
        exec_id = execution["Id"]
        LOG.info("Beam process started in container=%s exec_id=%s", beam_container.name, exec_id)
        for stdout, stderr in client.api.exec_start(
            exec_id, stream=True, demux=True
        ):
            if stdout:
                LOG.info("Beam stdout: %s", stdout.decode("utf-8", errors="replace").rstrip())
            if stderr:
                LOG.info("Beam stderr: %s", stderr.decode("utf-8", errors="replace").rstrip())
        exit_code = client.api.exec_inspect(exec_id)["ExitCode"]
    except docker.errors.DockerException as error:
        raise AirflowException(f"Could not execute the Beam container: {error}") from error
    finally:
        if client is not None:
            client.close()

    if exit_code is None:
        raise AirflowException("Beam process ended without a reported exit code")
    if exit_code != 0:
        raise AirflowException(
            f"Beam pipeline exited with status {exit_code}"
        )
    LOG.info("Beam submission process completed successfully (exit_code=0)")
    log_persisted_predictions()


with DAG(
    dag_id="beam_flink_tensorflow_postgres",
    description="Run a Beam Flink pipeline with TensorFlow Serving and Postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["beam", "flink", "tensorflow"],
) as dag:
    run_pipeline = PythonOperator(
        task_id="run_beam_pipeline",
        python_callable=run_beam_pipeline,
    )
