from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow")
TRUSTED_SEMISTRUCTURED_DIR = os.path.join(
    PROJECT_DIR,
    "scripts",
    "trusted",
    "semistructured",
)

COMMON_ENV = {
    **os.environ,
    "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.getenv(
        "MINIO_ACCESS_KEY",
        os.getenv("MINIO_ROOT_USER", "minioadmin"),
    ),
    "MINIO_SECRET_KEY": os.getenv(
        "MINIO_SECRET_KEY",
        os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    ),
    "BRONZE_BUCKET": os.getenv("BRONZE_BUCKET", "bronze"),
    "TRUSTED_BUCKET": os.getenv("TRUSTED_BUCKET", "trusted"),
    "BRONZE_TRENDS_PREFIX": os.getenv(
        "BRONZE_TRENDS_PREFIX",
        "persistent/semi_structured/trends/raw/",
    ),
    "TRUSTED_TRENDS_DELTA_URI": os.getenv(
        "TRUSTED_TRENDS_DELTA_URI",
        "s3://trusted/semi_structured/trends/delta/trends_clean_delta",
    ),
    "TRUSTED_TRENDS_REJECTED_PREFIX": os.getenv(
        "TRUSTED_TRENDS_REJECTED_PREFIX",
        "semi_structured/trends/rejected/",
    ),
    "TRUSTED_METADATA_PREFIX": os.getenv(
        "TRUSTED_METADATA_PREFIX",
        "metadata/semi_structured/trends/",
    ),
    "TRUSTED_TRENDS_MAX_FILES": os.getenv("TRUSTED_TRENDS_MAX_FILES", "0"),
    "TRUSTED_TRENDS_SPARK_PARTITIONS": os.getenv("TRUSTED_TRENDS_SPARK_PARTITIONS", "8"),
    "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
    "SPARK_DRIVER_PYTHON": os.getenv("SPARK_DRIVER_PYTHON", "/home/airflow/.local/bin/python"),
    "SPARK_EXECUTOR_PYTHON": os.getenv("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12"),
}


@dag(
    dag_id="trusted_trends_pipeline",
    description="Run a Spark micro-batch job to clean persisted semistructured trend JSONL into the Trusted Zone.",
    start_date=datetime(2025, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["trusted", "trends", "semi-structured", "spark", "minio"],
)
def trusted_trends_pipeline():
    clean_trends_task = BashOperator(
        task_id="spark_clean_trends_to_trusted",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"spark-submit "
            f"--master {COMMON_ENV['SPARK_MASTER_URL']} "
            f"--conf spark.pyspark.driver.python={COMMON_ENV['SPARK_DRIVER_PYTHON']} "
            f"--conf spark.pyspark.python={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"--conf spark.executorEnv.PYSPARK_PYTHON={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"{TRUSTED_SEMISTRUCTURED_DIR}/clean_trends_spark.py"
        ),
        env=COMMON_ENV,
    )

    trigger_exploitation_task = TriggerDagRunOperator(
        task_id="trigger_exploitation_trends_song_aggregates",
        trigger_dag_id="exploitation_trends_song_aggregates_pipeline",
        trigger_run_id="trusted_trends__{{ run_id }}",
        conf={"source_dag_run_id": "{{ run_id }}"},
        reset_dag_run=False,
        skip_when_already_exists=True,
    )

    clean_trends_task >> trigger_exploitation_task


trusted_trends_pipeline()
