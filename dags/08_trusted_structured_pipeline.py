from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow")

CLEAN_TRACKS_SCRIPT = os.path.join(
    PROJECT_DIR,
    "scripts",
    "trusted",
    "structured",
    "clean_tracks.py",
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
    "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
    "SPARK_DRIVER_PYTHON": os.getenv(
        "SPARK_DRIVER_PYTHON",
        "/home/airflow/.local/bin/python",
    ),
    "SPARK_EXECUTOR_PYTHON": os.getenv("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12"),
}


@dag(
    dag_id="trusted_clean_tracks_pipeline",
    description="Run Spark job to clean structured tracks into the Trusted Zone.",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual execution only
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["trusted", "structured", "tracks", "spark", "minio"],
)
def trusted_clean_tracks_pipeline():
    clean_tracks_task = BashOperator(
        task_id="spark_clean_tracks_to_trusted",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"spark-submit "
            f"--master {COMMON_ENV['SPARK_MASTER_URL']} "
            f"--conf spark.pyspark.driver.python={COMMON_ENV['SPARK_DRIVER_PYTHON']} "
            f"--conf spark.pyspark.python={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"--conf spark.executorEnv.PYSPARK_PYTHON={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"--packages org.apache.hadoop:hadoop-aws:3.4.1 "
            f"{CLEAN_TRACKS_SCRIPT}"
        ),
        env=COMMON_ENV,
    )

    clean_tracks_task


trusted_clean_tracks_pipeline()