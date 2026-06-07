from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow")
TRUSTED_UNSTRUCTURED_DIR = os.path.join(
    PROJECT_DIR,
    "scripts",
    "trusted",
    "unstructured",
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
    "BRONZE_IMAGES_PREFIX": os.getenv(
        "BRONZE_IMAGES_PREFIX",
        "persistent/unstructured/images/raw/",
    ),
    "TRUSTED_IMAGES_PREFIX": os.getenv(
        "TRUSTED_IMAGES_PREFIX",
        "unstructured/images/clean/",
    ),
    "TRUSTED_REJECTED_PREFIX": os.getenv(
        "TRUSTED_REJECTED_PREFIX",
        "unstructured/images/rejected/",
    ),
    "TRUSTED_METADATA_PREFIX": os.getenv(
        "TRUSTED_METADATA_PREFIX",
        "metadata/unstructured/images/",
    ),
    "TRUSTED_IMAGE_RESIZE_MAX_DIM": os.getenv("TRUSTED_IMAGE_RESIZE_MAX_DIM", "512"),
    "TRUSTED_IMAGE_JPEG_QUALITY": os.getenv("TRUSTED_IMAGE_JPEG_QUALITY", "85"),
    "TRUSTED_IMAGE_MAX_IMAGES": os.getenv("TRUSTED_IMAGE_MAX_IMAGES", "0"),
    "TRUSTED_IMAGE_SKIP_EXISTING": os.getenv("TRUSTED_IMAGE_SKIP_EXISTING", "true"),
    "TRUSTED_IMAGE_SPARK_PARTITIONS": os.getenv("TRUSTED_IMAGE_SPARK_PARTITIONS", "8"),
    "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
    "SPARK_DRIVER_PYTHON": os.getenv("SPARK_DRIVER_PYTHON", "/home/airflow/.local/bin/python"),
    "SPARK_EXECUTOR_PYTHON": os.getenv("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12"),
}


@dag(
    dag_id="04_trusted_images",
    description="Run a Spark micro-batch job to clean persisted landing images into the Trusted Zone.",
    start_date=datetime(2025, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["trusted", "images", "unstructured", "minio"],
)
def trusted_images_pipeline():
    clean_images_task = BashOperator(
        task_id="spark_clean_images_to_trusted",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"spark-submit "
            f"--master {COMMON_ENV['SPARK_MASTER_URL']} "
            f"--conf spark.pyspark.driver.python={COMMON_ENV['SPARK_DRIVER_PYTHON']} "
            f"--conf spark.pyspark.python={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"--conf spark.executorEnv.PYSPARK_PYTHON={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"{TRUSTED_UNSTRUCTURED_DIR}/clean_images_spark.py"
        ),
        env=COMMON_ENV,
    )

    trigger_image_embeddings_task = TriggerDagRunOperator(
        task_id="trigger_05_image_embeddings",
        trigger_dag_id="05_image_embeddings",
        trigger_run_id="trusted_images__{{ run_id }}",
        conf={"source_dag_run_id": "{{ run_id }}"},
        reset_dag_run=False,
        skip_when_already_exists=True,
    )

    clean_images_task >> trigger_image_embeddings_task


trusted_images_pipeline()
