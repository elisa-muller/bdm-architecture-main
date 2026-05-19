from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow")
TRUSTED_RECOMMENDER_DIR = os.path.join(PROJECT_DIR, "scripts", "trusted", "recommender")

COMMON_ENV = {
    **os.environ,
    "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")),
    "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")),
    "BRONZE_BUCKET": os.getenv("BRONZE_BUCKET", "bronze"),
    "TRUSTED_BUCKET": os.getenv("TRUSTED_BUCKET", "trusted"),
    "BRONZE_RECOMMENDATION_FEEDBACK_PREFIX": os.getenv(
        "BRONZE_RECOMMENDATION_FEEDBACK_PREFIX",
        "persistent/recommender/feedback/raw/",
    ),
    "TRUSTED_RECOMMENDATION_FEEDBACK_DELTA_URI": os.getenv(
        "TRUSTED_RECOMMENDATION_FEEDBACK_DELTA_URI",
        "s3://trusted/recommender/feedback/delta/recommendation_feedback_clean_delta",
    ),
    "TRUSTED_RECOMMENDATION_FEEDBACK_REJECTED_PREFIX": os.getenv(
        "TRUSTED_RECOMMENDATION_FEEDBACK_REJECTED_PREFIX",
        "recommender/feedback/rejected/",
    ),
    "TRUSTED_RECOMMENDATION_FEEDBACK_METADATA_PREFIX": os.getenv(
        "TRUSTED_RECOMMENDATION_FEEDBACK_METADATA_PREFIX",
        "metadata/recommender/feedback/",
    ),
    "TRUSTED_RECOMMENDATION_FEEDBACK_MAX_FILES": os.getenv(
        "TRUSTED_RECOMMENDATION_FEEDBACK_MAX_FILES",
        "0",
    ),
    "TRUSTED_RECOMMENDATION_FEEDBACK_SPARK_PARTITIONS": os.getenv(
        "TRUSTED_RECOMMENDATION_FEEDBACK_SPARK_PARTITIONS",
        "4",
    ),
    "SPARK_MASTER_URL": os.getenv("RECOMMENDATION_FEEDBACK_SPARK_MASTER_URL", "local[*]"),
    "SPARK_DRIVER_PYTHON": os.getenv(
        "RECOMMENDATION_FEEDBACK_SPARK_DRIVER_PYTHON",
        "/home/airflow/.local/bin/python",
    ),
    "SPARK_EXECUTOR_PYTHON": os.getenv(
        "RECOMMENDATION_FEEDBACK_SPARK_EXECUTOR_PYTHON",
        "/home/airflow/.local/bin/python",
    ),
}


@dag(
    dag_id="trusted_recommendation_feedback_pipeline",
    description="Clean raw recommendation feedback events into the Trusted Zone.",
    start_date=datetime(2025, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["trusted", "recommendations", "feedback", "spark"],
)
def trusted_recommendation_feedback_pipeline():
    clean_feedback_task = BashOperator(
        task_id="spark_clean_recommendation_feedback_to_trusted",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"spark-submit "
            f"--master {COMMON_ENV['SPARK_MASTER_URL']} "
            f"--conf spark.pyspark.driver.python={COMMON_ENV['SPARK_DRIVER_PYTHON']} "
            f"--conf spark.pyspark.python={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"--conf spark.executorEnv.PYSPARK_PYTHON={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"{TRUSTED_RECOMMENDER_DIR}/clean_recommendation_feedback_spark.py"
        ),
        env=COMMON_ENV,
    )

    trigger_exploitation_task = TriggerDagRunOperator(
        task_id="trigger_exploitation_recommendation_feedback",
        trigger_dag_id="exploitation_recommendation_feedback_pipeline",
        trigger_run_id="trusted_feedback__{{ run_id }}",
        conf={"source_dag_run_id": "{{ run_id }}"},
        reset_dag_run=False,
        skip_when_already_exists=True,
    )

    clean_feedback_task >> trigger_exploitation_task


trusted_recommendation_feedback_pipeline()
