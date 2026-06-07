from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


# Base path inside the Airflow container
PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow")
LANDING_SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts", "landing")

# Subdirectories by data type
LANDING_STRUCTURED_DIR = os.path.join(LANDING_SCRIPTS_DIR, "structured")
LANDING_SEMISTRUCTURED_DIR = os.path.join(LANDING_SCRIPTS_DIR, "semistructured")
LANDING_UNSTRUCTURED_DIR = os.path.join(LANDING_SCRIPTS_DIR, "unstructured")

# Environment variables
COMMON_ENV = {
    **os.environ,
    # APIs
    "LASTFM_API_KEY": os.getenv("LASTFM_API_KEY", ""),
    "RECCOBEATS_API_KEY": os.getenv("RECCOBEATS_API_KEY", ""),
    "MUSICBRAINZ_CONTACT_EMAIL": os.getenv("MUSICBRAINZ_CONTACT_EMAIL", "team@example.com"),
    # MinIO
    "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")),
    "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")),
    "MINIO_SECURE": os.getenv("MINIO_SECURE", "false"),
    "BRONZE_BUCKET": os.getenv("BRONZE_BUCKET", "bronze"),
    # Optional AWS compatibility
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
}


@dag(
    dag_id="03_raw_music",
    description="Batch ingestion pipeline: Last.fm -> MusicBrainz -> ReccoBeats",
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *",  # daily
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["batch", "structured", "landing", "lakehouse", "delta"],
)
def structured_batch():
    lastfm_task = BashOperator(
        task_id="extract_lastfm_raw",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python {LANDING_STRUCTURED_DIR}/lastfm_batch.py" 
        ),
        env=COMMON_ENV,
    )

    musicbrainz_task = BashOperator(
        task_id="resolve_isrc_musicbrainz",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python {LANDING_STRUCTURED_DIR}/musicbrainz_to_isrc.py" 
        ),
        env=COMMON_ENV,
    )

    reccobeats_task = BashOperator(
        task_id="fetch_reccobeats_features",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python {LANDING_STRUCTURED_DIR}/fetch_reccobeats.py" 
        ),
        env=COMMON_ENV,
    )

    trigger_trusted_music_task = TriggerDagRunOperator(
        task_id="trigger_08_trusted_music",
        trigger_dag_id="08_trusted_music",
        trigger_run_id="raw_music__{{ run_id }}",
        conf={"source_dag_run_id": "{{ run_id }}"},
        reset_dag_run=False,
        skip_when_already_exists=True,
    )

    lastfm_task >> musicbrainz_task >> reccobeats_task >> trigger_trusted_music_task


structured_batch()
