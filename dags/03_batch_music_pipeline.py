from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


# Base path inside the Airflow container
PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow")
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")

# -----------------------------
# Environment variables
# -----------------------------
COMMON_ENV = {
    **os.environ,

    # APIs
    "LASTFM_API_KEY": os.getenv("LASTFM_API_KEY", ""),
    "RECCOBEATS_API_KEY": os.getenv("RECCOBEATS_API_KEY", ""),
    "MUSICBRAINZ_CONTACT_EMAIL": os.getenv("MUSICBRAINZ_CONTACT_EMAIL", "team@example.com"),

    # MinIO (CRUCIAL)
    "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")),
    "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")),
    "MINIO_SECURE": os.getenv("MINIO_SECURE", "false"),
    "BRONZE_BUCKET": os.getenv("BRONZE_BUCKET", "bronze"),

    # Optional AWS compatibility
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
}


@dag(
    dag_id="structured_batch",
    description="Batch ingestion pipeline: Last.fm → MusicBrainz → ReccoBeats → merge",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["batch", "structured", "lakehouse", "delta"],
)
def structured_batch():

    # -----------------------------
    # 1. Last.fm ingestion
    # -----------------------------
    lastfm_task = BashOperator(
        task_id="extract_lastfm_raw",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python {SCRIPTS_DIR}/01_lastfm_batch.py"
        ),
        env=COMMON_ENV,
    )

    # -----------------------------
    # 2. MusicBrainz enrichment
    # -----------------------------
    musicbrainz_task = BashOperator(
        task_id="resolve_isrc_musicbrainz",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python {SCRIPTS_DIR}/02_musicbrainz_to_isrc.py"
        ),
        env=COMMON_ENV,
    )

    # -----------------------------
    # 3. ReccoBeats enrichment
    # -----------------------------
    reccobeats_task = BashOperator(
        task_id="fetch_reccobeats_features",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python {SCRIPTS_DIR}/03_fetch_reccobeats.py"
        ),
        env=COMMON_ENV,
    )

    # -----------------------------
    # Pipeline order
    # -----------------------------
    lastfm_task >> musicbrainz_task >> reccobeats_task


structured_batch()