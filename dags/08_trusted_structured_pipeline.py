from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow")

CLEAN_TRACKS_SCRIPT = os.path.join(
    PROJECT_DIR,
    "scripts",
    "trusted",
    "structured",
    "clean_tracks.py",
)

CLEAN_ISRC_SCRIPT = os.path.join(
    PROJECT_DIR,
    "scripts",
    "trusted",
    "structured",
    "clean_isrc.py",
)

CLEAN_RECCOBEATS_SCRIPT = os.path.join(
    PROJECT_DIR,
    "scripts",
    "trusted",
    "structured",
    "clean_reccobeats.py",
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
    "LANDING_BUCKET": os.getenv("LANDING_BUCKET", os.getenv("BRONZE_BUCKET", "landing")),
    "TRUSTED_BUCKET": os.getenv("TRUSTED_BUCKET", "trusted"),
    "SPARK_EXECUTOR_PYTHON": os.getenv("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12"),
}


@dag(
    dag_id="08_trusted_music",
    description="Clean structured Last.fm, MusicBrainz ISRC and ReccoBeats data into the Trusted Zone.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["trusted", "structured", "spark", "minio"],
)
def trusted_structured_cleaning_pipeline():
    clean_tracks_task = BashOperator(
        task_id="clean_tracks_to_trusted",
        bash_command=f"cd {PROJECT_DIR} && python {CLEAN_TRACKS_SCRIPT}",
        env=COMMON_ENV,
    )

    clean_isrc_task = BashOperator(
        task_id="clean_isrc_to_trusted",
        bash_command=f"cd {PROJECT_DIR} && python {CLEAN_ISRC_SCRIPT}",
        env=COMMON_ENV,
    )

    clean_reccobeats_task = BashOperator(
        task_id="clean_reccobeats_to_trusted",
        bash_command=f"cd {PROJECT_DIR} && python {CLEAN_RECCOBEATS_SCRIPT}",
        env=COMMON_ENV,
    )

    trigger_song_features_task = TriggerDagRunOperator(
        task_id="trigger_09_song_features",
        trigger_dag_id="09_song_features",
        trigger_run_id="trusted_music__{{ run_id }}",
        conf={"source_dag_run_id": "{{ run_id }}"},
        reset_dag_run=False,
        skip_when_already_exists=True,
    )

    clean_tracks_task >> clean_isrc_task >> clean_reccobeats_task >> trigger_song_features_task


trusted_structured_cleaning_pipeline()
