from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow")

SONG_EMBEDDINGS_SCRIPT = os.path.join(
    PROJECT_DIR,
    "scripts",
    "exploitation",
    "recommender",
    "song_embeddings_milvus.py",
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
    "EXPLOITATION_BUCKET": os.getenv("EXPLOITATION_BUCKET", "exploitation"),
    "EXPLOITATION_RECOMMENDER_FEATURES_DELTA_URI": os.getenv(
        "EXPLOITATION_RECOMMENDER_FEATURES_DELTA_URI",
        "s3://exploitation/recommender/song_features/delta/song_recommender_features_delta",
    ),
    "EXPLOITATION_SONG_EMBEDDINGS_DELTA_URI": os.getenv(
        "EXPLOITATION_SONG_EMBEDDINGS_DELTA_URI",
        "s3://exploitation/recommender/song_embeddings/delta/song_embedding_snapshots_delta",
    ),
    "EXPLOITATION_SONG_EMBEDDINGS_METADATA_PREFIX": os.getenv(
        "EXPLOITATION_SONG_EMBEDDINGS_METADATA_PREFIX",
        "metadata/recommender/song_embeddings/",
    ),
    "MILVUS_HOST": os.getenv("MILVUS_HOST", "milvus-standalone"),
    "MILVUS_PORT": os.getenv("MILVUS_PORT", "19530"),
    "MILVUS_SONG_COLLECTION": os.getenv("MILVUS_SONG_COLLECTION", "song_recommender_embeddings"),
    "SONG_EMBEDDING_MODEL": os.getenv("SONG_EMBEDDING_MODEL", "openai/clip-vit-base-patch32"),
    "SONG_EMBEDDING_DIM": os.getenv("SONG_EMBEDDING_DIM", "512"),
    "SONG_EMBEDDING_BATCH_SIZE": os.getenv("SONG_EMBEDDING_BATCH_SIZE", "16"),
    "SONG_EMBEDDING_USE_SPARK": os.getenv("SONG_EMBEDDING_USE_SPARK", "true"),
    "SONG_EMBEDDING_SPARK_PARTITIONS": os.getenv("SONG_EMBEDDING_SPARK_PARTITIONS", "1"),
    "SONG_EMBEDDING_FORCE_REFRESH": os.getenv("SONG_EMBEDDING_FORCE_REFRESH", "false"),
    "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
    "SPARK_DRIVER_PYTHON": os.getenv("SPARK_DRIVER_PYTHON", "/home/airflow/.local/bin/python"),
    "SPARK_EXECUTOR_PYTHON": os.getenv("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12"),
    "HF_HOME": os.getenv("HF_HOME", "/tmp/huggingface"),
}


@dag(
    dag_id="11_song_index",
    description="Daily hash-based CLIP text embedding refresh for song recommender retrieval.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["exploitation", "recommendations", "embeddings", "milvus"],
)
def song_embeddings_to_milvus_pipeline():
    generate_song_embeddings_task = BashOperator(
        task_id="generate_song_embeddings_to_milvus",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"spark-submit "
            f"--master {COMMON_ENV['SPARK_MASTER_URL']} "
            f"--conf spark.pyspark.driver.python={COMMON_ENV['SPARK_DRIVER_PYTHON']} "
            f"--conf spark.pyspark.python={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"--conf spark.executorEnv.PYSPARK_PYTHON={COMMON_ENV['SPARK_EXECUTOR_PYTHON']} "
            f"{SONG_EMBEDDINGS_SCRIPT}"
        ),
        env=COMMON_ENV,
    )

    generate_song_embeddings_task


song_embeddings_to_milvus_pipeline()
