from __future__ import annotations

import io
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IMAGES_RAW = os.getenv("TOPIC_IMAGES_RAW", "music-images-raw")
TOPIC_TRENDS_RAW = os.getenv("TOPIC_TRENDS_RAW", "music-trends-raw")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
TRUSTED_BUCKET = os.getenv("TRUSTED_BUCKET", "trusted")
EXPLOITATION_BUCKET = os.getenv("EXPLOITATION_BUCKET", "exploitation")


@dag(
    dag_id="init_platform",
    description="Initialize platform infrastructure: zone buckets, Kafka topics, and folder structure.",
    schedule=None,
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["init", "kafka", "minio", "bronze", "trusted", "exploitation"],
)
def init_platform():

    @task()
    def create_zone_buckets() -> list[str]:
        from minio import Minio

        endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        secure = MINIO_ENDPOINT.startswith("https://")

        client = Minio(
            endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=secure,
        )

        buckets = [BRONZE_BUCKET, TRUSTED_BUCKET, EXPLOITATION_BUCKET]
        created_or_existing = []

        for bucket_name in buckets:
            if client.bucket_exists(bucket_name):
                print(f"Bucket '{bucket_name}' already exists.")
            else:
                client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' created.")
            created_or_existing.append(bucket_name)

        return created_or_existing

    @task()
    def create_bronze_layout(bucket_names: list[str]) -> list[str]:
        from minio import Minio
        from minio.error import S3Error

        endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        secure = MINIO_ENDPOINT.startswith("https://")

        client = Minio(
            endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=secure,
        )

        prefixes = [
            # Landing / Bronze
            f"{BRONZE_BUCKET}:temporal/structured/lastfm/raw/",
            f"{BRONZE_BUCKET}:temporal/structured/musicbrainz/raw/",
            f"{BRONZE_BUCKET}:temporal/structured/reccobeats/raw/",
            f"{BRONZE_BUCKET}:temporal/semi_structured/trends/raw/",
            f"{BRONZE_BUCKET}:persistent/structured/lastfm/delta/",
            f"{BRONZE_BUCKET}:persistent/structured/musicbrainz/delta/",
            f"{BRONZE_BUCKET}:persistent/structured/reccobeats/delta/",
            f"{BRONZE_BUCKET}:persistent/semi_structured/trends/raw/",
            f"{BRONZE_BUCKET}:persistent/semi_structured/trends/delta/",

            # Trusted
            f"{TRUSTED_BUCKET}:structured/lastfm/delta/",
            f"{TRUSTED_BUCKET}:structured/musicbrainz/delta/",
            f"{TRUSTED_BUCKET}:structured/reccobeats/delta/",
            f"{TRUSTED_BUCKET}:semi_structured/trends/delta/",
            f"{TRUSTED_BUCKET}:semi_structured/trends/rejected/",
            f"{TRUSTED_BUCKET}:unstructured/images/clean/",
            f"{TRUSTED_BUCKET}:metadata/semi_structured/trends/",

            # Exploitation
            f"{EXPLOITATION_BUCKET}:structured/music_analytics/delta/",
            f"{EXPLOITATION_BUCKET}:semi_structured/trends_analytics/delta/",
            f"{EXPLOITATION_BUCKET}:recommender/song_features/delta/",
            f"{EXPLOITATION_BUCKET}:recommender/song_embeddings/delta/",
            f"{EXPLOITATION_BUCKET}:unstructured/images/embeddings/",
            f"{EXPLOITATION_BUCKET}:consumption/recommendations/",
            f"{EXPLOITATION_BUCKET}:consumption/recommendations/image_context_events/",
            f"{EXPLOITATION_BUCKET}:metadata/recommender/song_features/",
            f"{EXPLOITATION_BUCKET}:metadata/recommender/song_embeddings/",
            f"{EXPLOITATION_BUCKET}:metadata/",
        ]

        created = []

        for item in prefixes:
            bucket_name, prefix = item.split(":", 1)
            object_name = f"{prefix}.keep"

            try:
                client.stat_object(bucket_name, object_name)
                print(f"Prefix already initialized: s3://{bucket_name}/{object_name}")
            except S3Error as e:
                if e.code in {"NoSuchKey", "NoSuchObject", "NoSuchResource"}:
                    data = io.BytesIO(b"")
                    client.put_object(
                        bucket_name=bucket_name,
                        object_name=object_name,
                        data=data,
                        length=0,
                        content_type="text/plain",
                    )
                    print(f"Created prefix placeholder: s3://{bucket_name}/{object_name}")
                else:
                    raise

            created.append(f"s3://{bucket_name}/{object_name}")

        return created

    @task()
    def create_kafka_topics() -> list[str]:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

        topics_to_create = [
            NewTopic(name=TOPIC_IMAGES_RAW, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC_TRENDS_RAW, num_partitions=1, replication_factor=1),
        ]

        created_or_existing = []
        existing_topics = set(admin.list_topics())

        for topic in topics_to_create:
            if topic.name in existing_topics:
                print(f"Topic '{topic.name}' already exists.")
                created_or_existing.append(topic.name)
                continue

            try:
                admin.create_topics(new_topics=[topic], validate_only=False)
                print(f"Topic '{topic.name}' created.")
                created_or_existing.append(topic.name)
            except TopicAlreadyExistsError:
                print(f"Topic '{topic.name}' already exists.")
                created_or_existing.append(topic.name)

        admin.close()
        return created_or_existing

    buckets = create_zone_buckets()
    layout = create_bronze_layout(buckets)
    topics = create_kafka_topics()

    buckets >> layout
    buckets >> topics


init_platform()
