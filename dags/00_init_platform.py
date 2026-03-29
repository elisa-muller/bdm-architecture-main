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


@dag(
    dag_id="init_platform",
    description="Initialize platform infrastructure: Bronze bucket, Kafka topics, and Bronze folder structure.",
    schedule=None,
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["init", "kafka", "minio", "bronze"],
)
def init_platform():

    @task()
    def create_bronze_bucket() -> str:
        from minio import Minio

        endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        secure = MINIO_ENDPOINT.startswith("https://")

        client = Minio(
            endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=secure,
        )

        if client.bucket_exists(BRONZE_BUCKET):
            print(f"Bucket '{BRONZE_BUCKET}' already exists.")
        else:
            client.make_bucket(BRONZE_BUCKET)
            print(f"Bucket '{BRONZE_BUCKET}' created.")

        return BRONZE_BUCKET

    @task()
    def create_bronze_layout(bucket_name: str) -> list[str]:
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

        # These are placeholder objects so the structure is visible in MinIO UI.
        # In S3/MinIO, folders are really just prefixes.
        prefixes = [
            # Temporal = raw landing zone used in P1
            "temporal/structured/lastfm/raw/",
            "temporal/structured/musicbrainz/raw/",
            "temporal/structured/reccobeats/raw/",
            "temporal/semi_structured/trends/raw/",
            #"temporal/unstructured/images/raw/", --> we decided not to store them

            # Persistent = future-ready area, mostly architecture placeholder for now
            "persistent/structured/lastfm/delta/",
            "persistent/structured/musicbrainz/delta/",
            "persistent/structured/reccobeats/delta/",
            "persistent/semi_structured/trends/delta/",
            #"persistent/unstructured/images/delta/", --> we decided not to store them
        ]

        created = []

        for prefix in prefixes:
            object_name = f"{prefix}.keep"

            try:
                # Check whether placeholder already exists
                client.stat_object(bucket_name, object_name)
                print(f"Prefix already initialized: {object_name}")
            except S3Error as e:
                if e.code == "NoSuchKey" or e.code == "NoSuchObject" or e.code == "NoSuchResource":
                    data = io.BytesIO(b"")
                    client.put_object(
                        bucket_name=bucket_name,
                        object_name=object_name,
                        data=data,
                        length=0,
                        content_type="text/plain",
                    )
                    print(f"Created prefix placeholder: {object_name}")
                else:
                    raise

            created.append(object_name)

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

    bucket = create_bronze_bucket()
    layout = create_bronze_layout(bucket)
    topics = create_kafka_topics()

    bucket >> layout
    bucket >> topics


init_platform()