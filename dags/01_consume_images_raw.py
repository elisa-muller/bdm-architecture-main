from __future__ import annotations

import base64
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from kafka import KafkaConsumer
from airflow.sdk import dag, task

# Add scripts to path for imports
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
sys.path.insert(0, os.path.join(AIRFLOW_HOME, "scripts"))


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IMAGES_RAW = os.getenv("TOPIC_IMAGES_RAW", "music-images-raw")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

CONSUMER_GROUP = "airflow-images-bronze"
MAX_MESSAGES = int(os.getenv("IMAGES_MAX_MESSAGES", "20"))
CONSUMER_TIMEOUT_MS = int(os.getenv("IMAGES_CONSUMER_TIMEOUT_MS", "5000"))


@dag(
    dag_id="01_raw_images",
    description="Consume image payloads from Kafka, store in temporal, migrate to persistent, record metadata.",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["images", "kafka", "bronze", "unstructured"],
)
def consume_images_raw_to_bronze():

    @task()
    def consume_to_temporal() -> dict:
        """Consume images from Kafka and store in temporal zone."""
        consumer = KafkaConsumer(
            TOPIC_IMAGES_RAW,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=MAX_MESSAGES,
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        stored = 0
        date_part = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            for message in consumer:
                event = message.value
                event_id = event["event_id"]
                file_extension = event.get("file_extension", ".jpg")
                content_b64 = event["content_b64"]
                content = base64.b64decode(content_b64)

                object_key = (
                    f"temporal/unstructured/images/raw/ingest_date={date_part}/"
                    f"{event_id}{file_extension}"
                )

                s3.put_object(
                    Bucket=BRONZE_BUCKET,
                    Key=object_key,
                    Body=content,
                    ContentType=event.get("mime_type", "image/jpeg"),
                )

                stored += 1
                if stored >= MAX_MESSAGES:
                    break

            if stored == 0:
                return {"stored": 0, "date": date_part}

            consumer.commit()
            return {"stored": stored, "date": date_part}

        finally:
            consumer.close()

    @task()
    def migrate_to_persistent(result: dict) -> dict:
        """Move images from temporal to persistent zone."""
        if result["stored"] == 0:
            return {"migrated": 0}

        date_part = result["date"]
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        temporal_prefix = f"temporal/unstructured/images/raw/ingest_date={date_part}/"
        persistent_prefix = f"persistent/unstructured/images/raw/ingest_date={date_part}/"

        migrated = 0
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=temporal_prefix)

        for page in pages:
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                dest_key = obj["Key"].replace(temporal_prefix, persistent_prefix)
                s3.copy_object(
                    CopySource={"Bucket": BRONZE_BUCKET, "Key": obj["Key"]},
                    Bucket=BRONZE_BUCKET,
                    Key=dest_key,
                )
                s3.delete_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])
                migrated += 1

        return {"migrated": migrated, "date": date_part}

    @task()
    def record_metadata(ingest: dict, migration: dict) -> str:
        """Record metadata about the ingestion."""
        if ingest["stored"] == 0:
            return "No images to record"

        from utils.metadata import create_metadata_record

        metadata = create_metadata_record(
            data_type="unstructured",
            format_name="image",
            source="kafka",
            temporal_path=f"temporal/unstructured/images/raw/ingest_date={ingest['date']}/",
            persistent_path=f"persistent/unstructured/images/raw/ingest_date={ingest['date']}/",
            record_count=ingest["stored"],
            attributes={"topic": TOPIC_IMAGES_RAW, "migrated": migration["migrated"]},
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        metadata_key = (
            f"metadata/unstructured/image/{metadata['timestamp'].replace(':', '-')}.json"
        )
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
        )

        return f"Metadata recorded: {metadata_key}"

    # Task dependencies
    ingest = consume_to_temporal()
    migration = migrate_to_persistent(ingest)
    record_metadata(ingest, migration)


consume_images_raw_to_bronze()
