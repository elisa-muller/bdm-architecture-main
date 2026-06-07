from __future__ import annotations

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
TOPIC_TRENDS_RAW = os.getenv("TOPIC_TRENDS_RAW", "music-trends-raw")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

CONSUMER_GROUP = os.getenv("TRENDS_CONSUMER_GROUP", "airflow-trends-bronze")
MAX_MESSAGES = int(os.getenv("TRENDS_MAX_MESSAGES", "100"))
CONSUMER_TIMEOUT_MS = int(os.getenv("TRENDS_CONSUMER_TIMEOUT_MS", "5000"))


@dag(
    dag_id="02_raw_trends",
    description="Consume semistructured trend events from Kafka, store in temporal, migrate to persistent, record metadata.",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",  # every 5 minutes
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["trends", "kafka", "bronze", "semi-structured"],
)
def consume_trends_raw_to_bronze():

    @task()
    def consume_to_temporal() -> dict:
        """Consume trends from Kafka and store in temporal zone."""
        consumer = KafkaConsumer(
            TOPIC_TRENDS_RAW,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=MAX_MESSAGES,
        )

        records: list[dict] = []

        try:
            for message in consumer:
                records.append(message.value)
                if len(records) >= MAX_MESSAGES:
                    break

            if not records:
                return {"stored": 0}

            now = datetime.now(timezone.utc)
            date_part = now.strftime("%Y-%m-%d")
            hour_part = now.strftime("%H")
            ts_part = now.strftime("%Y%m%dT%H%M%SZ")

            object_key = (
                f"temporal/semi_structured/trends/raw/"
                f"ingest_date={date_part}/"
                f"ingest_hour={hour_part}/"
                f"part-{ts_part}.jsonl"
            )

            body = "\n".join(
                json.dumps(record, ensure_ascii=False) for record in records
            ).encode("utf-8")

            s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
            )

            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=object_key,
                Body=body,
                ContentType="application/x-ndjson",
            )

            consumer.commit()
            return {"stored": len(records), "date": date_part, "hour": hour_part}

        finally:
            consumer.close()

    @task()
    def migrate_to_persistent(result: dict) -> dict:
        """Move trends from temporal to persistent zone."""
        if result["stored"] == 0:
            return {"migrated": 0}

        date_part = result["date"]
        hour_part = result["hour"]
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        temporal_prefix = (
            f"temporal/semi_structured/trends/raw/"
            f"ingest_date={date_part}/"
            f"ingest_hour={hour_part}/"
        )
        persistent_prefix = (
            f"persistent/semi_structured/trends/raw/"
            f"ingest_date={date_part}/"
            f"ingest_hour={hour_part}/"
        )

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

        return {"migrated": migrated, "date": date_part, "hour": hour_part}

    @task()
    def record_metadata(ingest: dict, migration: dict) -> str:
        """Record metadata about the ingestion."""
        if ingest["stored"] == 0:
            return "No trends to record"

        from utils.metadata import create_metadata_record

        metadata = create_metadata_record(
            data_type="semi_structured",
            format_name="jsonl",
            source="kafka",
            temporal_path=(
                f"temporal/semi_structured/trends/raw/"
                f"ingest_date={ingest['date']}/"
                f"ingest_hour={ingest['hour']}/"
            ),
            persistent_path=(
                f"persistent/semi_structured/trends/raw/"
                f"ingest_date={ingest['date']}/"
                f"ingest_hour={ingest['hour']}/"
            ),
            record_count=ingest["stored"],
            attributes={"topic": TOPIC_TRENDS_RAW, "migrated": migration["migrated"]},
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        metadata_key = (
            f"metadata/semi_structured/jsonl/{metadata['timestamp'].replace(':', '-')}.json"
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


consume_trends_raw_to_bronze()
