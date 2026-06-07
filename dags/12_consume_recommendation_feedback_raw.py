from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from kafka import KafkaConsumer


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
sys.path.insert(0, os.path.join(AIRFLOW_HOME, "scripts"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RECOMMENDATION_FEEDBACK = os.getenv(
    "TOPIC_RECOMMENDATION_FEEDBACK",
    "music-recommendation-feedback",
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

CONSUMER_GROUP = os.getenv("RECOMMENDATION_FEEDBACK_CONSUMER_GROUP", "airflow-recommendation-feedback-bronze")
MAX_MESSAGES = int(os.getenv("RECOMMENDATION_FEEDBACK_MAX_MESSAGES", "100"))
CONSUMER_TIMEOUT_MS = int(os.getenv("RECOMMENDATION_FEEDBACK_CONSUMER_TIMEOUT_MS", "5000"))


@dag(
    dag_id="12_raw_feedback",
    description="Consume recommendation feedback events from Kafka and store them as raw JSONL in Bronze.",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["feedback", "recommendations", "kafka", "bronze"],
)
def consume_recommendation_feedback_raw_to_bronze():

    @task()
    def consume_to_temporal() -> dict:
        consumer = KafkaConsumer(
            TOPIC_RECOMMENDATION_FEEDBACK,
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
                "temporal/recommender/feedback/raw/"
                f"ingest_date={date_part}/"
                f"ingest_hour={hour_part}/"
                f"part-{ts_part}.jsonl"
            )

            body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records).encode("utf-8")
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
        if result["stored"] == 0:
            return {"migrated": 0}

        date_part = result["date"]
        hour_part = result["hour"]
        temporal_prefix = (
            "temporal/recommender/feedback/raw/"
            f"ingest_date={date_part}/"
            f"ingest_hour={hour_part}/"
        )
        persistent_prefix = (
            "persistent/recommender/feedback/raw/"
            f"ingest_date={date_part}/"
            f"ingest_hour={hour_part}/"
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        migrated = 0
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=temporal_prefix):
            for obj in page.get("Contents", []):
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
        if ingest["stored"] == 0:
            return "No feedback to record"

        from utils.metadata import create_metadata_record

        metadata = create_metadata_record(
            data_type="semi_structured",
            format_name="jsonl",
            source="kafka",
            temporal_path=(
                "temporal/recommender/feedback/raw/"
                f"ingest_date={ingest['date']}/"
                f"ingest_hour={ingest['hour']}/"
            ),
            persistent_path=(
                "persistent/recommender/feedback/raw/"
                f"ingest_date={ingest['date']}/"
                f"ingest_hour={ingest['hour']}/"
            ),
            record_count=ingest["stored"],
            attributes={
                "topic": TOPIC_RECOMMENDATION_FEEDBACK,
                "migrated": migration["migrated"],
                "event_family": "recommendation_feedback",
            },
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        metadata_key = (
            "metadata/recommender/feedback/jsonl/"
            f"{metadata['timestamp'].replace(':', '-')}.json"
        )
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        return f"Metadata recorded: {metadata_key}"

    ingest = consume_to_temporal()
    migration = migrate_to_persistent(ingest)
    metadata = record_metadata(ingest, migration)

    trigger_trusted_feedback_task = TriggerDagRunOperator(
        task_id="trigger_13_trusted_feedback",
        trigger_dag_id="13_trusted_feedback",
        trigger_run_id="raw_feedback__{{ run_id }}",
        conf={"source_dag_run_id": "{{ run_id }}"},
        reset_dag_run=False,
        skip_when_already_exists=True,
    )

    metadata >> trigger_trusted_feedback_task


consume_recommendation_feedback_raw_to_bronze()
