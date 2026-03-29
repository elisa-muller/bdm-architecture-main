from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import boto3
from kafka import KafkaConsumer
from airflow.decorators import dag, task


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
    dag_id="consume_trends_raw_to_bronze",
    description="Consume semistructured trend events from Kafka and store raw JSONL in Bronze temporal.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["trends", "kafka", "bronze", "semi-structured"],
)
def consume_trends_raw_to_bronze():

    @task()
    def consume_and_store() -> str:
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
                return "No trend messages found."

            now = datetime.now(timezone.utc)
            date_part = now.strftime("%Y-%m-%d")
            hour_part = now.strftime("%H")
            ts_part = now.strftime("%Y%m%dT%H%M%SZ")

            object_key = (
                "temporal/semi_structured/trends/raw/"
                f"ingest_date={date_part}/"
                f"ingest_hour={hour_part}/"
                f"part-{ts_part}.jsonl"
            )

            body = "\n".join(
                json.dumps(record, ensure_ascii=False)
                for record in records
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

            return (
                f"Stored {len(records)} trend events in "
                f"s3://{BRONZE_BUCKET}/{object_key}"
            )

        finally:
            consumer.close()

    consume_and_store()


consume_trends_raw_to_bronze()