from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
from kafka import KafkaConsumer
from airflow.decorators import dag, task


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
    dag_id="consume_images_raw_to_bronze",
    description="Consume image payloads from Kafka and store native image files in Bronze temporal.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["images", "kafka", "bronze", "unstructured"],
)
def consume_images_raw_to_bronze():

    @task()
    def consume_and_store() -> str:
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

        try:
            for message in consumer:
                event = message.value

                event_id = event["event_id"]
                file_extension = event.get("file_extension", ".jpg")
                content_b64 = event["content_b64"]

                content = base64.b64decode(content_b64)

                now = datetime.now(timezone.utc)
                date_part = now.strftime("%Y-%m-%d")

                object_key = (
                    "temporal/unstructured/images/raw/"
                    f"ingest_date={date_part}/"
                    f"{event_id}{file_extension}"
                )

                s3.put_object(
                    Bucket=BRONZE_BUCKET,
                    Key=object_key,
                    Body=content,
                    ContentType=event.get("mime_type", "image/jpeg"),
                )

                stored += 1
                print(f"Stored image in MinIO: {object_key}")

                if stored >= MAX_MESSAGES:
                    break

            if stored == 0:
                return "No image messages found."

            consumer.commit()
            return f"Stored {stored} image files in Bronze."

        finally:
            consumer.close()

    consume_and_store()


consume_images_raw_to_bronze()