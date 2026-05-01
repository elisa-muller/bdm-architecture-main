from __future__ import annotations

import base64
import io
import json
import os
from datetime import datetime, timezone

import boto3
from PIL import Image, ImageOps, UnidentifiedImageError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


DEFAULT_CLIP_MODEL = "openai/clip-vit-base-patch32"
DEFAULT_CONTEXT_LABELS = [
    "party",
    "night out",
    "energetic",
    "happy",
    "chill",
    "relaxing",
    "sad",
    "romantic",
    "workout",
    "travel",
    "beach",
    "study",
    "urban",
    "nature",
]
_CLIP_EMBEDDER: ClipEmbedder | None = None


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("hot-path-realtime-image-context")
        .getOrCreate()
    )


class ClipEmbedder:
    def __init__(self, model_name: str):
        import torch
        from transformers import CLIPModel, CLIPProcessor

        torch.set_num_threads(1)
        self.torch = torch
        self.processor = CLIPProcessor.from_pretrained(model_name)
        self.model = CLIPModel.from_pretrained(model_name)
        self.model.eval()
        self.model_name = model_name

    def classify_image(self, content: bytes, labels: list[str], top_k: int) -> list[dict]:
        with Image.open(io.BytesIO(content)) as img:
            img = ImageOps.exif_transpose(img).convert("RGB")

        prompts = [f"a social media image that feels {label}" for label in labels]
        inputs = self.processor(
            text=prompts,
            images=img,
            return_tensors="pt",
            padding=True,
        )

        with self.torch.no_grad():
            outputs = self.model(**inputs)
            probabilities = outputs.logits_per_image.softmax(dim=1)[0]

        top_k = min(top_k, len(labels))
        top_indices = probabilities.topk(top_k).indices.tolist()
        return [
            {
                "label": labels[index],
                "score": round(float(probabilities[index].cpu()), 6),
            }
            for index in top_indices
        ]


def get_clip_embedder(model_name: str) -> ClipEmbedder:
    global _CLIP_EMBEDDER

    if _CLIP_EMBEDDER is None or _CLIP_EMBEDDER.model_name != model_name:
        _CLIP_EMBEDDER = ClipEmbedder(model_name)

    return _CLIP_EMBEDDER


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=env("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=env("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket: str) -> None:
    existing = [item["Name"] for item in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def labels_from_env() -> list[str]:
    raw_labels = env("IMAGE_CONTEXT_LABELS", "")
    if not raw_labels.strip():
        return DEFAULT_CONTEXT_LABELS
    return [label.strip() for label in raw_labels.split(",") if label.strip()]


def event_to_context(event: dict, embedder: ClipEmbedder, labels: list[str]) -> dict | None:
    try:
        event_id = event.get("event_id")
        content_b64 = event.get("content_b64")
        if not event_id or not content_b64:
            return None

        content = base64.b64decode(content_b64)
        top_tags = embedder.classify_image(
            content=content,
            labels=labels,
            top_k=int(env("IMAGE_CONTEXT_TOP_K", "3")),
        )

        return {
            "event_id": str(event_id),
            "event_ts": str(event.get("event_ts") or ""),
            "image_name": str(event.get("image_name") or ""),
            "source": str(event.get("source") or "kafka"),
            "platform": str(event.get("platform") or ""),
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "context_method": f"clip-zero-shot::{embedder.model_name}",
            "visual_mood": top_tags[0]["label"] if top_tags else "",
            "visual_tags": [item["label"] for item in top_tags],
            "visual_tag_scores": top_tags,
            "intended_consumer": "song_recommender",
        }
    except (UnidentifiedImageError, OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        print(f"[HotPath][Images] Failed event={event.get('event_id')}: {exc}")
        return None


def write_context_events(events: list[dict], batch_id: int) -> None:
    client = build_s3_client()
    bucket = env("EXPLOITATION_BUCKET", "exploitation")
    prefix = env(
        "IMAGE_CONTEXT_EVENTS_PREFIX",
        "consumption/recommendations/image_context_events/",
    ).strip("/")

    ensure_bucket(client, bucket)

    now = datetime.now(timezone.utc)
    for event in events:
        event_id = event["event_id"]
        object_key = (
            f"{prefix}/event_date={now.date().isoformat()}/"
            f"batch_id={batch_id}/{event_id}.json"
        )
        client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=json.dumps(event, ensure_ascii=True).encode("utf-8"),
            ContentType="application/json",
        )


def process_batch(batch_df, batch_id: int) -> None:
    events = [
        json.loads(row.value)
        for row in batch_df.select("value").collect()
        if row.value and row.value != "null"
    ]
    if not events:
        print(f"[HotPath][Images] Batch {batch_id}: no events")
        return

    model_name = env("IMAGE_EMBEDDING_MODEL", DEFAULT_CLIP_MODEL)
    labels = labels_from_env()

    embedder = get_clip_embedder(model_name)

    context_events = []
    for event in events:
        context = event_to_context(event, embedder, labels)
        if context is not None:
            context_events.append(context)

    write_context_events(context_events, batch_id)
    print(
        f"[HotPath][Images] Batch {batch_id}: events={len(events)} "
        f"context_events={len(context_events)}"
    )


def main() -> None:
    kafka_bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = env("TOPIC_IMAGES_RAW", "music-images-raw")
    checkpoint = env(
        "REALTIME_IMAGE_CHECKPOINT_PATH",
        "/tmp/spark-checkpoints/realtime-image-context",
    )
    trigger_seconds = int(env("REALTIME_IMAGE_TRIGGER_SECONDS", "20"))
    max_offsets = int(env("REALTIME_IMAGE_MAX_OFFSETS_PER_TRIGGER", "10"))

    print("[HotPath][Images] Starting Spark Structured Streaming context job")
    print(f"[HotPath][Images] Kafka: {kafka_bootstrap}")
    print(f"[HotPath][Images] Topic: {topic}")
    print(f"[HotPath][Images] Checkpoint: {checkpoint}")
    print(f"[HotPath][Images] Trigger seconds: {trigger_seconds}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("source", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("image_name", StringType(), True),
            StructField("content_b64", StringType(), True),
        ]
    )

    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    parsed_df = (
        stream_df
        .selectExpr("CAST(value AS STRING) AS value")
        .select(from_json(col("value"), schema).alias("event"))
        .selectExpr("to_json(event) AS value")
    )

    query = (
        parsed_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
