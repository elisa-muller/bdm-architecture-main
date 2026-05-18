from __future__ import annotations

import base64
import io
import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from PIL import Image, ImageOps, UnidentifiedImageError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


DEFAULT_CLIP_MODEL = "openai/clip-vit-base-patch32"
CLIP_EMBEDDING_DIM = 512
_CLIP_EMBEDDER: ClipImageEmbedder | None = None
_MILVUS_COLLECTION = None


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("hot-path-song-recommendations")
        .getOrCreate()
    )


class ClipImageEmbedder:
    def __init__(self, model_name: str):
        import torch
        from transformers import CLIPModel, CLIPProcessor

        torch.set_num_threads(1)
        self.torch = torch
        self.processor = CLIPProcessor.from_pretrained(model_name)
        self.model = CLIPModel.from_pretrained(model_name)
        self.model.eval()
        self.model_name = model_name

    def embed_image(self, content: bytes) -> list[float]:
        with Image.open(io.BytesIO(content)) as img:
            img = ImageOps.exif_transpose(img).convert("RGB")

        inputs = self.processor(images=img, return_tensors="pt")
        with self.torch.no_grad():
            image_features = self.model.get_image_features(**inputs)
            image_features = normalize_clip_image_features(image_features, self.model, self.torch)
            image_features = image_features / image_features.norm(p=2, dim=-1, keepdim=True)

        embedding = image_features[0].cpu().tolist()
        if len(embedding) != CLIP_EMBEDDING_DIM:
            raise ValueError(
                f"Expected CLIP image embedding dimension {CLIP_EMBEDDING_DIM}, got {len(embedding)}"
            )
        return [float(value) for value in embedding]


def normalize_clip_image_features(image_features, model, torch):
    if torch.is_tensor(image_features):
        return image_features
    if hasattr(image_features, "image_embeds") and torch.is_tensor(image_features.image_embeds):
        return image_features.image_embeds
    if hasattr(image_features, "pooler_output") and torch.is_tensor(image_features.pooler_output):
        pooled = image_features.pooler_output
        if pooled.shape[-1] != CLIP_EMBEDDING_DIM and hasattr(model, "visual_projection"):
            return model.visual_projection(pooled)
        return pooled
    if hasattr(image_features, "last_hidden_state") and torch.is_tensor(image_features.last_hidden_state):
        pooled = image_features.last_hidden_state[:, 0, :]
        if pooled.shape[-1] != CLIP_EMBEDDING_DIM and hasattr(model, "visual_projection"):
            return model.visual_projection(pooled)
        return pooled
    raise TypeError(f"Unexpected CLIP image feature output type: {type(image_features)}")


def get_clip_embedder(model_name: str) -> ClipImageEmbedder:
    global _CLIP_EMBEDDER

    if _CLIP_EMBEDDER is None or _CLIP_EMBEDDER.model_name != model_name:
        _CLIP_EMBEDDER = ClipImageEmbedder(model_name)

    return _CLIP_EMBEDDER


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=env("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=env("MINIO_SECRET_KEY", "minioadmin"),
        region_name=env("AWS_REGION", "us-east-1"),
    )


def ensure_bucket(client, bucket: str) -> None:
    existing = [item["Name"] for item in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def milvus_collection():
    from pymilvus import Collection, connections, utility

    global _MILVUS_COLLECTION
    collection_name = env("MILVUS_SONG_COLLECTION", "song_recommender_embeddings")
    if _MILVUS_COLLECTION is not None and _MILVUS_COLLECTION.name == collection_name:
        return _MILVUS_COLLECTION

    connections.connect(
        alias="default",
        host=env("MILVUS_HOST", "milvus-standalone"),
        port=env("MILVUS_PORT", "19530"),
    )
    if not utility.has_collection(collection_name):
        raise RuntimeError(
            f"Milvus collection '{collection_name}' does not exist. "
            "Run the song embeddings pipeline before starting recommendations."
        )

    collection = Collection(collection_name)
    collection.load()
    _MILVUS_COLLECTION = collection
    return _MILVUS_COLLECTION


def hit_value(hit: Any, field_name: str, default: Any = None) -> Any:
    try:
        value = hit.entity.get(field_name)
        if value is not None:
            return value
    except Exception:
        pass
    try:
        value = getattr(hit, field_name)
        if value is not None:
            return value
    except Exception:
        pass
    return default


def search_song_embeddings(image_embedding: list[float], top_k: int) -> list[dict]:
    collection = milvus_collection()
    results = collection.search(
        data=[image_embedding],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {}},
        limit=top_k,
        output_fields=[
            "isrc",
            "track_name",
            "artist_name",
            "embedding_method",
            "embedding_input_hash",
        ],
    )

    candidates = []
    for rank, hit in enumerate(results[0], start=1):
        candidates.append(
            {
                "rank": rank,
                "isrc": str(hit_value(hit, "isrc", "")),
                "track_name": str(hit_value(hit, "track_name", "")),
                "artist_name": str(hit_value(hit, "artist_name", "")),
                "similarity_score": round(float(hit.distance), 6),
                "embedding_method": str(hit_value(hit, "embedding_method", "")),
                "embedding_input_hash": str(hit_value(hit, "embedding_input_hash", "")),
                "was_selected": rank == 1,
            }
        )
    return candidates


def image_embedding_payload(embedding: list[float]) -> dict:
    store_full_embedding = env("RECOMMENDER_STORE_IMAGE_EMBEDDING", "true").lower() == "true"
    payload = {
        "dim": len(embedding),
        "l2_normalized": True,
    }
    if store_full_embedding:
        payload["values"] = [round(value, 8) for value in embedding]
    else:
        payload["preview"] = [round(value, 6) for value in embedding[:8]]
    return payload


def recommendation_event(event: dict, embedder: ClipImageEmbedder) -> dict | None:
    try:
        request_id = str(event.get("request_id") or event.get("event_id") or "")
        content_b64 = event.get("content_b64")
        if not request_id or not content_b64:
            return None

        image_embedding = embedder.embed_image(base64.b64decode(content_b64))
        top_k = int(env("RECOMMENDER_TOP_K", "10"))
        candidates = search_song_embeddings(image_embedding, top_k)
        selected = candidates[0] if candidates else {}
        second_score = candidates[1]["similarity_score"] if len(candidates) > 1 else None
        selected_score = selected.get("similarity_score")
        top_1_margin = (
            round(float(selected_score) - float(second_score), 6)
            if selected_score is not None and second_score is not None
            else None
        )

        return {
            "request_id": request_id,
            "source_event_id": str(event.get("event_id") or ""),
            "event_ts": str(event.get("event_ts") or ""),
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": str(event.get("source") or "kafka"),
            "platform": str(event.get("platform") or ""),
            "image_name": str(event.get("image_name") or ""),
            "image_embedding_model": embedder.model_name,
            "image_embedding": image_embedding_payload(image_embedding),
            "milvus_collection": env("MILVUS_SONG_COLLECTION", "song_recommender_embeddings"),
            "selection_method": "top_similarity",
            "top_k": top_k,
            "candidate_count": len(candidates),
            "selected_isrc": selected.get("isrc", ""),
            "selected_track_name": selected.get("track_name", ""),
            "selected_artist_name": selected.get("artist_name", ""),
            "selected_similarity_score": selected_score,
            "selected_rank": selected.get("rank"),
            "top_1_margin": top_1_margin,
            "recommendations": candidates,
            "dashboard_enrichment_key": "isrc",
            "song_feature_table_uri": env(
                "EXPLOITATION_RECOMMENDER_FEATURES_DELTA_URI",
                "s3://exploitation/recommender/song_features/delta/song_recommender_features_delta",
            ),
        }
    except (UnidentifiedImageError, OSError, ValueError, TypeError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"[HotPath][Recommender] Failed request={event.get('request_id')}: {exc}")
        return None


def write_recommendation_events(events: list[dict], batch_id: int) -> None:
    client = build_s3_client()
    bucket = env("EXPLOITATION_BUCKET", "exploitation")
    prefix = env(
        "RECOMMENDATION_EVENTS_PREFIX",
        "consumption/recommendations/recommendation_events/",
    ).strip("/")

    ensure_bucket(client, bucket)

    now = datetime.now(timezone.utc)
    for event in events:
        request_id = event["request_id"]
        object_key = (
            f"{prefix}/event_date={now.date().isoformat()}/"
            f"batch_id={batch_id}/{request_id}.json"
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
        print(f"[HotPath][Recommender] Batch {batch_id}: no events")
        return

    model_name = env("IMAGE_EMBEDDING_MODEL", DEFAULT_CLIP_MODEL)
    embedder = get_clip_embedder(model_name)

    recommendation_events = []
    for event in events:
        recommendation = recommendation_event(event, embedder)
        if recommendation is not None:
            recommendation_events.append(recommendation)

    write_recommendation_events(recommendation_events, batch_id)
    print(
        f"[HotPath][Recommender] Batch {batch_id}: requests={len(events)} "
        f"recommendation_events={len(recommendation_events)}"
    )


def main() -> None:
    kafka_bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = env("TOPIC_RECOMMENDATION_REQUESTS", "music-recommendation-requests")
    checkpoint = env(
        "REALTIME_RECOMMENDER_CHECKPOINT_PATH",
        "/tmp/spark-checkpoints/realtime-song-recommendations",
    )
    trigger_seconds = int(env("REALTIME_RECOMMENDER_TRIGGER_SECONDS", "20"))
    max_offsets = int(env("REALTIME_RECOMMENDER_MAX_OFFSETS_PER_TRIGGER", "5"))

    print("[HotPath][Recommender] Starting Spark Structured Streaming recommender")
    print(f"[HotPath][Recommender] Kafka: {kafka_bootstrap}")
    print(f"[HotPath][Recommender] Topic: {topic}")
    print(f"[HotPath][Recommender] Checkpoint: {checkpoint}")
    print(f"[HotPath][Recommender] Trigger seconds: {trigger_seconds}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("request_id", StringType(), True),
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
