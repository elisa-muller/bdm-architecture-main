from __future__ import annotations

import hashlib
import io
import json
import os
from datetime import datetime, timezone
from pathlib import PurePosixPath

import boto3
from botocore.exceptions import ClientError
from PIL import Image, ImageOps, UnidentifiedImageError
from pyspark.sql import SparkSession


ENV_NAMES = [
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "BRONZE_BUCKET",
    "TRUSTED_BUCKET",
    "BRONZE_IMAGES_PREFIX",
    "TRUSTED_IMAGES_PREFIX",
    "TRUSTED_REJECTED_PREFIX",
    "TRUSTED_METADATA_PREFIX",
    "TRUSTED_IMAGE_RESIZE_MAX_DIM",
    "TRUSTED_IMAGE_JPEG_QUALITY",
    "TRUSTED_IMAGE_MAX_IMAGES",
    "TRUSTED_IMAGE_SKIP_EXISTING",
    "SPARK_EXECUTOR_PYTHON",
]

VALID_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=env("MINIO_ACCESS_KEY", env("MINIO_ROOT_USER", "minioadmin")),
        aws_secret_access_key=env(
            "MINIO_SECRET_KEY",
            env("MINIO_ROOT_PASSWORD", "minioadmin"),
        ),
    )


def ensure_bucket_exists(s3, bucket_name: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)


def trusted_key_for(source_key: str, bronze_prefix: str, trusted_prefix: str) -> str:
    relative_key = source_key.removeprefix(bronze_prefix)
    relative_path = PurePosixPath(relative_key)
    parent = "" if str(relative_path.parent) == "." else f"{relative_path.parent}/"
    return f"{trusted_prefix}{parent}{relative_path.stem}.jpg"


def rejected_key_for(source_key: str, bronze_prefix: str, rejected_prefix: str) -> str:
    relative_key = source_key.removeprefix(bronze_prefix)
    return f"{rejected_prefix}{relative_key}.json"


def list_source_images(s3, bucket_name: str, prefix: str, max_images: int) -> list[dict]:
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if PurePosixPath(key).suffix.lower() not in VALID_EXTENSIONS:
                continue
            objects.append({"Key": key, "Size": obj.get("Size")})
            if max_images > 0 and len(objects) >= max_images:
                return objects

    return objects


def clean_image(content: bytes, resize_max_dim: int, jpeg_quality: int) -> tuple[bytes, dict]:
    with Image.open(io.BytesIO(content)) as img:
        original_format = img.format
        original_width, original_height = img.size
        img.verify()

    with Image.open(io.BytesIO(content)) as img:
        img = ImageOps.exif_transpose(img)
        img = img.convert("RGB")
        img.thumbnail((resize_max_dim, resize_max_dim), Image.Resampling.LANCZOS)

        output = io.BytesIO()
        img.save(output, format="JPEG", quality=jpeg_quality, optimize=True)
        cleaned = output.getvalue()

        metadata = {
            "original_format": original_format,
            "original_width": original_width,
            "original_height": original_height,
            "clean_format": "JPEG",
            "clean_width": img.size[0],
            "clean_height": img.size[1],
            "clean_size_bytes": len(cleaned),
            "sha256": hashlib.sha256(cleaned).hexdigest(),
        }

    return cleaned, metadata


def write_json(s3, bucket_name: str, key: str, payload: dict) -> None:
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def target_exists(s3, bucket_name: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def process_partition(rows):
    s3 = build_s3_client()

    bronze_bucket = env("BRONZE_BUCKET", "bronze")
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")
    bronze_prefix = env("BRONZE_IMAGES_PREFIX", "persistent/unstructured/images/raw/")
    trusted_prefix = env("TRUSTED_IMAGES_PREFIX", "unstructured/images/clean/")
    rejected_prefix = env("TRUSTED_REJECTED_PREFIX", "unstructured/images/rejected/")
    resize_max_dim = int(env("TRUSTED_IMAGE_RESIZE_MAX_DIM", "512"))
    jpeg_quality = int(env("TRUSTED_IMAGE_JPEG_QUALITY", "85"))
    skip_existing = env("TRUSTED_IMAGE_SKIP_EXISTING", "true").lower() == "true"
    processed_at = env("TRUSTED_IMAGE_PROCESSED_AT", datetime.now(timezone.utc).isoformat())

    for obj in rows:
        source_key = obj["Key"]
        target_key = trusted_key_for(source_key, bronze_prefix, trusted_prefix)
        result = {
            "source_bucket": bronze_bucket,
            "source_key": source_key,
            "processed_at_utc": processed_at,
            "status": "unknown",
        }

        try:
            if skip_existing and target_exists(s3, trusted_bucket, target_key):
                result.update(
                    {
                        "status": "skipped_existing",
                        "target_bucket": trusted_bucket,
                        "target_key": target_key,
                    }
                )
                yield result
                continue

            response = s3.get_object(Bucket=bronze_bucket, Key=source_key)
            content = response["Body"].read()
            cleaned, image_metadata = clean_image(content, resize_max_dim, jpeg_quality)

            s3.put_object(
                Bucket=trusted_bucket,
                Key=target_key,
                Body=cleaned,
                ContentType="image/jpeg",
                Metadata={
                    "source-bucket": bronze_bucket,
                    "source-key-sha256": hashlib.sha256(source_key.encode("utf-8")).hexdigest(),
                    "cleaned-at-utc": processed_at,
                },
            )

            result.update(
                {
                    "status": "cleaned",
                    "target_bucket": trusted_bucket,
                    "target_key": target_key,
                    "source_size_bytes": obj.get("Size"),
                    **image_metadata,
                }
            )

        except (UnidentifiedImageError, OSError, ValueError) as exc:
            result.update(
                {
                    "status": "rejected",
                    "reason": "invalid_or_corrupted_image",
                    "error": str(exc),
                }
            )
            write_json(s3, trusted_bucket, rejected_key_for(source_key, bronze_prefix, rejected_prefix), result)

        yield result


def build_spark(processed_at: str) -> SparkSession:
    builder = SparkSession.builder.appName("trusted-images-cleaning")
    executor_python = env("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12")
    builder = builder.config("spark.pyspark.python", executor_python)
    builder = builder.config("spark.executorEnv.PYSPARK_PYTHON", executor_python)
    for name in ENV_NAMES:
        value = os.getenv(name)
        if value is not None:
            builder = builder.config(f"spark.executorEnv.{name}", value)
    builder = builder.config("spark.executorEnv.TRUSTED_IMAGE_PROCESSED_AT", processed_at)
    return builder.getOrCreate()


def main() -> None:
    bronze_bucket = env("BRONZE_BUCKET", "bronze")
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")
    bronze_prefix = env("BRONZE_IMAGES_PREFIX", "persistent/unstructured/images/raw/")
    trusted_prefix = env("TRUSTED_IMAGES_PREFIX", "unstructured/images/clean/")
    metadata_prefix = env("TRUSTED_METADATA_PREFIX", "metadata/unstructured/images/")
    resize_max_dim = int(env("TRUSTED_IMAGE_RESIZE_MAX_DIM", "512"))
    jpeg_quality = int(env("TRUSTED_IMAGE_JPEG_QUALITY", "85"))
    max_images = int(env("TRUSTED_IMAGE_MAX_IMAGES", "0"))
    partitions = int(env("TRUSTED_IMAGE_SPARK_PARTITIONS", "8"))

    processed_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    print("[Trusted][Images][Spark] Starting image validation and standardization...")
    print(f"[Trusted][Images][Spark] Source: s3://{bronze_bucket}/{bronze_prefix}")
    print(f"[Trusted][Images][Spark] Target: s3://{trusted_bucket}/{trusted_prefix}")

    s3 = build_s3_client()
    ensure_bucket_exists(s3, trusted_bucket)

    objects = list_source_images(s3, bronze_bucket, bronze_prefix, max_images)
    print(f"[Trusted][Images][Spark] Source images discovered: {len(objects)}")

    spark = build_spark(processed_at)
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not objects:
            results = []
        else:
            slices = max(1, min(partitions, len(objects)))
            results = (
                spark.sparkContext
                .parallelize(objects, slices)
                .mapPartitions(process_partition)
                .collect()
            )
    finally:
        spark.stop()

    counts = {}
    for item in results:
        counts[item["status"]] = counts.get(item["status"], 0) + 1

    report = {
        "run_id": run_id,
        "processed_at_utc": processed_at,
        "engine": "apache_spark",
        "source": f"s3://{bronze_bucket}/{bronze_prefix}",
        "target": f"s3://{trusted_bucket}/{trusted_prefix}",
        "resize_max_dim": resize_max_dim,
        "jpeg_quality": jpeg_quality,
        "spark_partitions": partitions,
        "total_seen": len(results),
        "status_counts": counts,
        "results": results,
    }

    report_key = f"{metadata_prefix}clean_images_spark_run_{run_id}.json"
    write_json(s3, trusted_bucket, report_key, report)

    print(f"[Trusted][Images][Spark] Status counts: {counts}")
    print(f"[Trusted][Images][Spark] Report: s3://{trusted_bucket}/{report_key}")
    print("[Trusted][Images][Spark] Done.")


if __name__ == "__main__":
    main()
