from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


ENV_NAMES = [
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "BRONZE_BUCKET",
    "TRUSTED_BUCKET",
    "BRONZE_TRENDS_PREFIX",
    "TRUSTED_TRENDS_DELTA_URI",
    "TRUSTED_TRENDS_REJECTED_PREFIX",
    "TRUSTED_METADATA_PREFIX",
    "TRUSTED_TRENDS_MANIFEST_KEY",
    "TRUSTED_TRENDS_RUN_ID",
    "TRUSTED_TRENDS_PROCESSED_AT",
    "SPARK_EXECUTOR_PYTHON",
]

SOURCE_FIELDS = [
    "post_id",
    "event_ts",
    "source",
    "user_id",
    "artist",
    "track",
    "isrc",
    "caption",
    "hashtags",
    "region",
    "is_viral",
    "views",
    "likes",
    "comments",
    "shares",
]

FIELD_ALIASES = {
    "post_id": ["post_id", "postId", "postID", "id"],
    "event_ts": ["event_ts", "event_timestamp", "eventTime", "timestamp", "created_at"],
    "source": ["source", "platform", "origin"],
    "user_id": ["user_id", "userid", "userId", "userID", "id_user"],
    "artist": ["artist", "artist_name", "artistName"],
    "track": ["track", "track_name", "trackName", "song"],
    "isrc": ["isrc", "ISRC"],
    "caption": ["caption", "text", "description"],
    "hashtags": ["hashtags", "tags", "hash_tags"],
    "region": ["region", "country", "market"],
    "is_viral": ["is_viral", "viral", "isViral"],
    "views": ["views", "view_count", "viewCount"],
    "likes": ["likes", "like_count", "likeCount"],
    "comments": ["comments", "comment_count", "commentCount"],
    "shares": ["shares", "share_count", "shareCount"],
}

REGIONS = ["US", "ES", "FR", "DE", "BR", "UK", "MX", "IT", "JP", "KR"]

RAW_SCHEMA = StructType(
    [
        *[StructField(f"{field}_raw", StringType(), True) for field in SOURCE_FIELDS],
        StructField("source_bucket", StringType(), True),
        StructField("source_key", StringType(), True),
        StructField("source_line_number", LongType(), True),
        StructField("source_last_modified_utc", StringType(), True),
        StructField("source_size_bytes", LongType(), True),
        StructField("raw_json", StringType(), True),
        StructField("malformed_json_error", StringType(), True),
    ]
)


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


def storage_options() -> dict[str, str]:
    endpoint = env("MINIO_ENDPOINT", "http://minio:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "").rstrip("/")
    secure = env("MINIO_SECURE", "false").lower() == "true"
    return {
        "AWS_ACCESS_KEY_ID": env("MINIO_ACCESS_KEY", env("MINIO_ROOT_USER", "minioadmin")),
        "AWS_SECRET_ACCESS_KEY": env(
            "MINIO_SECRET_KEY",
            env("MINIO_ROOT_PASSWORD", "minioadmin"),
        ),
        "AWS_REGION": env("AWS_REGION", "us-east-1"),
        "AWS_ENDPOINT_URL": f"http{'s' if secure else ''}://{endpoint}",
        "AWS_ALLOW_HTTP": "false" if secure else "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def list_source_jsonl(s3, bucket_name: str, prefix: str, max_files: int) -> list[dict]:
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl"):
                continue
            last_modified = obj.get("LastModified")
            objects.append(
                {
                    "Key": key,
                    "Size": obj.get("Size"),
                    "LastModified": (
                        last_modified.astimezone(timezone.utc).isoformat()
                        if last_modified is not None
                        else None
                    ),
                }
            )
            if max_files > 0 and len(objects) >= max_files:
                return objects

    return objects


def delta_table_exists(delta_uri: str) -> bool:
    from deltalake import DeltaTable

    try:
        DeltaTable(delta_uri, storage_options=storage_options())
        return True
    except Exception:
        return False


def load_processed_manifest(s3, bucket_name: str, key: str) -> dict:
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return {"version": 1, "processed_files": {}}
        raise


def write_processed_manifest(s3, bucket_name: str, key: str, manifest: dict) -> None:
    write_json(s3, bucket_name, key, manifest)


def source_file_signature(obj: dict) -> dict:
    return {
        "size": obj.get("Size"),
        "last_modified_utc": obj.get("LastModified"),
    }


def processed_signature_matches(processed: dict, obj: dict) -> bool:
    signature = source_file_signature(obj)
    return (
        processed.get("size") == signature["size"]
        and processed.get("last_modified_utc") == signature["last_modified_utc"]
    )


def select_pending_source_files(
    objects: list[dict],
    manifest: dict,
    max_files: int,
    force_all: bool,
) -> list[dict]:
    processed_files = manifest.get("processed_files", {})
    ordered_objects = sorted(
        objects,
        key=lambda obj: (obj.get("LastModified") or "", obj["Key"]),
    )

    if force_all:
        pending = ordered_objects
    else:
        pending = [
            obj
            for obj in ordered_objects
            if not processed_signature_matches(processed_files.get(obj["Key"], {}), obj)
        ]

    if max_files > 0:
        return pending[:max_files]
    return pending


def mark_source_files_processed(
    manifest: dict,
    objects: list[dict],
    processed_at: str,
    run_id: str,
) -> dict:
    processed_files = dict(manifest.get("processed_files", {}))
    for obj in objects:
        processed_files[obj["Key"]] = {
            **source_file_signature(obj),
            "processed_at_utc": processed_at,
            "run_id": run_id,
        }

    return {
        "version": 1,
        "updated_at_utc": processed_at,
        "processed_files": processed_files,
    }


def first_present(payload: dict[str, Any], aliases: list[str]) -> Any:
    for key in aliases:
        if key in payload:
            return payload[key]
    return None


def scalar_to_raw(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def hashtags_to_raw(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.startswith("["):
            return stripped
        tokens = [token for token in re.split(r"[,;|\s]+", stripped) if token]
        return json.dumps(tokens, ensure_ascii=False)
    return json.dumps([str(value)], ensure_ascii=False)


def canonicalize_record(payload: dict[str, Any]) -> dict[str, str | None]:
    record = {}
    for field in SOURCE_FIELDS:
        value = first_present(payload, FIELD_ALIASES[field])
        if field == "hashtags":
            record[f"{field}_raw"] = hashtags_to_raw(value)
        else:
            record[f"{field}_raw"] = scalar_to_raw(value)
    return record


def read_jsonl_partition(rows: Iterable[dict]) -> Iterable[dict]:
    s3 = build_s3_client()
    bronze_bucket = env("BRONZE_BUCKET", "bronze")

    for obj in rows:
        source_key = obj["Key"]
        response = s3.get_object(Bucket=bronze_bucket, Key=source_key)
        body = response["Body"].read().decode("utf-8", errors="replace")

        for line_number, line in enumerate(body.splitlines(), start=1):
            if not line.strip():
                continue

            base = {
                "source_bucket": bronze_bucket,
                "source_key": source_key,
                "source_line_number": line_number,
                "source_last_modified_utc": obj.get("LastModified"),
                "source_size_bytes": obj.get("Size"),
                "raw_json": line,
                "malformed_json_error": None,
            }
            try:
                payload = json.loads(line)
                if not isinstance(payload, dict):
                    raise ValueError("JSON line is not an object")
                yield {**canonicalize_record(payload), **base}
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                empty = {f"{field}_raw": None for field in SOURCE_FIELDS}
                yield {**empty, **base, "malformed_json_error": str(exc)}


def null_if_blank(column: str) -> F.Column:
    trimmed = F.trim(F.col(column))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def parsed_bool(column: str) -> F.Column:
    normalized = F.lower(null_if_blank(column))
    return (
        F.when(normalized.isin("true", "t", "1", "yes", "y"), F.lit(True))
        .when(normalized.isin("false", "f", "0", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def add_quality_error(errors: list[F.Column], condition: F.Column, label: str) -> None:
    errors.append(F.when(condition, F.lit(label)))


def clean_trends(raw_df: DataFrame, processed_at: str) -> tuple[DataFrame, DataFrame]:
    df = raw_df

    for field in SOURCE_FIELDS:
        if field != "hashtags":
            df = df.withColumn(field, null_if_blank(f"{field}_raw"))

    empty_string_array = F.array().cast("array<string>")
    df = df.withColumn(
        "hashtags",
        F.array_sort(
            F.array_distinct(
                F.filter(
                    F.transform(
                        F.coalesce(
                            F.from_json("hashtags_raw", "array<string>"),
                            empty_string_array,
                        ),
                        lambda tag: F.lower(F.trim(tag)),
                    ),
                    lambda tag: tag.isNotNull() & (tag != ""),
                )
            )
        ),
    )

    df = (
        df.withColumn("isrc", F.upper(F.regexp_replace("isrc", r"[-\s]", "")))
        .withColumn("region", F.upper("region"))
        .withColumn("is_viral", parsed_bool("is_viral_raw"))
        .withColumn("views", F.expr("try_cast(views_raw as BIGINT)"))
        .withColumn("likes", F.expr("try_cast(likes_raw as BIGINT)"))
        .withColumn("comments", F.expr("try_cast(comments_raw as BIGINT)"))
        .withColumn("shares", F.expr("try_cast(shares_raw as BIGINT)"))
        .withColumn("event_timestamp", F.expr("try_to_timestamp(event_ts)"))
        .withColumn("event_ts_utc", F.date_format("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("event_date", F.to_date("event_timestamp").cast("string"))
    )

    df = (
        df.withColumn(
            "caption",
            F.when(F.col("caption").isNull(), F.lit(None)).otherwise(
                F.regexp_replace(F.col("caption"), r"\s+", " ")
            ),
        )
    )

    quality_errors: list[F.Column] = []
    add_quality_error(quality_errors, F.col("malformed_json_error").isNotNull(), "malformed_json")
    add_quality_error(quality_errors, F.col("post_id").isNull(), "missing_post_id")
    add_quality_error(quality_errors, F.col("event_ts").isNull(), "missing_event_ts")
    add_quality_error(quality_errors, F.col("event_timestamp").isNull(), "invalid_event_ts")
    add_quality_error(quality_errors, F.col("source").isNull(), "missing_source")
    add_quality_error(quality_errors, F.col("user_id").isNull(), "missing_user_id")
    add_quality_error(quality_errors, F.col("artist").isNull(), "missing_artist")
    add_quality_error(quality_errors, F.col("track").isNull(), "missing_track")
    add_quality_error(quality_errors, F.col("isrc").isNull(), "missing_isrc")
    add_quality_error(
        quality_errors,
        F.col("post_id").isNotNull()
        & ~F.col("post_id").rlike(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
        ),
        "invalid_post_id_uuid",
    )
    add_quality_error(
        quality_errors,
        F.col("isrc").isNotNull() & ~F.col("isrc").rlike(r"^[A-Z]{2}[A-Z0-9]{3}[0-9]{7}$"),
        "invalid_isrc_format",
    )
    add_quality_error(
        quality_errors,
        F.col("region").isNull() | ~F.col("region").isin(REGIONS),
        "invalid_region",
    )
    add_quality_error(quality_errors, F.size("hashtags") < 1, "missing_hashtags")
    add_quality_error(quality_errors, F.col("is_viral").isNull(), "invalid_is_viral")

    for field in ["views", "likes", "comments", "shares"]:
        add_quality_error(quality_errors, F.col(f"{field}_raw").isNotNull() & F.col(field).isNull(), f"invalid_{field}")
        add_quality_error(quality_errors, F.col(field).isNull(), f"missing_{field}")
        add_quality_error(quality_errors, F.col(field) < 0, f"negative_{field}")

    add_quality_error(quality_errors, F.col("likes") > F.col("views"), "likes_exceed_views")
    add_quality_error(quality_errors, F.col("comments") > F.col("views"), "comments_exceed_views")
    add_quality_error(quality_errors, F.col("shares") > F.col("views"), "shares_exceed_views")

    df = df.withColumn(
        "quality_errors",
        F.filter(F.array(*quality_errors), lambda item: item.isNotNull()),
    ).withColumn("is_valid_record", F.size("quality_errors") == 0)

    valid_candidates = df.filter(F.col("is_valid_record"))
    dedup_window = Window.partitionBy("post_id").orderBy(
        F.col("event_timestamp").asc(),
        F.col("source_last_modified_utc").asc_nulls_last(),
        F.col("source_key").asc(),
        F.col("source_line_number").asc(),
    )
    valid_df = (
        valid_candidates.withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
        .withColumn("trusted_processed_at_utc", F.lit(processed_at))
    )

    rejected_df = df.filter(~F.col("is_valid_record")).select(
        "source_bucket",
        "source_key",
        "source_line_number",
        "raw_json",
        "malformed_json_error",
        "quality_errors",
    )

    final_cols = [
        "post_id",
        "event_ts",
        "event_ts_utc",
        "event_date",
        "source",
        "user_id",
        "artist",
        "track",
        "isrc",
        "caption",
        "hashtags",
        "region",
        "is_viral",
        "views",
        "likes",
        "comments",
        "shares",
        "source_bucket",
        "source_key",
        "source_line_number",
        "trusted_processed_at_utc",
    ]

    return valid_df.select(final_cols), rejected_df


def write_json(s3, bucket_name: str, key: str, payload: dict) -> None:
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def write_rejected_partition(rows: Iterable) -> None:
    rows = list(rows)
    if not rows:
        return

    s3 = build_s3_client()
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")
    rejected_prefix = env("TRUSTED_TRENDS_REJECTED_PREFIX", "semi_structured/trends/rejected/")
    run_id = env("TRUSTED_TRENDS_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    key = f"{rejected_prefix.rstrip('/')}/run_id={run_id}/part-{uuid.uuid4().hex}.jsonl"
    body = "\n".join(json.dumps(row.asDict(recursive=True), ensure_ascii=False) for row in rows)

    s3.put_object(
        Bucket=trusted_bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def trusted_delta_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("post_id", pa.string()),
            pa.field("event_ts", pa.string()),
            pa.field("event_ts_utc", pa.string()),
            pa.field("event_date", pa.string()),
            pa.field("source", pa.string()),
            pa.field("user_id", pa.string()),
            pa.field("artist", pa.string()),
            pa.field("track", pa.string()),
            pa.field("isrc", pa.string()),
            pa.field("caption", pa.string()),
            pa.field("hashtags", pa.list_(pa.string())),
            pa.field("region", pa.string()),
            pa.field("is_viral", pa.bool_()),
            pa.field("views", pa.int64()),
            pa.field("likes", pa.int64()),
            pa.field("comments", pa.int64()),
            pa.field("shares", pa.int64()),
            pa.field("source_bucket", pa.string()),
            pa.field("source_key", pa.string()),
            pa.field("source_line_number", pa.int64()),
            pa.field("trusted_processed_at_utc", pa.string()),
        ]
    )


def write_delta_from_rows(delta_uri: str, df: DataFrame, full_refresh: bool) -> dict[str, Any]:
    from deltalake import DeltaTable, write_deltalake
    import pyarrow as pa

    rows = [row.asDict(recursive=True) for row in df.collect()]
    table = pa.Table.from_pylist(rows, schema=trusted_delta_schema())

    table_exists = delta_table_exists(delta_uri)
    if full_refresh or not table_exists:
        write_deltalake(
            delta_uri,
            table,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=["event_date"],
            storage_options=storage_options(),
        )
        return {
            "mode": "overwrite",
            "rows_in_batch": len(rows),
            "rows_inserted": len(rows),
            "rows_updated": 0,
        }

    if not rows:
        return {
            "mode": "merge",
            "rows_in_batch": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
        }

    metrics = (
        DeltaTable(delta_uri, storage_options=storage_options())
        .merge(
            source=table,
            predicate="target.post_id = source.post_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all(
            predicate=(
                "source.event_ts_utc < target.event_ts_utc OR "
                "(source.event_ts_utc = target.event_ts_utc AND source.source_key < target.source_key) OR "
                "(source.event_ts_utc = target.event_ts_utc AND source.source_key = target.source_key "
                "AND source.source_line_number < target.source_line_number)"
            )
        )
        .when_not_matched_insert_all()
        .execute()
    )
    return {
        "mode": "merge",
        "rows_in_batch": len(rows),
        "rows_inserted": int(metrics.get("num_target_rows_inserted", 0)),
        "rows_updated": int(metrics.get("num_target_rows_updated", 0)),
        "merge_metrics": metrics,
    }


def build_spark(processed_at: str, run_id: str) -> SparkSession:
    builder = SparkSession.builder.appName("trusted-trends-cleaning")
    executor_python = env("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12")
    builder = builder.config("spark.pyspark.python", executor_python)
    builder = builder.config("spark.executorEnv.PYSPARK_PYTHON", executor_python)
    for name in ENV_NAMES:
        value = os.getenv(name)
        if value is not None:
            builder = builder.config(f"spark.executorEnv.{name}", value)
    builder = builder.config("spark.executorEnv.TRUSTED_TRENDS_PROCESSED_AT", processed_at)
    builder = builder.config("spark.executorEnv.TRUSTED_TRENDS_RUN_ID", run_id)
    return builder.getOrCreate()


def main() -> None:
    bronze_bucket = env("BRONZE_BUCKET", "bronze")
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")
    bronze_prefix = env("BRONZE_TRENDS_PREFIX", "persistent/semi_structured/trends/raw/")
    trusted_delta_uri = env(
        "TRUSTED_TRENDS_DELTA_URI",
        f"s3://{trusted_bucket}/semi_structured/trends/delta/trends_clean_delta",
    )
    metadata_prefix = env("TRUSTED_METADATA_PREFIX", "metadata/semi_structured/trends/")
    manifest_key = env(
        "TRUSTED_TRENDS_MANIFEST_KEY",
        f"{metadata_prefix.rstrip('/')}/checkpoints/trusted_trends_processed_files.json",
    )
    max_files = int(env("TRUSTED_TRENDS_MAX_FILES", "0"))
    partitions = int(env("TRUSTED_TRENDS_SPARK_PARTITIONS", "8"))
    full_refresh = env("TRUSTED_TRENDS_FULL_REFRESH", "false").lower() == "true"

    processed_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.environ["TRUSTED_TRENDS_RUN_ID"] = run_id
    os.environ["TRUSTED_TRENDS_PROCESSED_AT"] = processed_at

    print("[Trusted][Trends][Spark] Starting trend JSONL validation and standardization...")
    print(f"[Trusted][Trends][Spark] Source: s3://{bronze_bucket}/{bronze_prefix}")
    print(f"[Trusted][Trends][Spark] Target: {trusted_delta_uri}")

    s3 = build_s3_client()
    ensure_bucket_exists(s3, trusted_bucket)

    objects = list_source_jsonl(s3, bronze_bucket, bronze_prefix, 0)
    manifest = load_processed_manifest(s3, trusted_bucket, manifest_key)
    trusted_table_exists = delta_table_exists(trusted_delta_uri)
    force_all_files = full_refresh or not trusted_table_exists
    pending_objects = select_pending_source_files(
        objects=objects,
        manifest=manifest,
        max_files=max_files,
        force_all=force_all_files,
    )
    print(f"[Trusted][Trends][Spark] Source JSONL files discovered: {len(objects)}")
    print(f"[Trusted][Trends][Spark] Source JSONL files pending: {len(pending_objects)}")

    if not pending_objects:
        report = {
            "run_id": run_id,
            "processed_at_utc": processed_at,
            "engine": "apache_spark",
            "source": f"s3://{bronze_bucket}/{bronze_prefix}",
            "target": trusted_delta_uri,
            "status": "no_new_source_files",
            "total_source_files": len(objects),
            "source_files": 0,
            "source_files_processed": 0,
            "raw_records": 0,
            "valid_records_written": 0,
            "invalid_records_rejected": 0,
            "duplicates_removed": 0,
            "delta_write": {"mode": "skip", "rows_in_batch": 0},
            "manifest": f"s3://{trusted_bucket}/{manifest_key}",
            "spark_partitions": partitions,
            "schema_fields": SOURCE_FIELDS,
            "valid_regions": REGIONS,
        }
        report_key = f"{metadata_prefix.rstrip('/')}/clean_trends_spark_run_{run_id}.json"
        write_json(s3, trusted_bucket, report_key, report)
        print("[Trusted][Trends][Spark] No new source files to process.")
        print(f"[Trusted][Trends][Spark] Report: s3://{trusted_bucket}/{report_key}")
        print("[Trusted][Trends][Spark] Done.")
        return

    spark = build_spark(processed_at, run_id)
    spark.sparkContext.setLogLevel("WARN")

    try:
        if pending_objects:
            slices = max(1, min(partitions, len(pending_objects)))
            raw_rdd = (
                spark.sparkContext.parallelize(pending_objects, slices)
                .mapPartitions(read_jsonl_partition)
            )
            raw_df = spark.createDataFrame(raw_rdd, schema=RAW_SCHEMA)
        else:
            raw_df = spark.createDataFrame([], schema=RAW_SCHEMA)

        raw_df = raw_df.cache()
        raw_count = raw_df.count()
        valid_df, rejected_df = clean_trends(raw_df, processed_at)
        valid_df = valid_df.cache()
        rejected_df = rejected_df.cache()

        valid_before_dedup = raw_df.count() - rejected_df.count()
        valid_count = valid_df.count()
        rejected_count = rejected_df.count()
        duplicates_removed = max(0, valid_before_dedup - valid_count)

        if rejected_count > 0:
            rejected_df.foreachPartition(write_rejected_partition)

        delta_write = write_delta_from_rows(
            trusted_delta_uri,
            valid_df,
            full_refresh=force_all_files,
        )

    finally:
        spark.stop()

    manifest_to_update = {"version": 1, "processed_files": {}} if full_refresh else manifest
    manifest_to_update = mark_source_files_processed(
        manifest=manifest_to_update,
        objects=pending_objects,
        processed_at=processed_at,
        run_id=run_id,
    )
    write_processed_manifest(s3, trusted_bucket, manifest_key, manifest_to_update)

    report = {
        "run_id": run_id,
        "processed_at_utc": processed_at,
        "engine": "apache_spark",
        "source": f"s3://{bronze_bucket}/{bronze_prefix}",
        "target": trusted_delta_uri,
        "total_source_files": len(objects),
        "source_files": len(pending_objects),
        "source_files_processed": len(pending_objects),
        "raw_records": raw_count,
        "valid_records_written": delta_write["rows_in_batch"],
        "invalid_records_rejected": rejected_count,
        "duplicates_removed": duplicates_removed,
        "delta_write": delta_write,
        "manifest": f"s3://{trusted_bucket}/{manifest_key}",
        "spark_partitions": partitions,
        "schema_fields": SOURCE_FIELDS,
        "valid_regions": REGIONS,
    }

    report_key = f"{metadata_prefix.rstrip('/')}/clean_trends_spark_run_{run_id}.json"
    write_json(s3, trusted_bucket, report_key, report)

    print(
        "[Trusted][Trends][Spark] "
        f"files={len(pending_objects)}/{len(objects)}, raw={raw_count}, "
        f"written={delta_write['rows_in_batch']}, rejected={rejected_count}, "
        f"duplicates_removed={duplicates_removed}, delta_mode={delta_write['mode']}"
    )
    print(f"[Trusted][Trends][Spark] Report: s3://{trusted_bucket}/{report_key}")
    print("[Trusted][Trends][Spark] Done.")


if __name__ == "__main__":
    main()
