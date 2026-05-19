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


SOURCE_FIELDS = [
    "feedback_id",
    "request_id",
    "event_ts",
    "source",
    "platform",
    "user_id",
    "action",
    "selected_isrc",
    "selected_track_name",
    "selected_artist_name",
    "selected_rank",
    "candidate_count",
    "top_k",
    "system_selected_isrc",
    "system_selected_track_name",
    "system_selected_artist_name",
    "system_selected_rank",
    "system_selected_similarity_score",
    "accepted_system_recommendation",
    "was_candidate_selection",
    "dwell_time_ms",
    "satisfaction_score",
    "recommendation_event_key",
]

VALID_ACTIONS = ["accepted", "selected_candidate", "skipped", "external_selection"]
ISRC_PATTERN = r"^[A-Z]{2}[A-Z0-9]{3}[0-9]{7}$"
UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"

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
        aws_secret_access_key=env("MINIO_SECRET_KEY", env("MINIO_ROOT_PASSWORD", "minioadmin")),
    )


def storage_options() -> dict[str, str]:
    endpoint = env("MINIO_ENDPOINT", "http://minio:9000").rstrip("/")
    secure = endpoint.startswith("https://")
    return {
        "AWS_ACCESS_KEY_ID": env("MINIO_ACCESS_KEY", env("MINIO_ROOT_USER", "minioadmin")),
        "AWS_SECRET_ACCESS_KEY": env("MINIO_SECRET_KEY", env("MINIO_ROOT_PASSWORD", "minioadmin")),
        "AWS_REGION": env("AWS_REGION", "us-east-1"),
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "false" if secure else "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def ensure_bucket_exists(s3, bucket_name: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)


def write_json(s3, bucket_name: str, key: str, payload: dict) -> None:
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def list_source_jsonl(s3, bucket_name: str, prefix: str, max_files: int) -> list[dict]:
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
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


def source_file_signature(obj: dict) -> dict:
    return {"size": obj.get("Size"), "last_modified_utc": obj.get("LastModified")}


def processed_signature_matches(processed: dict, obj: dict) -> bool:
    signature = source_file_signature(obj)
    return (
        processed.get("size") == signature["size"]
        and processed.get("last_modified_utc") == signature["last_modified_utc"]
    )


def select_pending_source_files(objects: list[dict], manifest: dict, max_files: int, force_all: bool) -> list[dict]:
    ordered_objects = sorted(objects, key=lambda obj: (obj.get("LastModified") or "", obj["Key"]))
    if force_all:
        pending = ordered_objects
    else:
        processed_files = manifest.get("processed_files", {})
        pending = [
            obj
            for obj in ordered_objects
            if not processed_signature_matches(processed_files.get(obj["Key"], {}), obj)
        ]
    return pending[:max_files] if max_files > 0 else pending


def mark_source_files_processed(manifest: dict, objects: list[dict], processed_at: str, run_id: str) -> dict:
    processed_files = dict(manifest.get("processed_files", {}))
    for obj in objects:
        processed_files[obj["Key"]] = {
            **source_file_signature(obj),
            "processed_at_utc": processed_at,
            "run_id": run_id,
        }
    return {"version": 1, "updated_at_utc": processed_at, "processed_files": processed_files}


def scalar_to_raw(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def canonicalize_record(payload: dict[str, Any]) -> dict[str, str | None]:
    return {f"{field}_raw": scalar_to_raw(payload.get(field)) for field in SOURCE_FIELDS}


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
    lowered = F.lower(trimmed)
    return F.when((trimmed == "") | lowered.isin("nan", "none", "null"), F.lit(None)).otherwise(trimmed)


def parsed_bool(column: str) -> F.Column:
    normalized = F.lower(null_if_blank(column))
    return (
        F.when(normalized.isin("true", "t", "1", "yes", "y"), F.lit(True))
        .when(normalized.isin("false", "f", "0", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def add_quality_error(errors: list[F.Column], condition: F.Column, label: str) -> None:
    errors.append(F.when(condition, F.lit(label)))


def clean_feedback(raw_df: DataFrame, processed_at: str) -> tuple[DataFrame, DataFrame]:
    df = raw_df
    for field in SOURCE_FIELDS:
        df = df.withColumn(field, null_if_blank(f"{field}_raw"))

    for field in ["selected_isrc", "system_selected_isrc"]:
        df = df.withColumn(field, F.upper(F.regexp_replace(F.col(field), r"[-\s]", "")))

    df = (
        df.withColumn("event_timestamp", F.expr("try_to_timestamp(event_ts)"))
        .withColumn("event_ts_utc", F.date_format("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("event_date", F.to_date("event_timestamp").cast("string"))
        .withColumn("action", F.lower("action"))
        .withColumn("selected_rank", F.expr("try_cast(selected_rank as INT)"))
        .withColumn("candidate_count", F.expr("try_cast(candidate_count as INT)"))
        .withColumn("top_k", F.expr("try_cast(top_k as INT)"))
        .withColumn("system_selected_rank", F.expr("try_cast(system_selected_rank as INT)"))
        .withColumn("system_selected_similarity_score", F.expr("try_cast(system_selected_similarity_score as DOUBLE)"))
        .withColumn("accepted_system_recommendation", parsed_bool("accepted_system_recommendation_raw"))
        .withColumn("was_candidate_selection", parsed_bool("was_candidate_selection_raw"))
        .withColumn("dwell_time_ms", F.expr("try_cast(dwell_time_ms as BIGINT)"))
        .withColumn("satisfaction_score", F.expr("try_cast(satisfaction_score as INT)"))
    )

    quality_errors: list[F.Column] = []
    add_quality_error(quality_errors, F.col("malformed_json_error").isNotNull(), "malformed_json")
    add_quality_error(quality_errors, F.col("feedback_id").isNull(), "missing_feedback_id")
    add_quality_error(quality_errors, F.col("request_id").isNull(), "missing_request_id")
    add_quality_error(quality_errors, F.col("event_ts").isNull(), "missing_event_ts")
    add_quality_error(quality_errors, F.col("event_timestamp").isNull(), "invalid_event_ts")
    add_quality_error(quality_errors, F.col("source").isNull(), "missing_source")
    add_quality_error(quality_errors, F.col("platform").isNull(), "missing_platform")
    add_quality_error(quality_errors, F.col("action").isNull(), "missing_action")
    add_quality_error(quality_errors, ~F.col("action").isin(VALID_ACTIONS), "invalid_action")
    add_quality_error(quality_errors, F.col("feedback_id").isNotNull() & ~F.col("feedback_id").rlike(UUID_PATTERN), "invalid_feedback_id_uuid")
    add_quality_error(quality_errors, F.col("request_id").isNotNull() & ~F.col("request_id").rlike(UUID_PATTERN), "invalid_request_id_uuid")
    add_quality_error(quality_errors, F.col("candidate_count").isNull() | (F.col("candidate_count") < 0), "invalid_candidate_count")
    add_quality_error(quality_errors, F.col("top_k").isNull() | (F.col("top_k") < 0), "invalid_top_k")
    add_quality_error(quality_errors, F.col("system_selected_rank").isNotNull() & (F.col("system_selected_rank") < 1), "invalid_system_selected_rank")
    add_quality_error(quality_errors, F.col("selected_rank").isNotNull() & (F.col("selected_rank") < 1), "invalid_selected_rank")
    add_quality_error(quality_errors, F.col("selected_rank").isNotNull() & (F.col("selected_rank") > F.col("candidate_count")), "selected_rank_exceeds_candidate_count")
    add_quality_error(quality_errors, F.col("selected_isrc").isNotNull() & ~F.col("selected_isrc").rlike(ISRC_PATTERN), "invalid_selected_isrc")
    add_quality_error(quality_errors, F.col("system_selected_isrc").isNotNull() & ~F.col("system_selected_isrc").rlike(ISRC_PATTERN), "invalid_system_selected_isrc")
    add_quality_error(quality_errors, F.col("dwell_time_ms").isNotNull() & (F.col("dwell_time_ms") < 0), "negative_dwell_time_ms")
    add_quality_error(quality_errors, F.col("satisfaction_score").isNotNull() & ~F.col("satisfaction_score").between(1, 5), "invalid_satisfaction_score")

    add_quality_error(
        quality_errors,
        (F.col("action") == "accepted")
        & ((F.col("selected_rank") != 1) | (F.col("accepted_system_recommendation") != F.lit(True))),
        "accepted_action_inconsistent",
    )
    add_quality_error(
        quality_errors,
        (F.col("action") == "skipped")
        & (F.col("selected_rank").isNotNull() | F.col("selected_isrc").isNotNull()),
        "skipped_action_has_selection",
    )
    add_quality_error(
        quality_errors,
        F.col("action").isin("accepted", "selected_candidate")
        & (F.col("selected_isrc").isNull() | F.col("selected_rank").isNull()),
        "candidate_action_missing_selection",
    )

    df = df.withColumn(
        "quality_errors",
        F.filter(F.array(*quality_errors), lambda item: item.isNotNull()),
    ).withColumn("is_valid_record", F.size("quality_errors") == 0)

    valid_candidates = df.filter(F.col("is_valid_record"))
    dedup_window = Window.partitionBy("feedback_id").orderBy(
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
        "feedback_id",
        "request_id",
        "event_ts",
        "event_ts_utc",
        "event_date",
        "source",
        "platform",
        "user_id",
        "action",
        "selected_isrc",
        "selected_track_name",
        "selected_artist_name",
        "selected_rank",
        "candidate_count",
        "top_k",
        "system_selected_isrc",
        "system_selected_track_name",
        "system_selected_artist_name",
        "system_selected_rank",
        "system_selected_similarity_score",
        "accepted_system_recommendation",
        "was_candidate_selection",
        "dwell_time_ms",
        "satisfaction_score",
        "recommendation_event_key",
        "source_bucket",
        "source_key",
        "source_line_number",
        "trusted_processed_at_utc",
    ]
    return valid_df.select(final_cols), rejected_df


def trusted_delta_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("feedback_id", pa.string()),
            pa.field("request_id", pa.string()),
            pa.field("event_ts", pa.string()),
            pa.field("event_ts_utc", pa.string()),
            pa.field("event_date", pa.string()),
            pa.field("source", pa.string()),
            pa.field("platform", pa.string()),
            pa.field("user_id", pa.string()),
            pa.field("action", pa.string()),
            pa.field("selected_isrc", pa.string()),
            pa.field("selected_track_name", pa.string()),
            pa.field("selected_artist_name", pa.string()),
            pa.field("selected_rank", pa.int32()),
            pa.field("candidate_count", pa.int32()),
            pa.field("top_k", pa.int32()),
            pa.field("system_selected_isrc", pa.string()),
            pa.field("system_selected_track_name", pa.string()),
            pa.field("system_selected_artist_name", pa.string()),
            pa.field("system_selected_rank", pa.int32()),
            pa.field("system_selected_similarity_score", pa.float64()),
            pa.field("accepted_system_recommendation", pa.bool_()),
            pa.field("was_candidate_selection", pa.bool_()),
            pa.field("dwell_time_ms", pa.int64()),
            pa.field("satisfaction_score", pa.int32()),
            pa.field("recommendation_event_key", pa.string()),
            pa.field("source_bucket", pa.string()),
            pa.field("source_key", pa.string()),
            pa.field("source_line_number", pa.int64()),
            pa.field("trusted_processed_at_utc", pa.string()),
        ]
    )


def write_rejected_partition(rows: Iterable) -> None:
    rows = list(rows)
    if not rows:
        return
    s3 = build_s3_client()
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")
    rejected_prefix = env("TRUSTED_RECOMMENDATION_FEEDBACK_REJECTED_PREFIX", "recommender/feedback/rejected/")
    run_id = env("TRUSTED_RECOMMENDATION_FEEDBACK_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    key = f"{rejected_prefix.rstrip('/')}/run_id={run_id}/part-{uuid.uuid4().hex}.jsonl"
    body = "\n".join(json.dumps(row.asDict(recursive=True), ensure_ascii=False) for row in rows)
    s3.put_object(Bucket=trusted_bucket, Key=key, Body=body.encode("utf-8"), ContentType="application/x-ndjson")


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
        return {"mode": "overwrite", "rows_in_batch": len(rows), "rows_inserted": len(rows), "rows_updated": 0}
    if not rows:
        return {"mode": "merge", "rows_in_batch": 0, "rows_inserted": 0, "rows_updated": 0}
    metrics = (
        DeltaTable(delta_uri, storage_options=storage_options())
        .merge(
            source=table,
            predicate="target.feedback_id = source.feedback_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all(
            predicate=(
                "source.event_ts_utc < target.event_ts_utc OR "
                "(source.event_ts_utc = target.event_ts_utc AND source.source_key < target.source_key)"
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
    builder = SparkSession.builder.appName("trusted-recommendation-feedback-cleaning")
    executor_python = env("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12")
    builder = builder.config("spark.pyspark.python", executor_python)
    builder = builder.config("spark.executorEnv.PYSPARK_PYTHON", executor_python)
    for name in [
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "BRONZE_BUCKET",
        "TRUSTED_BUCKET",
        "TRUSTED_RECOMMENDATION_FEEDBACK_REJECTED_PREFIX",
        "TRUSTED_RECOMMENDATION_FEEDBACK_RUN_ID",
        "SPARK_EXECUTOR_PYTHON",
    ]:
        value = os.getenv(name)
        if value is not None:
            builder = builder.config(f"spark.executorEnv.{name}", value)
    builder = builder.config("spark.executorEnv.TRUSTED_RECOMMENDATION_FEEDBACK_PROCESSED_AT", processed_at)
    builder = builder.config("spark.executorEnv.TRUSTED_RECOMMENDATION_FEEDBACK_RUN_ID", run_id)
    return builder.getOrCreate()


def main() -> None:
    bronze_bucket = env("BRONZE_BUCKET", "bronze")
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")
    bronze_prefix = env("BRONZE_RECOMMENDATION_FEEDBACK_PREFIX", "persistent/recommender/feedback/raw/")
    trusted_delta_uri = env(
        "TRUSTED_RECOMMENDATION_FEEDBACK_DELTA_URI",
        f"s3://{trusted_bucket}/recommender/feedback/delta/recommendation_feedback_clean_delta",
    )
    metadata_prefix = env("TRUSTED_RECOMMENDATION_FEEDBACK_METADATA_PREFIX", "metadata/recommender/feedback/")
    manifest_key = env(
        "TRUSTED_RECOMMENDATION_FEEDBACK_MANIFEST_KEY",
        f"{metadata_prefix.rstrip('/')}/checkpoints/trusted_recommendation_feedback_processed_files.json",
    )
    max_files = int(env("TRUSTED_RECOMMENDATION_FEEDBACK_MAX_FILES", "0"))
    partitions = int(env("TRUSTED_RECOMMENDATION_FEEDBACK_SPARK_PARTITIONS", "4"))
    full_refresh = env("TRUSTED_RECOMMENDATION_FEEDBACK_FULL_REFRESH", "false").lower() == "true"

    processed_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.environ["TRUSTED_RECOMMENDATION_FEEDBACK_RUN_ID"] = run_id

    print("[Trusted][Feedback][Spark] Starting recommendation feedback validation...")
    print(f"[Trusted][Feedback][Spark] Source: s3://{bronze_bucket}/{bronze_prefix}")
    print(f"[Trusted][Feedback][Spark] Target: {trusted_delta_uri}")

    s3 = build_s3_client()
    ensure_bucket_exists(s3, trusted_bucket)

    objects = list_source_jsonl(s3, bronze_bucket, bronze_prefix, 0)
    manifest = load_processed_manifest(s3, trusted_bucket, manifest_key)
    table_exists = delta_table_exists(trusted_delta_uri)
    force_all_files = full_refresh or not table_exists
    pending_objects = select_pending_source_files(objects, manifest, max_files, force_all_files)

    if not pending_objects:
        report = {
            "run_id": run_id,
            "processed_at_utc": processed_at,
            "source": f"s3://{bronze_bucket}/{bronze_prefix}",
            "target": trusted_delta_uri,
            "status": "no_new_source_files",
            "total_source_files": len(objects),
            "raw_records": 0,
            "valid_records_written": 0,
            "invalid_records_rejected": 0,
        }
        report_key = f"{metadata_prefix.rstrip('/')}/clean_recommendation_feedback_spark_run_{run_id}.json"
        write_json(s3, trusted_bucket, report_key, report)
        print("[Trusted][Feedback][Spark] No new source files to process.")
        return

    spark = build_spark(processed_at, run_id)
    spark.sparkContext.setLogLevel("WARN")
    try:
        slices = max(1, min(partitions, len(pending_objects)))
        raw_rdd = spark.sparkContext.parallelize(pending_objects, slices).mapPartitions(read_jsonl_partition)
        raw_df = spark.createDataFrame(raw_rdd, schema=RAW_SCHEMA).cache()
        raw_count = raw_df.count()
        valid_df, rejected_df = clean_feedback(raw_df, processed_at)
        valid_df = valid_df.cache()
        rejected_df = rejected_df.cache()
        valid_before_dedup = raw_df.count() - rejected_df.count()
        valid_count = valid_df.count()
        rejected_count = rejected_df.count()
        duplicates_removed = max(0, valid_before_dedup - valid_count)

        if rejected_count > 0:
            rejected_df.foreachPartition(write_rejected_partition)

        delta_write = write_delta_from_rows(trusted_delta_uri, valid_df, full_refresh=force_all_files)
    finally:
        spark.stop()

    manifest_to_update = {"version": 1, "processed_files": {}} if full_refresh else manifest
    manifest_to_update = mark_source_files_processed(manifest_to_update, pending_objects, processed_at, run_id)
    write_json(s3, trusted_bucket, manifest_key, manifest_to_update)

    report = {
        "run_id": run_id,
        "processed_at_utc": processed_at,
        "engine": "apache_spark",
        "source": f"s3://{bronze_bucket}/{bronze_prefix}",
        "target": trusted_delta_uri,
        "total_source_files": len(objects),
        "source_files_processed": len(pending_objects),
        "raw_records": raw_count,
        "valid_records_written": delta_write["rows_in_batch"],
        "invalid_records_rejected": rejected_count,
        "duplicates_removed": duplicates_removed,
        "delta_write": delta_write,
        "valid_actions": VALID_ACTIONS,
    }
    report_key = f"{metadata_prefix.rstrip('/')}/clean_recommendation_feedback_spark_run_{run_id}.json"
    write_json(s3, trusted_bucket, report_key, report)

    print(
        "[Trusted][Feedback][Spark] "
        f"files={len(pending_objects)}/{len(objects)}, raw={raw_count}, "
        f"written={delta_write['rows_in_batch']}, rejected={rejected_count}, "
        f"duplicates_removed={duplicates_removed}, delta_mode={delta_write['mode']}"
    )
    print(f"[Trusted][Feedback][Spark] Report: s3://{trusted_bucket}/{report_key}")
    print("[Trusted][Feedback][Spark] Done.")


if __name__ == "__main__":
    main()
