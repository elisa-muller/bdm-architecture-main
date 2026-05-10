from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import boto3
import pyarrow as pa
from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


ENV_NAMES = [
    "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_SECURE",
    "AWS_REGION", "BRONZE_BUCKET", "TRUSTED_BUCKET",
    "BRONZE_TRACKS_PREFIX", "TRUSTED_TRACKS_DELTA_URI",
    "TRUSTED_TRACKS_REJECTED_PREFIX", "TRUSTED_METADATA_PREFIX",
    "SPARK_EXECUTOR_PYTHON",
]

REQUIRED_COLUMNS = {
    "run_id", "run_date", "ingested_at_utc", "source_type", "source_value",
    "source_page", "lastfm_track_name", "lastfm_track_mbid",
    "lastfm_artist_name", "lastfm_artist_mbid", "lastfm_url",
    "lastfm_duration", "lastfm_image_url",
}

FINAL_COLS = [
    "trusted_track_key",
    "track_mbid",
    "artist_mbid",
    "track_name",
    "track_name_norm",
    "artist_name",
    "artist_name_norm",
    "duration_seconds",
    "url",
    "image_url",
    "source_type",
    "source_value",
    "source_page",
    "run_id",
    "run_date",
    "ingested_at_utc",
    "trusted_processed_at_utc",
]


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def minio_endpoint_url() -> str:
    endpoint = env("MINIO_ENDPOINT", "http://minio:9000")
    if not endpoint.startswith("http"):
        endpoint = "http://" + endpoint
    return endpoint.rstrip("/")


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint_url(),
        aws_access_key_id=env("MINIO_ACCESS_KEY", env("MINIO_ROOT_USER", "minioadmin")),
        aws_secret_access_key=env("MINIO_SECRET_KEY", env("MINIO_ROOT_PASSWORD", "minioadmin")),
    )


def ensure_bucket_exists(s3, bucket_name: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)


def storage_options() -> dict[str, str]:
    endpoint = minio_endpoint_url()
    secure = endpoint.startswith("https://")
    return {
        "AWS_ACCESS_KEY_ID": env("MINIO_ACCESS_KEY", env("MINIO_ROOT_USER", "minioadmin")),
        "AWS_SECRET_ACCESS_KEY": env("MINIO_SECRET_KEY", env("MINIO_ROOT_PASSWORD", "minioadmin")),
        "AWS_REGION": env("AWS_REGION", "us-east-1"),
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "false" if secure else "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def clean_string(column: str) -> F.Column:
    value = F.trim(F.col(column).cast("string"))
    lower_value = F.lower(value)

    return (
        F.when(value == "", F.lit(None))
        .when(lower_value.isin("nan", "none", "null"), F.lit(None))
        .otherwise(value)
    )


def normalize_name(column: str) -> F.Column:
    raw = clean_string(column)

    has_non_roman_script = raw.rlike(
        r"[\p{InHiragana}\p{InKatakana}\p{InCJK_Unified_Ideographs}\p{InHangul_Syllables}]"
    )

    col = F.lower(F.trim(raw))
    col = F.regexp_replace(col, r"[àáâãäå]", "a")
    col = F.regexp_replace(col, r"[èéêë]", "e")
    col = F.regexp_replace(col, r"[ìíîï]", "i")
    col = F.regexp_replace(col, r"[òóôõö]", "o")
    col = F.regexp_replace(col, r"[ùúûü]", "u")
    col = F.regexp_replace(col, r"ñ", "n")
    col = F.regexp_replace(col, r"ç", "c")
    col = F.regexp_replace(col, r"[^a-z0-9\s]", "")
    col = F.regexp_replace(col, r"\s+", " ")
    col = F.trim(col)

    return (
        F.when(raw.isNull(), F.lit(None))
        .when(has_non_roman_script, raw)
        .when(col == "", raw)
        .otherwise(col)
    )


def add_quality_error(errors: list[F.Column], condition: F.Column, label: str) -> None:
    errors.append(F.when(condition, F.lit(label)))


def validate_schema(df: DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required bronze columns: {sorted(missing)}")


def clean_tracks(raw_df: DataFrame, processed_at: str) -> tuple[DataFrame, DataFrame]:
    df = raw_df

    for col_name in [
        "run_id", "run_date", "ingested_at_utc", "source_type", "source_value",
        "lastfm_track_name", "lastfm_track_mbid", "lastfm_artist_name",
        "lastfm_artist_mbid", "lastfm_url", "lastfm_image_url",
    ]:
        df = df.withColumn(col_name, clean_string(col_name))

    df = (
        df.withColumn("duration_seconds", F.expr("try_cast(lastfm_duration as BIGINT)"))
        .withColumnRenamed("lastfm_track_name", "track_name")
        .withColumnRenamed("lastfm_track_mbid", "track_mbid")
        .withColumnRenamed("lastfm_artist_name", "artist_name")
        .withColumnRenamed("lastfm_artist_mbid", "artist_mbid")
        .withColumnRenamed("lastfm_url", "url")
        .withColumnRenamed("lastfm_image_url", "image_url")
        .withColumn("track_name_norm", normalize_name("track_name"))
        .withColumn("artist_name_norm", normalize_name("artist_name"))
    )

    df = df.withColumn(
        "trusted_track_key",
        F.when(
            F.col("track_mbid").isNotNull(),
            F.concat(F.lit("mbid::"), F.col("track_mbid")),
        )
        .when(
            F.col("artist_name_norm").isNotNull() & F.col("track_name_norm").isNotNull(),
            F.concat(
                F.lit("name_norm::"),
                F.col("artist_name_norm"),
                F.lit("::"),
                F.col("track_name_norm"),
            ),
        )
        .when(
            F.col("artist_name").isNotNull() & F.col("track_name").isNotNull(),
            F.concat(
                F.lit("name_raw::"),
                F.lower(F.trim(F.col("artist_name"))),
                F.lit("::"),
                F.lower(F.trim(F.col("track_name"))),
            ),
        )
        .otherwise(F.lit(None)),
    )

    quality_errors: list[F.Column] = []
    add_quality_error(quality_errors, F.col("trusted_track_key").isNull(), "missing_identity")
    add_quality_error(quality_errors, F.col("track_name").isNull(), "missing_track_name")
    add_quality_error(quality_errors, F.col("artist_name").isNull(), "missing_artist_name")
    add_quality_error(quality_errors, F.col("run_id").isNull(), "missing_run_id")
    add_quality_error(quality_errors, F.col("run_date").isNull(), "missing_run_date")
    add_quality_error(quality_errors, F.col("ingested_at_utc").isNull(), "missing_ingested_at")
    add_quality_error(quality_errors, F.col("source_type").isNull(), "missing_source_type")

    add_quality_error(
        quality_errors,
        F.col("duration_seconds").isNotNull() & (F.col("duration_seconds") < 0),
        "negative_duration",
    )

    df = (
        df.withColumn(
            "quality_errors",
            F.filter(F.array(*quality_errors), lambda item: item.isNotNull()),
        )
        .withColumn("is_valid_record", F.size("quality_errors") == 0)
    )

    rejected_df = df.filter(~F.col("is_valid_record")).select(
        "run_id",
        "run_date",
        "track_mbid",
        "track_name",
        "artist_name",
        "source_type",
        "source_value",
        "quality_errors",
    )

    dedup_window = Window.partitionBy("trusted_track_key").orderBy(
        F.col("ingested_at_utc").asc_nulls_last(),
        F.col("run_id").asc_nulls_last(),
    )

    valid_df = (
        df.filter(F.col("is_valid_record"))
        .withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
        .withColumn("trusted_processed_at_utc", F.lit(processed_at))
    )

    return valid_df.select(FINAL_COLS), rejected_df


def write_json(s3, bucket_name: str, key: str, payload: dict) -> None:
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def write_rejected_rows(rows: list[dict]) -> None:
    if not rows:
        return

    s3 = build_s3_client()
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")
    rejected_prefix = env("TRUSTED_TRACKS_REJECTED_PREFIX", "structured/lastfm/rejected/")
    run_id = env("TRUSTED_TRACKS_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

    key = f"{rejected_prefix.rstrip('/')}/run_id={run_id}/part-{uuid.uuid4().hex}.jsonl"
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)

    s3.put_object(
        Bucket=trusted_bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def track_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("trusted_track_key", pa.string()),
            pa.field("track_mbid", pa.string()),
            pa.field("artist_mbid", pa.string()),
            pa.field("track_name", pa.string()),
            pa.field("track_name_norm", pa.string()),
            pa.field("artist_name", pa.string()),
            pa.field("artist_name_norm", pa.string()),
            pa.field("duration_seconds", pa.int64()),
            pa.field("url", pa.string()),
            pa.field("image_url", pa.string()),
            pa.field("source_type", pa.string()),
            pa.field("source_value", pa.string()),
            pa.field("source_page", pa.int64()),
            pa.field("run_id", pa.string()),
            pa.field("run_date", pa.string()),
            pa.field("ingested_at_utc", pa.string()),
            pa.field("trusted_processed_at_utc", pa.string()),
        ]
    )


def write_delta_from_dicts(delta_uri: str, rows: list[dict]) -> int:
    table = pa.Table.from_pylist(rows, schema=track_schema())

    write_deltalake(
        delta_uri,
        table,
        mode="overwrite",
        partition_by=["run_date"],
        storage_options=storage_options(),
    )

    return len(rows)


def build_spark(processed_at: str, run_id: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("trusted-lastfm-tracks-cleaning")
        .config("spark.driver.memory", env("SPARK_DRIVER_MEMORY", "512m"))
        .config("spark.executor.memory", env("SPARK_EXECUTOR_MEMORY", "512m"))
        .config("spark.sql.shuffle.partitions", env("SPARK_SQL_SHUFFLE_PARTITIONS", "2"))
    )

    executor_python = env("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12")
    builder = builder.config("spark.pyspark.python", executor_python)
    builder = builder.config("spark.executorEnv.PYSPARK_PYTHON", executor_python)

    for name in ENV_NAMES:
        value = os.getenv(name)
        if value is not None:
            builder = builder.config(f"spark.executorEnv.{name}", value)

    builder = builder.config("spark.executorEnv.TRUSTED_TRACKS_PROCESSED_AT", processed_at)
    builder = builder.config("spark.executorEnv.TRUSTED_TRACKS_RUN_ID", run_id)

    return builder.getOrCreate()


def read_bronze_delta_as_spark(spark: SparkSession, delta_uri: str, partitions: int) -> DataFrame:
    bronze_table = DeltaTable(delta_uri, storage_options=storage_options())
    pandas_df = bronze_table.to_pyarrow_table().to_pandas()
    return spark.createDataFrame(pandas_df).repartition(partitions)


def main() -> None:
    bronze_bucket = env("BRONZE_BUCKET", "bronze")
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")

    bronze_prefix = env("BRONZE_TRACKS_PREFIX", "persistent/structured/lastfm/delta/tracks_delta/")
    bronze_delta_uri = f"s3://{bronze_bucket}/{bronze_prefix.rstrip('/')}"

    trusted_delta_uri = env(
        "TRUSTED_TRACKS_DELTA_URI",
        f"s3://{trusted_bucket}/structured/lastfm/delta/tracks_clean_delta",
    )

    metadata_prefix = env("TRUSTED_METADATA_PREFIX", "metadata/structured/lastfm/")
    partitions = int(env("TRUSTED_TRACKS_SPARK_PARTITIONS", "2"))

    processed_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    os.environ["TRUSTED_TRACKS_RUN_ID"] = run_id
    os.environ["TRUSTED_TRACKS_PROCESSED_AT"] = processed_at

    print("[Trusted][Last.fm][Spark] Starting track cleaning...")
    print(f"[Trusted][Last.fm][Spark] Source: {bronze_delta_uri}")
    print(f"[Trusted][Last.fm][Spark] Target: {trusted_delta_uri}")

    s3 = build_s3_client()
    ensure_bucket_exists(s3, trusted_bucket)

    raw_count = 0
    valid_count = 0
    rejected_count = 0
    duplicates_removed = 0
    rows_written = 0

    spark = build_spark(processed_at, run_id)
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = read_bronze_delta_as_spark(spark, bronze_delta_uri, partitions)
        validate_schema(raw_df)

        raw_count = raw_df.count()

        valid_df, rejected_df = clean_tracks(raw_df, processed_at)

        valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
        rejected_rows = [row.asDict(recursive=True) for row in rejected_df.collect()]

        valid_count = len(valid_rows)
        rejected_count = len(rejected_rows)
        duplicates_removed = max(0, raw_count - rejected_count - valid_count)

        write_rejected_rows(rejected_rows)
        rows_written = write_delta_from_dicts(trusted_delta_uri, valid_rows)

    finally:
        try:
            spark.stop()
        except Exception as e:
            print(f"[Trusted][Last.fm][Spark] Warning: Spark stop failed: {e}")

    report = {
        "run_id": run_id,
        "processed_at_utc": processed_at,
        "engine": "apache_spark_python_deltalake",
        "source": bronze_delta_uri,
        "target": trusted_delta_uri,
        "raw_records": raw_count,
        "valid_records_written": rows_written,
        "invalid_records_rejected": rejected_count,
        "duplicates_removed": duplicates_removed,
        "spark_partitions": partitions,
        "required_columns": sorted(REQUIRED_COLUMNS),
        "final_columns": FINAL_COLS,
    }

    report_key = f"{metadata_prefix.rstrip('/')}/clean_tracks_spark_run_{run_id}.json"
    write_json(s3, trusted_bucket, report_key, report)

    print(
        "[Trusted][Last.fm][Spark] "
        f"raw={raw_count}, written={rows_written}, rejected={rejected_count}, "
        f"duplicates_removed={duplicates_removed}"
    )
    print(f"[Trusted][Last.fm][Spark] Report: s3://{trusted_bucket}/{report_key}")
    print("[Trusted][Last.fm][Spark] Done.")


if __name__ == "__main__":
    main()