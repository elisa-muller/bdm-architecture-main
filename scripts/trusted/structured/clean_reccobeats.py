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
from pyspark.sql import types as T


ENV_NAMES = [
    "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_SECURE",
    "AWS_REGION", "BRONZE_BUCKET", "TRUSTED_BUCKET",
    "BRONZE_RECCOBEATS_PREFIX", "TRUSTED_RECCOBEATS_DELTA_URI",
    "TRUSTED_RECCOBEATS_REJECTED_PREFIX", "TRUSTED_METADATA_PREFIX",
    "SPARK_EXECUTOR_PYTHON",
]

REQUIRED_COLUMNS = {
    "rb_track_id", "rb_href", "rb_name", "rb_artist", "rb_isrc",
    "rb_danceability", "rb_energy", "rb_valence", "rb_tempo",
    "rb_acousticness", "rb_instrumentalness", "rb_liveness",
    "rb_loudness", "rb_speechiness", "rb_mode", "rb_key",
    "rb_time_signature", "rb_duration_ms", "run_id", "run_date",
}

FINAL_COLS = [
    "reccobeats_track_id",
    "href",
    "track_name",
    "track_name_norm",
    "artist_name",
    "artist_name_norm",
    "isrc",
    "danceability",
    "energy",
    "valence",
    "tempo",
    "acousticness",
    "instrumentalness",
    "liveness",
    "loudness",
    "speechiness",
    "mode",
    "musical_key",
    "time_signature",
    "duration_ms",
    "run_id",
    "run_date",
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
        raise ValueError(f"Missing required ReccoBeats columns: {sorted(missing)}")


def clean_reccobeats(raw_df: DataFrame, processed_at: str) -> tuple[DataFrame, DataFrame]:
    df = raw_df

    for col_name in [
        "rb_track_id", "rb_href", "rb_name", "rb_artist",
        "rb_isrc", "run_id", "run_date",
    ]:
        df = df.withColumn(col_name, clean_string(col_name))

    df = (
        df.withColumnRenamed("rb_track_id", "reccobeats_track_id")
        .withColumnRenamed("rb_href", "href")
        .withColumnRenamed("rb_name", "track_name")
        .withColumnRenamed("rb_artist", "artist_name")
        .withColumnRenamed("rb_isrc", "isrc")
        .withColumnRenamed("rb_key", "musical_key")
        .withColumn("track_name_norm", normalize_name("track_name"))
        .withColumn("artist_name_norm", normalize_name("artist_name"))
        .withColumn("isrc", F.upper(F.trim(F.col("isrc"))))
        .withColumn("danceability", F.expr("try_cast(rb_danceability as DOUBLE)"))
        .withColumn("energy", F.expr("try_cast(rb_energy as DOUBLE)"))
        .withColumn("valence", F.expr("try_cast(rb_valence as DOUBLE)"))
        .withColumn("tempo", F.expr("try_cast(rb_tempo as DOUBLE)"))
        .withColumn("acousticness", F.expr("try_cast(rb_acousticness as DOUBLE)"))
        .withColumn("instrumentalness", F.expr("try_cast(rb_instrumentalness as DOUBLE)"))
        .withColumn("liveness", F.expr("try_cast(rb_liveness as DOUBLE)"))
        .withColumn("loudness", F.expr("try_cast(rb_loudness as DOUBLE)"))
        .withColumn("speechiness", F.expr("try_cast(rb_speechiness as DOUBLE)"))
        .withColumn("mode", F.expr("try_cast(rb_mode as INT)"))
        .withColumn("musical_key", F.expr("try_cast(musical_key as INT)"))
        .withColumn("time_signature", F.expr("try_cast(rb_time_signature as INT)"))
        .withColumn("duration_ms", F.expr("try_cast(rb_duration_ms as BIGINT)"))
    )

    isrc_regex = r"^[A-Z0-9]{12}$"
    uuid_regex = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

    quality_errors: list[F.Column] = []

    add_quality_error(quality_errors, F.col("isrc").isNull(), "missing_isrc")
    add_quality_error(quality_errors, F.col("isrc").isNotNull() & ~F.col("isrc").rlike(isrc_regex), "invalid_isrc_format")
    add_quality_error(quality_errors, F.col("reccobeats_track_id").isNull(), "missing_reccobeats_track_id")
    add_quality_error(
        quality_errors,
        F.col("reccobeats_track_id").isNotNull() & ~F.col("reccobeats_track_id").rlike(uuid_regex),
        "invalid_reccobeats_track_id",
    )
    add_quality_error(quality_errors, F.col("run_id").isNull(), "missing_run_id")
    add_quality_error(quality_errors, F.col("run_date").isNull(), "missing_run_date")

    for feature in [
        "danceability", "energy", "valence", "acousticness",
        "instrumentalness", "liveness", "speechiness",
    ]:
        add_quality_error(
            quality_errors,
            F.col(feature).isNotNull() & ((F.col(feature) < 0) | (F.col(feature) > 1)),
            f"invalid_{feature}",
        )

    add_quality_error(
        quality_errors,
        F.col("tempo").isNotNull() & ((F.col("tempo") <= 0) | (F.col("tempo") > 300)),
        "invalid_tempo",
    )

    add_quality_error(
        quality_errors,
        F.col("loudness").isNotNull() & ((F.col("loudness") < -60) | (F.col("loudness") > 5)),
        "invalid_loudness",
    )

    add_quality_error(
        quality_errors,
        F.col("duration_ms").isNotNull() & (F.col("duration_ms") <= 0),
        "invalid_duration_ms",
    )

    add_quality_error(
        quality_errors,
        F.col("musical_key").isNotNull() & ((F.col("musical_key") < 0) | (F.col("musical_key") > 11)),
        "invalid_musical_key",
    )

    add_quality_error(
        quality_errors,
        F.col("mode").isNotNull() & ~F.col("mode").isin(0, 1),
        "invalid_mode",
    )

    add_quality_error(
        quality_errors,
        F.col("time_signature").isNotNull() & ((F.col("time_signature") < 1) | (F.col("time_signature") > 12)),
        "invalid_time_signature",
    )

    audio_features = [
        "danceability", "energy", "valence", "tempo", "acousticness",
        "instrumentalness", "liveness", "loudness", "speechiness",
    ]

    df = df.withColumn(
        "non_null_audio_features",
        sum(F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for c in audio_features),
    )

    add_quality_error(
        quality_errors,
        F.col("non_null_audio_features") == 0,
        "missing_all_audio_features",
    )

    df = (
        df.withColumn(
            "quality_errors",
            F.filter(F.array(*quality_errors), lambda item: item.isNotNull()),
        )
        .withColumn("is_valid_record", F.size("quality_errors") == 0)
    )

    rejected_df = df.filter(~F.col("is_valid_record")).select(
        "reccobeats_track_id",
        "isrc",
        "track_name",
        "artist_name",
        "run_id",
        "run_date",
        "quality_errors",
    )

    dedup_window = Window.partitionBy("isrc").orderBy(
        F.col("non_null_audio_features").desc(),
        F.col("run_date").desc_nulls_last(),
        F.col("run_id").desc_nulls_last(),
    )

    valid_df = (
        df.filter(F.col("is_valid_record"))
        .withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank", "non_null_audio_features")
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
    rejected_prefix = env("TRUSTED_RECCOBEATS_REJECTED_PREFIX", "structured/reccobeats/rejected/")
    run_id = env("TRUSTED_RECCOBEATS_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

    key = f"{rejected_prefix.rstrip('/')}/run_id={run_id}/part-{uuid.uuid4().hex}.jsonl"
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)

    s3.put_object(
        Bucket=trusted_bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def reccobeats_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("reccobeats_track_id", pa.string()),
            pa.field("href", pa.string()),
            pa.field("track_name", pa.string()),
            pa.field("track_name_norm", pa.string()),
            pa.field("artist_name", pa.string()),
            pa.field("artist_name_norm", pa.string()),
            pa.field("isrc", pa.string()),
            pa.field("danceability", pa.float64()),
            pa.field("energy", pa.float64()),
            pa.field("valence", pa.float64()),
            pa.field("tempo", pa.float64()),
            pa.field("acousticness", pa.float64()),
            pa.field("instrumentalness", pa.float64()),
            pa.field("liveness", pa.float64()),
            pa.field("loudness", pa.float64()),
            pa.field("speechiness", pa.float64()),
            pa.field("mode", pa.int32()),
            pa.field("musical_key", pa.int32()),
            pa.field("time_signature", pa.int32()),
            pa.field("duration_ms", pa.int64()),
            pa.field("run_id", pa.string()),
            pa.field("run_date", pa.string()),
            pa.field("trusted_processed_at_utc", pa.string()),
        ]
    )


def write_delta_from_dicts(delta_uri: str, rows: list[dict]) -> int:
    table = pa.Table.from_pylist(rows, schema=reccobeats_schema())

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
        .appName("trusted-reccobeats-audio-features-cleaning")
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

    builder = builder.config("spark.executorEnv.TRUSTED_RECCOBEATS_PROCESSED_AT", processed_at)
    builder = builder.config("spark.executorEnv.TRUSTED_RECCOBEATS_RUN_ID", run_id)

    return builder.getOrCreate()


def read_bronze_delta_as_spark(
    spark: SparkSession,
    delta_uri: str,
    partitions: int,
) -> DataFrame:
    bronze_table = DeltaTable(delta_uri, storage_options=storage_options())
    pandas_df = bronze_table.to_pyarrow_table().to_pandas()
    schema = T.StructType(
        [
            T.StructField("rb_track_id", T.StringType(), True),
            T.StructField("rb_href", T.StringType(), True),
            T.StructField("rb_name", T.StringType(), True),
            T.StructField("rb_artist", T.StringType(), True),
            T.StructField("rb_isrc", T.StringType(), True),
            T.StructField("rb_danceability", T.DoubleType(), True),
            T.StructField("rb_energy", T.DoubleType(), True),
            T.StructField("rb_valence", T.DoubleType(), True),
            T.StructField("rb_tempo", T.DoubleType(), True),
            T.StructField("rb_acousticness", T.DoubleType(), True),
            T.StructField("rb_instrumentalness", T.DoubleType(), True),
            T.StructField("rb_liveness", T.DoubleType(), True),
            T.StructField("rb_loudness", T.DoubleType(), True),
            T.StructField("rb_speechiness", T.DoubleType(), True),
            T.StructField("rb_mode", T.StringType(), True),
            T.StructField("rb_key", T.StringType(), True),
            T.StructField("rb_time_signature", T.DoubleType(), True),
            T.StructField("rb_duration_ms", T.DoubleType(), True),
            T.StructField("run_id", T.StringType(), True),
            T.StructField("run_date", T.StringType(), True),
        ]
    )
    return spark.createDataFrame(pandas_df, schema=schema).repartition(partitions)


def main() -> None:
    bronze_bucket = env("BRONZE_BUCKET", "bronze")
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")

    bronze_prefix = env(
        "BRONZE_RECCOBEATS_PREFIX",
        "persistent/structured/reccobeats/delta/audio_features_delta/",
    )

    bronze_delta_uri = f"s3://{bronze_bucket}/{bronze_prefix.rstrip('/')}"

    trusted_delta_uri = env(
        "TRUSTED_RECCOBEATS_DELTA_URI",
        f"s3://{trusted_bucket}/structured/reccobeats/delta/audio_features_clean_delta",
    )

    metadata_prefix = env("TRUSTED_METADATA_PREFIX", "metadata/structured/reccobeats/")
    partitions = int(env("TRUSTED_RECCOBEATS_SPARK_PARTITIONS", "2"))

    processed_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    os.environ["TRUSTED_RECCOBEATS_RUN_ID"] = run_id
    os.environ["TRUSTED_RECCOBEATS_PROCESSED_AT"] = processed_at

    print("[Trusted][ReccoBeats][Spark] Starting audio feature cleaning...")
    print(f"[Trusted][ReccoBeats][Spark] Source: {bronze_delta_uri}")
    print(f"[Trusted][ReccoBeats][Spark] Target: {trusted_delta_uri}")

    s3 = build_s3_client()
    ensure_bucket_exists(s3, trusted_bucket)

    raw_count = 0
    rejected_count = 0
    duplicates_removed = 0
    rows_written = 0

    spark = build_spark(processed_at, run_id)
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = read_bronze_delta_as_spark(
            spark=spark,
            delta_uri=bronze_delta_uri,
            partitions=partitions,
        )

        validate_schema(raw_df)

        raw_count = raw_df.count()

        valid_df, rejected_df = clean_reccobeats(raw_df, processed_at)

        valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
        rejected_rows = [row.asDict(recursive=True) for row in rejected_df.collect()]

        rejected_count = len(rejected_rows)
        duplicates_removed = max(0, raw_count - rejected_count - len(valid_rows))

        write_rejected_rows(rejected_rows)
        rows_written = write_delta_from_dicts(trusted_delta_uri, valid_rows)

    finally:
        try:
            spark.stop()
        except Exception as e:
            print(f"[Trusted][ReccoBeats][Spark] Warning: Spark stop failed: {e}")

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

    report_key = f"{metadata_prefix.rstrip('/')}/clean_reccobeats_audio_features_spark_run_{run_id}.json"
    write_json(s3, trusted_bucket, report_key, report)

    print(
        "[Trusted][ReccoBeats][Spark] "
        f"raw={raw_count}, written={rows_written}, rejected={rejected_count}, "
        f"duplicates_removed={duplicates_removed}"
    )
    print(f"[Trusted][ReccoBeats][Spark] Report: s3://{trusted_bucket}/{report_key}")
    print("[Trusted][ReccoBeats][Spark] Done.")


if __name__ == "__main__":
    main()
