from __future__ import annotations

import json
import os
import re
import unicodedata
import uuid
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


ENV_NAMES = [
    "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_SECURE",
    "AWS_REGION", "LANDING_BUCKET", "TRUSTED_BUCKET",
    "LANDING_MUSICBRAINZ_PREFIX", "TRUSTED_MUSICBRAINZ_DELTA_URI",
    "TRUSTED_MUSICBRAINZ_REJECTED_PREFIX", "TRUSTED_METADATA_PREFIX",
    "SPARK_EXECUTOR_PYTHON",
]

REQUIRED_COLUMNS = {
    "lastfm_track_mbid",
    "lastfm_artist_name",
    "lastfm_track_name",
    "resolution_method",
    "matched_recording_mbid",
    "search_score",
    "resolved_mbid",
    "isrc",
    "mb_status",
    "resolved_at_utc",
    "run_id",
    "run_date",
}

FINAL_COLS = [
    "track_mbid",
    "artist_name",
    "artist_name_norm",
    "track_name",
    "track_name_norm",
    "resolution_method",
    "resolution_status",
    "matched_recording_mbid",
    "resolved_mbid",
    "search_score",
    "isrc",
    "run_id",
    "run_date",
    "resolved_at_utc",
    "trusted_processed_at_utc",
]


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is not None:
        return value
    if name.startswith("LANDING_"):
        legacy_value = os.getenv(name.replace("LANDING_", "BRONZE_", 1))
        if legacy_value is not None:
            return legacy_value
    return default


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
        raise ValueError(f"Missing required MusicBrainz columns: {sorted(missing)}")


def clean_string_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.mask(cleaned == "")
    cleaned = cleaned.mask(cleaned.str.lower().isin(["nan", "none", "null"]))
    return cleaned.astype(object).where(cleaned.notna(), None)


def normalize_name_value(value: object) -> object:
    if value is None or pd.isna(value):
        return None

    raw = str(value).strip()
    decomposed = unicodedata.normalize("NFKD", raw)
    ascii_name = "".join(char for char in decomposed if not unicodedata.combining(char))
    normalized = ascii_name.lower()
    normalized = re.sub(r"[^a-z0-9\s]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or raw


def add_quality_error_pandas(errors: list[str], condition: bool, label: str) -> None:
    if condition:
        errors.append(label)


def clean_musicbrainz_pandas(raw_df: pd.DataFrame, processed_at: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = raw_df.copy()

    for col_name in [
        "lastfm_track_mbid",
        "lastfm_artist_name",
        "lastfm_track_name",
        "resolution_method",
        "matched_recording_mbid",
        "resolved_mbid",
        "isrc",
        "mb_status",
        "resolved_at_utc",
        "run_id",
        "run_date",
    ]:
        df[col_name] = clean_string_series(df[col_name])

    df = df.rename(
        columns={
            "lastfm_track_mbid": "track_mbid",
            "lastfm_artist_name": "artist_name",
            "lastfm_track_name": "track_name",
            "mb_status": "resolution_status",
        }
    )

    df["artist_name_norm"] = df["artist_name"].map(normalize_name_value)
    df["track_name_norm"] = df["track_name"].map(normalize_name_value)
    df["search_score"] = pd.to_numeric(df["search_score"], errors="coerce")
    df["isrc"] = df["isrc"].map(lambda value: str(value).strip().upper() if value is not None else None)
    df["isrc"] = clean_string_series(df["isrc"])

    uuid_regex = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
    isrc_regex = re.compile(r"^[A-Z0-9]{12}$")
    expected_status_by_method = {
        "mbid": "ok",
        "search": "search_ok",
        "mbid_no_isrc": "no_isrc",
        "mbid_then_search_no_isrc": "search_no_isrc",
        "mbid_then_search_no_match": "search_no_match",
        "mbid_then_search_request_exception": "search_request_exception",
    }

    def row_errors(row: pd.Series) -> list[str]:
        errors: list[str] = []

        add_quality_error_pandas(errors, row["track_mbid"] is None, "missing_track_mbid")
        add_quality_error_pandas(errors, row["track_name"] is None, "missing_track_name")
        add_quality_error_pandas(errors, row["artist_name"] is None, "missing_artist_name")
        add_quality_error_pandas(errors, row["resolution_method"] is None, "missing_resolution_method")
        add_quality_error_pandas(errors, row["resolution_status"] is None, "missing_resolution_status")
        add_quality_error_pandas(errors, row["run_id"] is None, "missing_run_id")
        add_quality_error_pandas(errors, row["run_date"] is None, "missing_run_date")
        add_quality_error_pandas(errors, row["resolved_at_utc"] is None, "missing_resolved_at")

        add_quality_error_pandas(
            errors,
            row["track_mbid"] is not None and not uuid_regex.match(str(row["track_mbid"])),
            "invalid_track_mbid",
        )
        add_quality_error_pandas(
            errors,
            row["matched_recording_mbid"] is not None
            and not uuid_regex.match(str(row["matched_recording_mbid"])),
            "invalid_matched_recording_mbid",
        )
        add_quality_error_pandas(
            errors,
            row["resolved_mbid"] is not None and not uuid_regex.match(str(row["resolved_mbid"])),
            "invalid_resolved_mbid",
        )
        add_quality_error_pandas(
            errors,
            row["isrc"] is not None and not isrc_regex.match(str(row["isrc"])),
            "invalid_isrc_format",
        )
        add_quality_error_pandas(
            errors,
            pd.notna(row["search_score"]) and (row["search_score"] < 0 or row["search_score"] > 100),
            "invalid_search_score",
        )

        expected_status = expected_status_by_method.get(row["resolution_method"])
        add_quality_error_pandas(
            errors,
            expected_status is not None and row["resolution_status"] != expected_status,
            "inconsistent_resolution_status",
        )

        return errors

    df["quality_errors"] = df.apply(row_errors, axis=1)
    df["is_valid_record"] = df["quality_errors"].map(len).eq(0)

    rejected_df = df.loc[~df["is_valid_record"], [
        "track_mbid",
        "track_name",
        "artist_name",
        "resolution_method",
        "resolution_status",
        "matched_recording_mbid",
        "resolved_mbid",
        "search_score",
        "isrc",
        "run_id",
        "run_date",
        "quality_errors",
    ]].copy()

    valid_df = df.loc[df["is_valid_record"]].copy()
    valid_df["_has_isrc_rank"] = valid_df["isrc"].notna().map({True: 0, False: 1})
    valid_df["_status_rank"] = valid_df["resolution_status"].isin(["ok", "search_ok"]).map({True: 0, False: 1})
    valid_df["_search_score_sort"] = valid_df["search_score"].fillna(float("-inf"))
    valid_df = valid_df.sort_values(
        by=["track_mbid", "_has_isrc_rank", "_status_rank", "_search_score_sort", "resolved_at_utc", "run_id"],
        ascending=[True, True, True, False, True, True],
        na_position="last",
    )
    valid_df = valid_df.drop_duplicates(subset=["track_mbid"], keep="first")
    valid_df["trusted_processed_at_utc"] = processed_at

    return valid_df[FINAL_COLS].copy(), rejected_df


def clean_musicbrainz(raw_df: DataFrame, processed_at: str) -> tuple[DataFrame, DataFrame]:
    df = raw_df

    for col_name in [
        "lastfm_track_mbid",
        "lastfm_artist_name",
        "lastfm_track_name",
        "resolution_method",
        "matched_recording_mbid",
        "resolved_mbid",
        "isrc",
        "mb_status",
        "resolved_at_utc",
        "run_id",
        "run_date",
    ]:
        df = df.withColumn(col_name, clean_string(col_name))

    df = (
        df.withColumnRenamed("lastfm_track_mbid", "track_mbid")
        .withColumnRenamed("lastfm_artist_name", "artist_name")
        .withColumnRenamed("lastfm_track_name", "track_name")
        .withColumnRenamed("mb_status", "resolution_status")
        .withColumn("artist_name_norm", normalize_name("artist_name"))
        .withColumn("track_name_norm", normalize_name("track_name"))
        .withColumn("search_score", F.expr("try_cast(search_score as DOUBLE)"))
        .withColumn(
            "search_score",
            F.when(F.isnan(F.col("search_score")), F.lit(None)).otherwise(F.col("search_score")),
        )
        .withColumn("isrc", F.upper(F.trim(F.col("isrc"))))
    )

    uuid_regex = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    isrc_regex = r"^[A-Z0-9]{12}$"

    expected_status = (
        F.when(F.col("resolution_method") == "mbid", F.lit("ok"))
        .when(F.col("resolution_method") == "search", F.lit("search_ok"))
        .when(F.col("resolution_method") == "mbid_no_isrc", F.lit("no_isrc"))
        .when(F.col("resolution_method") == "mbid_then_search_no_isrc", F.lit("search_no_isrc"))
        .when(F.col("resolution_method") == "mbid_then_search_no_match", F.lit("search_no_match"))
        .when(
            F.col("resolution_method") == "mbid_then_search_request_exception",
            F.lit("search_request_exception"),
        )
        .otherwise(F.lit(None))
    )

    quality_errors: list[F.Column] = []

    add_quality_error(quality_errors, F.col("track_mbid").isNull(), "missing_track_mbid")
    add_quality_error(quality_errors, F.col("track_name").isNull(), "missing_track_name")
    add_quality_error(quality_errors, F.col("artist_name").isNull(), "missing_artist_name")
    add_quality_error(quality_errors, F.col("resolution_method").isNull(), "missing_resolution_method")
    add_quality_error(quality_errors, F.col("resolution_status").isNull(), "missing_resolution_status")
    add_quality_error(quality_errors, F.col("run_id").isNull(), "missing_run_id")
    add_quality_error(quality_errors, F.col("run_date").isNull(), "missing_run_date")
    add_quality_error(quality_errors, F.col("resolved_at_utc").isNull(), "missing_resolved_at")

    add_quality_error(
        quality_errors,
        F.col("track_mbid").isNotNull() & ~F.col("track_mbid").rlike(uuid_regex),
        "invalid_track_mbid",
    )

    add_quality_error(
        quality_errors,
        F.col("matched_recording_mbid").isNotNull()
        & ~F.col("matched_recording_mbid").rlike(uuid_regex),
        "invalid_matched_recording_mbid",
    )

    add_quality_error(
        quality_errors,
        F.col("resolved_mbid").isNotNull()
        & ~F.col("resolved_mbid").rlike(uuid_regex),
        "invalid_resolved_mbid",
    )

    add_quality_error(
        quality_errors,
        F.col("isrc").isNotNull()
        & ~F.col("isrc").rlike(isrc_regex),
        "invalid_isrc_format",
    )

    add_quality_error(
        quality_errors,
        F.col("search_score").isNotNull()
        & ((F.col("search_score") < 0) | (F.col("search_score") > 100)),
        "invalid_search_score",
    )

    add_quality_error(
        quality_errors,
        expected_status.isNotNull()
        & (F.col("resolution_status") != expected_status),
        "inconsistent_resolution_status",
    )

    df = (
        df.withColumn(
            "quality_errors",
            F.filter(F.array(*quality_errors), lambda item: item.isNotNull()),
        )
        .withColumn("is_valid_record", F.size("quality_errors") == 0)
    )

    rejected_df = df.filter(~F.col("is_valid_record")).select(
        "track_mbid",
        "track_name",
        "artist_name",
        "resolution_method",
        "resolution_status",
        "matched_recording_mbid",
        "resolved_mbid",
        "search_score",
        "isrc",
        "run_id",
        "run_date",
        "quality_errors",
    )

    dedup_window = Window.partitionBy("track_mbid").orderBy(
        F.when(F.col("isrc").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
        F.when(F.col("resolution_status").isin("ok", "search_ok"), F.lit(0)).otherwise(F.lit(1)),
        F.col("search_score").desc_nulls_last(),
        F.col("resolved_at_utc").asc_nulls_last(),
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
    rejected_prefix = env(
        "TRUSTED_MUSICBRAINZ_REJECTED_PREFIX",
        "structured/musicbrainz/rejected/",
    )
    run_id = env(
        "TRUSTED_MUSICBRAINZ_RUN_ID",
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
    )

    key = f"{rejected_prefix.rstrip('/')}/run_id={run_id}/part-{uuid.uuid4().hex}.jsonl"
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)

    s3.put_object(
        Bucket=trusted_bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def musicbrainz_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("track_mbid", pa.string()),
            pa.field("artist_name", pa.string()),
            pa.field("artist_name_norm", pa.string()),
            pa.field("track_name", pa.string()),
            pa.field("track_name_norm", pa.string()),
            pa.field("resolution_method", pa.string()),
            pa.field("resolution_status", pa.string()),
            pa.field("matched_recording_mbid", pa.string()),
            pa.field("resolved_mbid", pa.string()),
            pa.field("search_score", pa.float64()),
            pa.field("isrc", pa.string()),
            pa.field("run_id", pa.string()),
            pa.field("run_date", pa.string()),
            pa.field("resolved_at_utc", pa.string()),
            pa.field("trusted_processed_at_utc", pa.string()),
        ]
    )


def write_delta_from_dicts(delta_uri: str, rows: list[dict]) -> int:
    table = pa.Table.from_pylist(rows, schema=musicbrainz_schema())
    partition_by: list[str] | None = ["run_date"]

    try:
        existing_table = DeltaTable(delta_uri, storage_options=storage_options())
        existing_partitions = existing_table.metadata().partition_columns
        partition_by = existing_partitions if existing_partitions else None
    except Exception:
        partition_by = ["run_date"]

    write_deltalake(
        delta_uri,
        table,
        mode="overwrite",
        partition_by=partition_by,
        storage_options=storage_options(),
    )

    return len(rows)


def read_landing_delta_as_pandas(delta_uri: str) -> pd.DataFrame:
    return DeltaTable(delta_uri, storage_options=storage_options()).to_pandas()


def dataframe_to_records(df: pd.DataFrame) -> list[dict]:
    cleaned = df.astype(object).where(pd.notna(df), None)
    return cleaned.to_dict(orient="records")


def build_spark(processed_at: str, run_id: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("trusted-musicbrainz-isrc-cleaning")
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

    builder = builder.config("spark.executorEnv.TRUSTED_MUSICBRAINZ_PROCESSED_AT", processed_at)
    builder = builder.config("spark.executorEnv.TRUSTED_MUSICBRAINZ_RUN_ID", run_id)

    return builder.getOrCreate()


def read_landing_delta_as_spark(
    spark: SparkSession,
    delta_uri: str,
    partitions: int,
) -> DataFrame:
    landing_table = DeltaTable(delta_uri, storage_options=storage_options())
    pandas_df = landing_table.to_pyarrow_table().to_pandas()
    return spark.createDataFrame(pandas_df).repartition(partitions)


def main() -> None:
    landing_bucket = env("LANDING_BUCKET", "landing")
    trusted_bucket = env("TRUSTED_BUCKET", "trusted")

    landing_prefix = env(
        "LANDING_MUSICBRAINZ_PREFIX",
        "persistent/structured/musicbrainz/delta/isrc_cache_delta/",
    )

    landing_delta_uri = f"s3://{landing_bucket}/{landing_prefix.rstrip('/')}"

    trusted_delta_uri = env(
        "TRUSTED_MUSICBRAINZ_DELTA_URI",
        f"s3://{trusted_bucket}/structured/musicbrainz/delta/isrc_clean_delta",
    )

    metadata_prefix = env("TRUSTED_METADATA_PREFIX", "metadata/structured/musicbrainz/")
    partitions = int(env("TRUSTED_MUSICBRAINZ_SPARK_PARTITIONS", "2"))

    processed_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    os.environ["TRUSTED_MUSICBRAINZ_RUN_ID"] = run_id
    os.environ["TRUSTED_MUSICBRAINZ_PROCESSED_AT"] = processed_at

    print("[Trusted][MusicBrainz] Starting ISRC cleaning...")
    print(f"[Trusted][MusicBrainz] Source: {landing_delta_uri}")
    print(f"[Trusted][MusicBrainz] Target: {trusted_delta_uri}")

    s3 = build_s3_client()
    ensure_bucket_exists(s3, trusted_bucket)

    raw_count = 0
    rejected_count = 0
    duplicates_removed = 0
    rows_written = 0

    raw_df = read_landing_delta_as_pandas(landing_delta_uri)
    validate_schema(raw_df)

    raw_count = len(raw_df)

    valid_df, rejected_df = clean_musicbrainz_pandas(raw_df, processed_at)

    valid_rows = dataframe_to_records(valid_df)
    rejected_rows = dataframe_to_records(rejected_df)

    rejected_count = len(rejected_rows)
    duplicates_removed = max(0, raw_count - rejected_count - len(valid_rows))

    write_rejected_rows(rejected_rows)
    rows_written = write_delta_from_dicts(trusted_delta_uri, valid_rows)

    report = {
        "run_id": run_id,
        "processed_at_utc": processed_at,
        "engine": "pandas_deltalake",
        "source": landing_delta_uri,
        "target": trusted_delta_uri,
        "raw_records": raw_count,
        "valid_records_written": rows_written,
        "invalid_records_rejected": rejected_count,
        "duplicates_removed": duplicates_removed,
        "spark_partitions": partitions,
        "required_columns": sorted(REQUIRED_COLUMNS),
        "final_columns": FINAL_COLS,
    }

    report_key = f"{metadata_prefix.rstrip('/')}/clean_musicbrainz_isrc_run_{run_id}.json"
    write_json(s3, trusted_bucket, report_key, report)

    print(
        "[Trusted][MusicBrainz] "
        f"raw={raw_count}, written={rows_written}, rejected={rejected_count}, "
        f"duplicates_removed={duplicates_removed}"
    )
    print(f"[Trusted][MusicBrainz] Report: s3://{trusted_bucket}/{report_key}")
    print("[Trusted][MusicBrainz] Done.")


if __name__ == "__main__":
    main()
