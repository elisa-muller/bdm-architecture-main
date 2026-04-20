from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable, write_deltalake


# Environment / storage configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ENDPOINT = MINIO_ENDPOINT.replace("http://", "").replace("https://", "").rstrip("/")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
TRUSTED_BUCKET = os.getenv("TRUSTED_BUCKET", "trusted")

BRONZE_LASTFM_DELTA_URI = (
    f"s3://{BRONZE_BUCKET}/persistent/structured/lastfm/delta/tracks_delta"
)

TRUSTED_LASTFM_DELTA_URI = (
    f"s3://{TRUSTED_BUCKET}/structured/lastfm/delta/tracks_clean_delta"
)

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "AWS_ENDPOINT_URL": f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}",
    "AWS_ALLOW_HTTP": "false" if MINIO_SECURE else "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


# Helpers
def delta_table_exists(delta_uri: str) -> bool:
    try:
        DeltaTable(delta_uri, storage_options=DELTA_STORAGE_OPTIONS)
        return True
    except Exception:
        return False


def load_delta_as_df(delta_uri: str) -> pd.DataFrame:
    dt = DeltaTable(delta_uri, storage_options=DELTA_STORAGE_OPTIONS)
    return dt.to_pandas()


def safe_strip(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    return pd.NA if s == "" else s


def normalize_bool_like(x):
    if pd.isna(x):
        return pd.NA

    s = str(x).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "fulltrack"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False

    return pd.NA


def normalize_text_key(x) -> str:
    if pd.isna(x):
        return ""
    return " ".join(str(x).strip().lower().split())


def cast_nullable_string(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


def cast_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# Cleaning
def clean_lastfm_tracks(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "track_key",
        "lastfm_track_name",
        "lastfm_track_mbid",
        "lastfm_artist_name",
        "lastfm_artist_mbid",
        "lastfm_url",
        "lastfm_duration",
        "lastfm_streamable",
        "lastfm_image_url",
        "first_seen_run_id",
        "first_seen_run_date",
        "first_seen_ingested_at_utc",
        "first_seen_source_type",
        "first_seen_source_value",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in bronze Last.fm table: {missing}")

    df = df.copy()

    # Trim / normalize empties to null
    string_cols = [
        "track_key",
        "lastfm_track_name",
        "lastfm_track_mbid",
        "lastfm_artist_name",
        "lastfm_artist_mbid",
        "lastfm_url",
        "lastfm_streamable",
        "lastfm_image_url",
        "first_seen_run_id",
        "first_seen_run_date",
        "first_seen_ingested_at_utc",
        "first_seen_source_type",
        "first_seen_source_value",
    ]
    for col in string_cols:
        df[col] = df[col].apply(safe_strip)

    # Numeric casting
    df = cast_numeric(df, ["lastfm_duration"])

    # Basic standardization
    df["track_name"] = df["lastfm_track_name"]
    df["track_mbid"] = df["lastfm_track_mbid"]
    df["artist_name"] = df["lastfm_artist_name"]
    df["artist_mbid"] = df["lastfm_artist_mbid"]
    df["track_url"] = df["lastfm_url"]
    df["image_url"] = df["lastfm_image_url"]

    # Normalize streamable to boolean when possible
    df["streamable"] = df["lastfm_streamable"].apply(normalize_bool_like)

    # Create normalized helper fields for stable dedup / quality checks
    df["track_name_norm"] = df["track_name"].apply(normalize_text_key)
    df["artist_name_norm"] = df["artist_name"].apply(normalize_text_key)

    # Rebuild canonical key in trusted
    df["trusted_track_key"] = df.apply(
        lambda row: (
            f"mbid::{row['track_mbid']}"
            if pd.notna(row["track_mbid"])
            else f"name::{row['artist_name_norm']}::{row['track_name_norm']}"
        ),
        axis=1,
    )

    # Quality flags
    df["has_minimum_identity"] = (
        df["track_mbid"].notna()
        | (
            df["track_name"].notna()
            & df["artist_name"].notna()
            & (df["track_name_norm"] != "")
            & (df["artist_name_norm"] != "")
        )
    )

    df["valid_duration"] = df["lastfm_duration"].isna() | (df["lastfm_duration"] >= 0)

    df["is_valid_record"] = df["has_minimum_identity"] & df["valid_duration"]

    # Keep only valid rows in trusted
    df = df[df["is_valid_record"]].copy()

    # Deduplicate:
    # sort so earliest seen record survives for metadata consistency
    df["first_seen_ingested_at_utc_dt"] = pd.to_datetime(
        df["first_seen_ingested_at_utc"], errors="coerce", utc=True
    )
    df = df.sort_values(
        by=["trusted_track_key", "first_seen_ingested_at_utc_dt"],
        ascending=[True, True],
        na_position="last",
    )

    df = df.drop_duplicates(subset=["trusted_track_key"], keep="first").reset_index(drop=True)

    # Final curated schema
    df["trusted_processed_at_utc"] = datetime.now(timezone.utc).isoformat()
    df["trusted_source_zone"] = "bronze"
    df["trusted_target_zone"] = "trusted"

    final_cols = [
        "trusted_track_key",
        "track_name",
        "track_mbid",
        "artist_name",
        "artist_mbid",
        "track_url",
        "lastfm_duration",
        "streamable",
        "image_url",
        "track_name_norm",
        "artist_name_norm",
        "first_seen_run_id",
        "first_seen_run_date",
        "first_seen_ingested_at_utc",
        "first_seen_source_type",
        "first_seen_source_value",
        "trusted_processed_at_utc",
        "trusted_source_zone",
        "trusted_target_zone",
    ]
    df = df[final_cols].copy()

    # Rename duration to a canonical name
    df = df.rename(columns={"lastfm_duration": "duration_ms"})

    # Final casts
    df = cast_nullable_string(
        df,
        [
            "trusted_track_key",
            "track_name",
            "track_mbid",
            "artist_name",
            "artist_mbid",
            "track_url",
            "image_url",
            "track_name_norm",
            "artist_name_norm",
            "first_seen_run_id",
            "first_seen_run_date",
            "first_seen_ingested_at_utc",
            "first_seen_source_type",
            "first_seen_source_value",
            "trusted_processed_at_utc",
            "trusted_source_zone",
            "trusted_target_zone",
        ],
    )

    if "duration_ms" in df.columns:
        df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce")

    # pandas nullable boolean
    if "streamable" in df.columns:
        df["streamable"] = df["streamable"].astype("boolean")

    return df





# MAIN

def main():
    print("[Trusted][Last.fm] Starting trusted-zone cleaning for tracks...")

    if not delta_table_exists(BRONZE_LASTFM_DELTA_URI):
        raise RuntimeError(
            f"Bronze Last.fm Delta table does not exist: {BRONZE_LASTFM_DELTA_URI}"
        )

    print(f"[Trusted][Last.fm] Reading bronze table: {BRONZE_LASTFM_DELTA_URI}")
    df_bronze = load_delta_as_df(BRONZE_LASTFM_DELTA_URI)
    print(f"[Trusted][Last.fm] Rows read from bronze: {len(df_bronze)}")

    df_clean = clean_lastfm_tracks(df_bronze)
    print(f"[Trusted][Last.fm] Rows after trusted cleaning: {len(df_clean)}")

    # overwrite is fine here: trusted is rebuilt from bronze checkpoint
    write_deltalake(
        TRUSTED_LASTFM_DELTA_URI,
        df_clean,
        mode="overwrite",
        storage_options=DELTA_STORAGE_OPTIONS,
    )

    print(f"[Trusted][Last.fm] Trusted Delta written to: {TRUSTED_LASTFM_DELTA_URI}")
    print("[Trusted][Last.fm] Done.")


if __name__ == "__main__":
    main()