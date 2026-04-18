from __future__ import annotations

import os
import re
from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable, write_deltalake


# ---------------------------------------------------------------------
# Environment / storage configuration
# ---------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ENDPOINT = MINIO_ENDPOINT.replace("http://", "").replace("https://", "").rstrip("/")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
TRUSTED_BUCKET = os.getenv("TRUSTED_BUCKET", "trusted")

BRONZE_ISRC_DELTA_URI = (
    f"s3://{BRONZE_BUCKET}/persistent/structured/musicbrainz/delta/isrc_cache_delta"
)

TRUSTED_ISRC_DELTA_URI = (
    f"s3://{TRUSTED_BUCKET}/structured/musicbrainz/delta/isrc_clean_delta"
)

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "AWS_ENDPOINT_URL": f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}",
    "AWS_ALLOW_HTTP": "false" if MINIO_SECURE else "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

ISRC_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}\d{7}$")


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
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


def normalize_isrc(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().upper().replace("-", "").replace(" ", "")
    return pd.NA if s == "" else s


def is_valid_isrc(x) -> bool:
    if pd.isna(x):
        return False
    return bool(ISRC_REGEX.match(str(x)))


def status_priority(status: str) -> int:
    """
    Lower = better
    """
    priorities = {
        "search_ok": 1,
        "ok": 2,
        "search_no_isrc": 3,
        "no_isrc": 4,
        "search_no_match": 5,
        "not_found_404": 6,
        "rate_limited_503": 7,
        "other_http_error": 8,
        "request_exception": 9,
        "search_request_exception": 10,
        "invalid_mbid": 11,
    }
    return priorities.get(str(status), 999)


def method_priority(method: str) -> int:
    """
    Lower = better
    """
    method = "" if pd.isna(method) else str(method)
    if method == "mbid":
        return 1
    if method == "search":
        return 2
    if method.startswith("mbid_then_"):
        return 3
    if method.startswith("mbid_"):
        return 4
    return 999


# ---------------------------------------------------------------------
# Cleaning logic
# ---------------------------------------------------------------------
def clean_isrc_table(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
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
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in bronze ISRC table: {missing}")

    df = df.copy()

    # Trim / null normalization
    string_cols = [
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
    ]
    for col in string_cols:
        df[col] = df[col].apply(safe_strip)

    # Numeric cast
    df = cast_numeric(df, ["search_score"])

    # Canonical field names
    df["track_mbid"] = df["lastfm_track_mbid"]
    df["artist_name"] = df["lastfm_artist_name"]
    df["track_name"] = df["lastfm_track_name"]

    # Helper normalized text
    df["artist_name_norm"] = df["artist_name"].apply(normalize_text_key)
    df["track_name_norm"] = df["track_name"].apply(normalize_text_key)

    # Normalize IDs
    df["track_mbid"] = df["track_mbid"].apply(
        lambda x: pd.NA if pd.isna(x) else str(x).strip().lower()
    )
    df["matched_recording_mbid"] = df["matched_recording_mbid"].apply(
        lambda x: pd.NA if pd.isna(x) else str(x).strip().lower()
    )
    df["resolved_mbid"] = df["resolved_mbid"].apply(
        lambda x: pd.NA if pd.isna(x) else str(x).strip().lower()
    )

    # Normalize ISRC
    df["isrc"] = df["isrc"].apply(normalize_isrc)
    df["is_valid_isrc"] = df["isrc"].apply(is_valid_isrc)

    # Identity flags
    df["has_track_identity"] = (
        df["track_mbid"].notna()
        | (
            df["artist_name"].notna()
            & df["track_name"].notna()
            & (df["artist_name_norm"] != "")
            & (df["track_name_norm"] != "")
        )
    )

    # Resolution flags
    df["has_resolution_info"] = (
        df["mb_status"].notna()
        | df["resolution_method"].notna()
    )

    df["is_valid_record"] = df["has_track_identity"] & df["has_resolution_info"]

    # Keep only records with minimum identity and status
    df = df[df["is_valid_record"]].copy()

    # Trusted join key / identity key
    df["trusted_track_key"] = df.apply(
        lambda row: (
            f"mbid::{row['track_mbid']}"
            if pd.notna(row["track_mbid"])
            else f"name::{row['artist_name_norm']}::{row['track_name_norm']}"
        ),
        axis=1,
    )

    # Ranking for deduplication
    df["status_rank"] = df["mb_status"].apply(status_priority)
    df["method_rank"] = df["resolution_method"].apply(method_priority)
    df["resolved_at_utc_dt"] = pd.to_datetime(df["resolved_at_utc"], errors="coerce", utc=True)

    # Prefer:
    # 1) valid ISRC
    # 2) best status
    # 3) best method
    # 4) highest search score
    # 5) earliest resolution timestamp as tiebreaker
    df = df.sort_values(
        by=[
            "trusted_track_key",
            "is_valid_isrc",
            "status_rank",
            "method_rank",
            "search_score",
            "resolved_at_utc_dt",
        ],
        ascending=[True, False, True, True, False, True],
        na_position="last",
    )

    df = df.drop_duplicates(subset=["trusted_track_key"], keep="first").reset_index(drop=True)

    # Extra convenience fields
    df["is_resolved"] = df["is_valid_isrc"]
    df["resolution_source"] = df["resolution_method"]

    # Trusted metadata
    df["trusted_processed_at_utc"] = datetime.now(timezone.utc).isoformat()
    df["trusted_source_zone"] = "bronze"
    df["trusted_target_zone"] = "trusted"

    final_cols = [
        "trusted_track_key",
        "track_mbid",
        "artist_name",
        "track_name",
        "artist_name_norm",
        "track_name_norm",
        "isrc",
        "is_valid_isrc",
        "is_resolved",
        "resolution_source",
        "resolution_method",
        "mb_status",
        "matched_recording_mbid",
        "resolved_mbid",
        "search_score",
        "resolved_at_utc",
        "run_id",
        "run_date",
        "trusted_processed_at_utc",
        "trusted_source_zone",
        "trusted_target_zone",
    ]
    df = df[final_cols].copy()

    # Final casts
    df = cast_nullable_string(
        df,
        [
            "trusted_track_key",
            "track_mbid",
            "artist_name",
            "track_name",
            "artist_name_norm",
            "track_name_norm",
            "isrc",
            "resolution_source",
            "resolution_method",
            "mb_status",
            "matched_recording_mbid",
            "resolved_mbid",
            "resolved_at_utc",
            "run_id",
            "run_date",
            "trusted_processed_at_utc",
            "trusted_source_zone",
            "trusted_target_zone",
        ],
    )

    if "search_score" in df.columns:
        df["search_score"] = pd.to_numeric(df["search_score"], errors="coerce")

    df["is_valid_isrc"] = df["is_valid_isrc"].astype("boolean")
    df["is_resolved"] = df["is_resolved"].astype("boolean")

    return df


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    print("[Trusted][MusicBrainz] Starting trusted-zone cleaning for ISRC mappings...")

    if not delta_table_exists(BRONZE_ISRC_DELTA_URI):
        raise RuntimeError(
            f"Bronze ISRC Delta table does not exist: {BRONZE_ISRC_DELTA_URI}"
        )

    print(f"[Trusted][MusicBrainz] Reading bronze table: {BRONZE_ISRC_DELTA_URI}")
    df_bronze = load_delta_as_df(BRONZE_ISRC_DELTA_URI)
    print(f"[Trusted][MusicBrainz] Rows read from bronze: {len(df_bronze)}")

    df_clean = clean_isrc_table(df_bronze)
    print(f"[Trusted][MusicBrainz] Rows after trusted cleaning: {len(df_clean)}")
    print(f"[Trusted][MusicBrainz] Rows with valid ISRC: {df_clean['is_valid_isrc'].fillna(False).sum()}")

    write_deltalake(
        TRUSTED_ISRC_DELTA_URI,
        df_clean,
        mode="overwrite",
        storage_options=DELTA_STORAGE_OPTIONS,
    )

    print(f"[Trusted][MusicBrainz] Trusted Delta written to: {TRUSTED_ISRC_DELTA_URI}")
    print("[Trusted][MusicBrainz] Done.")


if __name__ == "__main__":
    main()