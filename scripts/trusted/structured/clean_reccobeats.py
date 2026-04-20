from __future__ import annotations

import os
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

BRONZE_RECCOBEATS_DELTA_URI = (
    f"s3://{BRONZE_BUCKET}/persistent/structured/reccobeats/delta/audio_features_delta"
)

TRUSTED_RECCOBEATS_DELTA_URI = (
    f"s3://{TRUSTED_BUCKET}/structured/reccobeats/delta/audio_features_clean_delta"
)

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "AWS_ENDPOINT_URL": f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}",
    "AWS_ALLOW_HTTP": "false" if MINIO_SECURE else "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


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


def normalize_isrc(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().upper().replace("-", "").replace(" ", "")
    return pd.NA if s == "" else s


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


def in_unit_interval(series: pd.Series) -> pd.Series:
    return series.isna() | ((series >= 0) & (series <= 1))


# ---------------------------------------------------------------------
# Cleaning logic
# ---------------------------------------------------------------------
def clean_reccobeats_table(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "rb_track_id",
        "rb_href",
        "rb_name",
        "rb_artist",
        "rb_isrc",
        "rb_danceability",
        "rb_energy",
        "rb_valence",
        "rb_tempo",
        "rb_acousticness",
        "rb_instrumentalness",
        "rb_liveness",
        "rb_loudness",
        "rb_speechiness",
        "rb_mode",
        "rb_key",
        "rb_time_signature",
        "rb_duration_ms",
        "run_id",
        "run_date",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in bronze ReccoBeats table: {missing}")

    df = df.copy()

    # Trim / normalize empties to null
    string_cols = [
        "rb_track_id",
        "rb_href",
        "rb_name",
        "rb_artist",
        "rb_isrc",
        "rb_mode",
        "rb_key",
        "run_id",
        "run_date",
    ]
    for col in string_cols:
        df[col] = df[col].apply(safe_strip)

    # Numeric cast
    numeric_cols = [
        "rb_danceability",
        "rb_energy",
        "rb_valence",
        "rb_tempo",
        "rb_acousticness",
        "rb_instrumentalness",
        "rb_liveness",
        "rb_loudness",
        "rb_speechiness",
        "rb_time_signature",
        "rb_duration_ms",
    ]
    df = cast_numeric(df, numeric_cols)

    # Canonical field names
    df["track_id"] = df["rb_track_id"]
    df["track_name"] = df["rb_name"]
    df["artist_name"] = df["rb_artist"]
    df["isrc"] = df["rb_isrc"].apply(normalize_isrc)
    df["track_href"] = df["rb_href"]

    # Normalized helper columns
    df["track_name_norm"] = df["track_name"].apply(normalize_text_key)
    df["artist_name_norm"] = df["artist_name"].apply(normalize_text_key)

    # Standardize mode/key
    df["mode"] = df["rb_mode"].apply(lambda x: pd.NA if pd.isna(x) else str(x).strip().lower())
    df["musical_key"] = df["rb_key"].apply(lambda x: pd.NA if pd.isna(x) else str(x).strip().upper())

    # Validity flags
    df["has_identity"] = df["isrc"].notna()

    df["valid_danceability"] = in_unit_interval(df["rb_danceability"])
    df["valid_energy"] = in_unit_interval(df["rb_energy"])
    df["valid_valence"] = in_unit_interval(df["rb_valence"])
    df["valid_acousticness"] = in_unit_interval(df["rb_acousticness"])
    df["valid_instrumentalness"] = in_unit_interval(df["rb_instrumentalness"])
    df["valid_liveness"] = in_unit_interval(df["rb_liveness"])
    df["valid_speechiness"] = in_unit_interval(df["rb_speechiness"])

    df["valid_tempo"] = df["rb_tempo"].isna() | (df["rb_tempo"] > 0)
    df["valid_duration_ms"] = df["rb_duration_ms"].isna() | (df["rb_duration_ms"] > 0)
    df["valid_time_signature"] = df["rb_time_signature"].isna() | (df["rb_time_signature"] > 0)

    df["has_any_core_feature"] = (
        df["rb_danceability"].notna()
        | df["rb_energy"].notna()
        | df["rb_valence"].notna()
        | df["rb_tempo"].notna()
        | df["rb_acousticness"].notna()
        | df["rb_instrumentalness"].notna()
        | df["rb_liveness"].notna()
        | df["rb_speechiness"].notna()
    )

    df["is_valid_record"] = (
        df["has_identity"]
        & df["has_any_core_feature"]
        & df["valid_danceability"]
        & df["valid_energy"]
        & df["valid_valence"]
        & df["valid_acousticness"]
        & df["valid_instrumentalness"]
        & df["valid_liveness"]
        & df["valid_speechiness"]
        & df["valid_tempo"]
        & df["valid_duration_ms"]
        & df["valid_time_signature"]
    )

    # Keep only valid trusted records
    df = df[df["is_valid_record"]].copy()

    # Deduplicate by ISRC
    # Prefer rows with more populated feature values
    feature_cols = [
        "rb_danceability",
        "rb_energy",
        "rb_valence",
        "rb_tempo",
        "rb_acousticness",
        "rb_instrumentalness",
        "rb_liveness",
        "rb_loudness",
        "rb_speechiness",
        "rb_time_signature",
        "rb_duration_ms",
    ]
    df["feature_count"] = df[feature_cols].notna().sum(axis=1)

    df = df.sort_values(
        by=["isrc", "feature_count", "run_date"],
        ascending=[True, False, True],
        na_position="last",
    )

    df = df.drop_duplicates(subset=["isrc"], keep="first").reset_index(drop=True)

    # Trusted metadata
    df["trusted_processed_at_utc"] = datetime.now(timezone.utc).isoformat()
    df["trusted_source_zone"] = "bronze"
    df["trusted_target_zone"] = "trusted"

    final_cols = [
        "isrc",
        "track_id",
        "track_name",
        "artist_name",
        "track_href",
        "track_name_norm",
        "artist_name_norm",
        "rb_danceability",
        "rb_energy",
        "rb_valence",
        "rb_tempo",
        "rb_acousticness",
        "rb_instrumentalness",
        "rb_liveness",
        "rb_loudness",
        "rb_speechiness",
        "mode",
        "musical_key",
        "rb_time_signature",
        "rb_duration_ms",
        "run_id",
        "run_date",
        "trusted_processed_at_utc",
        "trusted_source_zone",
        "trusted_target_zone",
    ]
    df = df[final_cols].copy()

    # Cleaner final names
    df = df.rename(
        columns={
            "rb_danceability": "danceability",
            "rb_energy": "energy",
            "rb_valence": "valence",
            "rb_tempo": "tempo",
            "rb_acousticness": "acousticness",
            "rb_instrumentalness": "instrumentalness",
            "rb_liveness": "liveness",
            "rb_loudness": "loudness",
            "rb_speechiness": "speechiness",
            "rb_time_signature": "time_signature",
            "rb_duration_ms": "duration_ms",
        }
    )

    # Final casts
    df = cast_nullable_string(
        df,
        [
            "isrc",
            "track_id",
            "track_name",
            "artist_name",
            "track_href",
            "track_name_norm",
            "artist_name_norm",
            "mode",
            "musical_key",
            "run_id",
            "run_date",
            "trusted_processed_at_utc",
            "trusted_source_zone",
            "trusted_target_zone",
        ],
    )

    numeric_final_cols = [
        "danceability",
        "energy",
        "valence",
        "tempo",
        "acousticness",
        "instrumentalness",
        "liveness",
        "loudness",
        "speechiness",
        "time_signature",
        "duration_ms",
    ]
    df = cast_numeric(df, numeric_final_cols)

    return df


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    print("[Trusted][ReccoBeats] Starting trusted-zone cleaning for audio features...")

    if not delta_table_exists(BRONZE_RECCOBEATS_DELTA_URI):
        raise RuntimeError(
            f"Bronze ReccoBeats Delta table does not exist: {BRONZE_RECCOBEATS_DELTA_URI}"
        )

    print(f"[Trusted][ReccoBeats] Reading bronze table: {BRONZE_RECCOBEATS_DELTA_URI}")
    df_bronze = load_delta_as_df(BRONZE_RECCOBEATS_DELTA_URI)
    print(f"[Trusted][ReccoBeats] Rows read from bronze: {len(df_bronze)}")

    df_clean = clean_reccobeats_table(df_bronze)
    print(f"[Trusted][ReccoBeats] Rows after trusted cleaning: {len(df_clean)}")
    print(f"[Trusted][ReccoBeats] Unique ISRCs after cleaning: {df_clean['isrc'].nunique()}")

    write_deltalake(
        TRUSTED_RECCOBEATS_DELTA_URI,
        df_clean,
        mode="overwrite",
        storage_options=DELTA_STORAGE_OPTIONS,
    )

    print(f"[Trusted][ReccoBeats] Trusted Delta written to: {TRUSTED_RECCOBEATS_DELTA_URI}")
    print("[Trusted][ReccoBeats] Done.")


if __name__ == "__main__":
    main()