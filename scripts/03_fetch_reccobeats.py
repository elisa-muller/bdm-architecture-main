# pip install requests pandas python-dotenv deltalake pyarrow minio

import io
import os
import time
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
import requests
from deltalake import DeltaTable, write_deltalake
from minio import Minio

# -----------------------------
# MinIO / Delta configuration
# -----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ENDPOINT = MINIO_ENDPOINT.replace("http://", "").replace("https://", "").rstrip("/")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

ISRC_CACHE_DELTA_URI = (
    f"s3://{MINIO_BUCKET}/persistent/structured/musicbrainz/delta/isrc_cache_delta"
)
AUDIO_FEATURES_DELTA_URI = (
    f"s3://{MINIO_BUCKET}/persistent/structured/reccobeats/delta/audio_features_delta"
)

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "AWS_ENDPOINT_URL": f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}",
    "AWS_ALLOW_HTTP": "false" if MINIO_SECURE else "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# -----------------------------
# ReccoBeats configuration
# -----------------------------
RECCOBEATS_BASE = os.getenv("RECCOBEATS_BASE", "https://api.reccobeats.com/v1")

RB_BATCH_SIZE = 25
RB_SLEEP = 0.25
CHECKPOINT_EVERY_BATCHES = 10

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# -----------------------------
# MinIO client
# -----------------------------

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

# -----------------------------
# Helpers
# -----------------------------
def safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def delta_table_exists(delta_uri: str) -> bool:
    try:
        DeltaTable(delta_uri, storage_options=DELTA_STORAGE_OPTIONS)
        return True
    except Exception:
        return False


def load_delta_as_df(delta_uri: str) -> pd.DataFrame:
    dt = DeltaTable(delta_uri, storage_options=DELTA_STORAGE_OPTIONS)
    return dt.to_pandas()


def append_to_delta(df: pd.DataFrame, delta_uri: str) -> None:
    if df.empty:
        return

    if not delta_table_exists(delta_uri):
        write_deltalake(
            delta_uri,
            df,
            mode="overwrite",
            storage_options=DELTA_STORAGE_OPTIONS,
        )
        print(f"[ReccoBeats] Created Delta table: {delta_uri}")
        return

    write_deltalake(
        delta_uri,
        df,
        mode="append",
        storage_options=DELTA_STORAGE_OPTIONS,
    )
    print(f"[ReccoBeats] Appended {len(df)} rows to Delta table: {delta_uri}")


def upload_csv_to_minio(df: pd.DataFrame, bucket_name: str, object_name: str) -> None:
    if df.empty:
        return

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    data_stream = io.BytesIO(csv_bytes)

    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )
    print(f"[ReccoBeats] Uploaded CSV to s3://{bucket_name}/{object_name}")


def export_full_debug_csv_local(delta_uri: str, local_path: str) -> None:
    if not delta_table_exists(delta_uri):
        return

    df_check = load_delta_as_df(delta_uri)
    df_check.to_csv(local_path, index=False)
    print(f"[ReccoBeats] Debug CSV exported locally: {local_path}")


def parse_feature_item(item: Dict) -> Dict:
    return {
        "rb_track_id": item.get("id"),
        "rb_href": item.get("href"),
        "rb_name": item.get("name"),
        "rb_artist": item.get("artist"),
        "rb_isrc": item.get("isrc"),
        "rb_danceability": item.get("danceability"),
        "rb_energy": item.get("energy"),
        "rb_valence": item.get("valence"),
        "rb_tempo": item.get("tempo"),
        "rb_acousticness": item.get("acousticness"),
        "rb_instrumentalness": item.get("instrumentalness"),
        "rb_liveness": item.get("liveness"),
        "rb_loudness": item.get("loudness"),
        "rb_speechiness": item.get("speechiness"),
        "rb_mode": item.get("mode"),
        "rb_key": item.get("key"),
        "rb_time_signature": item.get("time_signature"),
        "rb_duration_ms": item.get("duration_ms"),
    }


def cast_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
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

    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_audio_features_batch(ids_batch: List[str]) -> List[Dict]:
    try:
        r = requests.get(
            f"{RECCOBEATS_BASE}/audio-features",
            params={"ids": ",".join(ids_batch)},
            timeout=20,
        )

        print(f"[ReccoBeats] status={r.status_code} ids_batch_size={len(ids_batch)}")

        if r.status_code == 200:
            data = r.json()

            if isinstance(data, dict) and "content" in data:
                content = data["content"]
                return content if isinstance(content, list) else []

            if isinstance(data, list):
                return data

            print(f"[ReccoBeats] Unexpected response format: {type(data)}")
            return []

        print(f"[ReccoBeats] body={r.text[:300]}")
        return []

    except Exception as e:
        print(f"[ReccoBeats] request error: {e}")
        return []


def prepare_input_isrcs() -> pd.DataFrame:
    print(f"[ReccoBeats] Loading input ISRC cache from: {ISRC_CACHE_DELTA_URI}")
    df_isrc = load_delta_as_df(ISRC_CACHE_DELTA_URI)

    required_cols = ["isrc", "lastfm_track_name", "lastfm_artist_name", "lastfm_track_mbid"]
    missing = [c for c in required_cols if c not in df_isrc.columns]
    if missing:
        raise ValueError(f"Missing required columns in isrc_cache_delta: {missing}")

    df_valid = df_isrc[df_isrc["isrc"].notna()].copy()
    df_valid["isrc"] = df_valid["isrc"].apply(safe_str)
    df_valid = df_valid[df_valid["isrc"] != ""].copy()

    print(f"[ReccoBeats] Rows in ISRC cache: {len(df_isrc)}")
    print(f"[ReccoBeats] Rows with valid ISRC before dedup: {len(df_valid)}")

    df_valid = df_valid.drop_duplicates(subset=["isrc"]).reset_index(drop=True)

    print(f"[ReccoBeats] Unique valid ISRCs after dedup: {len(df_valid)}")

    if delta_table_exists(AUDIO_FEATURES_DELTA_URI):
        df_existing = load_delta_as_df(AUDIO_FEATURES_DELTA_URI)
        print(f"[ReccoBeats] Existing audio feature rows: {len(df_existing)}")

        existing_isrcs = set()
        if "rb_isrc" in df_existing.columns:
            existing_isrcs = set(df_existing["rb_isrc"].astype(str).str.strip())

        df_valid = df_valid[~df_valid["isrc"].isin(existing_isrcs)].copy().reset_index(drop=True)
        print(f"[ReccoBeats] ISRCs still to fetch after excluding existing ones: {len(df_valid)}")
    else:
        print("[ReccoBeats] audio_features_delta does not exist yet. It will be created.")

    return df_valid


def flush_feature_batch_rows(batch_feature_rows: List[Dict], flush_number: int) -> List[Dict]:
    if not batch_feature_rows:
        return []

    df_batch = pd.DataFrame(batch_feature_rows)

    if "rb_isrc" in df_batch.columns:
        df_batch["rb_isrc"] = df_batch["rb_isrc"].apply(safe_str)
        df_batch = df_batch[df_batch["rb_isrc"] != ""].copy()

    if df_batch.empty:
        return []

    df_batch = df_batch.drop_duplicates(subset=["rb_isrc"], keep="last").reset_index(drop=True)

    df_batch["run_id"] = run_id
    df_batch["run_date"] = run_date

    df_batch = cast_feature_columns(df_batch)

    temporal_object = (
        f"temporal/structured/reccobeats/raw/"
        f"run_date={run_date}/run_id={run_id}/"
        f"reccobeats_audio_features_batch_{flush_number:05d}.csv"
    )

    upload_csv_to_minio(df_batch, MINIO_BUCKET, temporal_object)
    append_to_delta(df_batch, AUDIO_FEATURES_DELTA_URI)
    export_full_debug_csv_local(AUDIO_FEATURES_DELTA_URI, "reccobeats_audio_features_debug.csv")

    print(
        f"[ReccoBeats] Checkpoint saved. Flush {flush_number} | "
        f"rows in batch: {len(df_batch)} | "
        f"temporal: s3://{MINIO_BUCKET}/{temporal_object}"
    )

    return []


# -----------------------------
# Main
# -----------------------------
def main():
    print(f"[ReccoBeats] Run ID: {run_id}")
    print(f"[ReccoBeats] Run date: {run_date}")
    print(f"[ReccoBeats] MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"[ReccoBeats] Bucket: {MINIO_BUCKET}")

    df_valid = prepare_input_isrcs()

    if df_valid.empty:
        print("[ReccoBeats] No new valid ISRC values to fetch. Exiting.")
        return

    ids_to_fetch = df_valid["isrc"].drop_duplicates().tolist()
    print(f"[ReccoBeats] Unique ISRCs to fetch this run: {len(ids_to_fetch)}")

    batch_feature_rows = []
    flush_number = 0
    total_returned_items = 0
    total_batches = 0
    total_empty_batches = 0

    try:
        for i in range(0, len(ids_to_fetch), RB_BATCH_SIZE):
            batch = ids_to_fetch[i:i + RB_BATCH_SIZE]
            batch_idx = i // RB_BATCH_SIZE + 1
            total_batches += 1

            print(f"[ReccoBeats] Fetching batch {batch_idx}: size={len(batch)}")

            items = get_audio_features_batch(batch)

            if not items:
                total_empty_batches += 1
                print("[ReccoBeats] No items returned for this batch.")
            else:
                parsed_rows = [parse_feature_item(item) for item in items]
                total_returned_items += len(parsed_rows)
                batch_feature_rows.extend(parsed_rows)

            if batch_idx % CHECKPOINT_EVERY_BATCHES == 0 and batch_feature_rows:
                flush_number += 1
                batch_feature_rows = flush_feature_batch_rows(batch_feature_rows, flush_number)

            time.sleep(RB_SLEEP)

    except KeyboardInterrupt:
        print("\n[ReccoBeats] Interrupted by user. Flushing remaining feature rows before exit...")
        if batch_feature_rows:
            flush_number += 1
            batch_feature_rows = flush_feature_batch_rows(batch_feature_rows, flush_number)
        print("[ReccoBeats] Partial progress saved.")
        return

    if batch_feature_rows:
        flush_number += 1
        batch_feature_rows = flush_feature_batch_rows(batch_feature_rows, flush_number)

    print("\n[ReccoBeats] Done.")
    print(f"[ReccoBeats] ISRCs requested this run: {len(ids_to_fetch)}")
    print(f"[ReccoBeats] API batches sent: {total_batches}")
    print(f"[ReccoBeats] Empty batches: {total_empty_batches}")
    print(f"[ReccoBeats] Returned feature items: {total_returned_items}")

    if delta_table_exists(AUDIO_FEATURES_DELTA_URI):
        df_check = load_delta_as_df(AUDIO_FEATURES_DELTA_URI)
        print(f"[ReccoBeats] Rows currently in audio_features_delta: {len(df_check)}")
        if "rb_danceability" in df_check.columns:
            print(f"[ReccoBeats] Rows with audio features: {df_check['rb_danceability'].notna().sum()}")

    print("\n[ReccoBeats] Final debug CSV exported locally: reccobeats_audio_features_debug.csv")
    print(f"[ReccoBeats] Persistent Delta in MinIO: {AUDIO_FEATURES_DELTA_URI}")
    print(
        f"[ReccoBeats] Temporal CSV batches in MinIO: "
        f"s3://{MINIO_BUCKET}/temporal/structured/reccobeats/raw/run_date={run_date}/run_id={run_id}/"
    )


if __name__ == "__main__":
    main()