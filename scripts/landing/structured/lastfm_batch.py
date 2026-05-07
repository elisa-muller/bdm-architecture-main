# pip install requests pandas python-dotenv minio deltalake pyarrow

import io
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from deltalake import write_deltalake

load_dotenv()

# -----------------------------
# Environment
# -----------------------------
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
if not LASTFM_API_KEY:
    raise ValueError("Missing LASTFM_API_KEY in environment variables or .env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ENDPOINT = MINIO_ENDPOINT.replace("http://", "").replace("https://", "").rstrip("/")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

API_URL = os.getenv("LASTFM_API_URL", "http://ws.audioscrobbler.com/2.0/")

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "AWS_ENDPOINT_URL": f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}",
    "AWS_ALLOW_HTTP": "false" if MINIO_SECURE else "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# Persistent landing path
DELTA_URI = f"s3://{MINIO_BUCKET}/persistent/structured/lastfm/delta/tracks_delta"

# For testing, use overwrite to replace the previous wrongly preprocessed table.
# Later, change to "append" if you want to keep all runs.
DELTA_WRITE_MODE = os.getenv("LASTFM_DELTA_WRITE_MODE", "overwrite")


# -----------------------------
# Config
# -----------------------------
tags = [
    "pop", "rock", "jazz", "electronic", "classical", "hip-hop", "reggae",
    "indie", "metal", "blues", "folk", "soul", "dance", "ambient",
    "techno", "house", "punk", "latin", "rnb", "country"
]

countries = [
    "spain", "united states", "united kingdom", "germany", "france",
    "italy", "japan", "south korea", "brazil", "mexico"
]

per_page_limit = 50
chart_pages = 1
tag_pages = 1
geo_pages = 1
sleep_seconds = 0.25

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ingested_at_utc = datetime.now(timezone.utc).isoformat()


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
def ensure_bucket(bucket_name: str) -> None:
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"[Last.fm] Bucket created: {bucket_name}")
        else:
            print(f"[Last.fm] Bucket already exists: {bucket_name}")
    except S3Error as e:
        raise RuntimeError(f"Error checking/creating bucket '{bucket_name}': {e}")


def call_lastfm(method: str, extra_params: dict) -> dict:
    params = {
        "method": method,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        **extra_params,
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_track_item(track: dict, source_type: str, source_value: str, page: int) -> dict:
    artist_name = None
    artist_mbid = None

    artist_field = track.get("artist")
    if isinstance(artist_field, dict):
        artist_name = artist_field.get("name")
        artist_mbid = artist_field.get("mbid")
    else:
        artist_name = artist_field

    image_url = None
    images = track.get("image", [])
    if isinstance(images, list) and images:
        for img in reversed(images):
            if isinstance(img, dict) and img.get("#text"):
                image_url = img.get("#text")
                break

    streamable = track.get("streamable")
    if isinstance(streamable, dict):
        streamable = streamable.get("fulltrack") or streamable.get("#text")

    return {
        "run_id": run_id,
        "run_date": run_date,
        "ingested_at_utc": ingested_at_utc,

        "source_type": source_type,
        "source_value": source_value,
        "source_page": page,

        "lastfm_track_name": track.get("name"),
        "lastfm_track_mbid": track.get("mbid"),
        "lastfm_artist_name": artist_name,
        "lastfm_artist_mbid": artist_mbid,
        "lastfm_url": track.get("url"),
        "lastfm_duration": track.get("duration"),
        "lastfm_streamable": streamable,
        "lastfm_image_url": image_url,

        "lastfm_listeners": track.get("listeners"),
        "lastfm_playcount": track.get("playcount"),
        "lastfm_rank": (
            track.get("@attr", {}).get("rank")
            if isinstance(track.get("@attr"), dict)
            else None
        ),
    }


def fetch_chart_tracks(max_pages: int = 20, limit: int = 50) -> list:
    rows = []
    for page in range(1, max_pages + 1):
        print(f"[Last.fm][chart] page {page}")
        data = call_lastfm("chart.getTopTracks", {"page": page, "limit": limit})
        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break
        for track in tracks:
            rows.append(parse_track_item(track, "chart", "global", page))
        time.sleep(sleep_seconds)
    return rows


def fetch_tag_tracks(tag: str, max_pages: int = 20, limit: int = 50) -> list:
    rows = []
    for page in range(1, max_pages + 1):
        print(f"[Last.fm][tag={tag}] page {page}")
        data = call_lastfm("tag.getTopTracks", {"tag": tag, "page": page, "limit": limit})
        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break
        for track in tracks:
            rows.append(parse_track_item(track, "tag", tag, page))
        time.sleep(sleep_seconds)
    return rows


def fetch_geo_tracks(country: str, max_pages: int = 20, limit: int = 50) -> list:
    rows = []
    for page in range(1, max_pages + 1):
        print(f"[Last.fm][country={country}] page {page}")
        data = call_lastfm("geo.getTopTracks", {"country": country, "page": page, "limit": limit})
        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break
        for track in tracks:
            rows.append(parse_track_item(track, "geo", country, page))
        time.sleep(sleep_seconds)
    return rows


def upload_csv_to_minio(df: pd.DataFrame, bucket_name: str, object_name: str) -> None:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    data_stream = io.BytesIO(csv_bytes)

    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )

    print(f"[Last.fm] Uploaded raw CSV to s3://{bucket_name}/{object_name}")


def write_raw_delta_to_minio(df_raw: pd.DataFrame, delta_uri: str) -> None:
    write_deltalake(
        delta_uri,
        df_raw,
        mode=DELTA_WRITE_MODE,
        storage_options=DELTA_STORAGE_OPTIONS,
        schema_mode="overwrite" if DELTA_WRITE_MODE == "overwrite" else None,
    )

    print(f"[Last.fm] Uploaded raw Delta table to {delta_uri}")
    print(f"[Last.fm] Delta write mode: {DELTA_WRITE_MODE}")


# -----------------------------
# Main
# -----------------------------
def main():
    print("[Last.fm] Starting ingestion...")
    print(f"[Last.fm] Run ID: {run_id}")
    print(f"[Last.fm] Run date: {run_date}")
    print(f"[Last.fm] MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"[Last.fm] Bucket: {MINIO_BUCKET}")

    ensure_bucket(MINIO_BUCKET)

    all_rows = []

    all_rows.extend(fetch_chart_tracks(max_pages=chart_pages, limit=per_page_limit))

    for tag in tags:
        try:
            all_rows.extend(fetch_tag_tracks(tag, max_pages=tag_pages, limit=per_page_limit))
        except Exception as e:
            print(f"[Last.fm] Skipping tag={tag} due to error: {e}")
            time.sleep(2)

    for country in countries:
        try:
            all_rows.extend(fetch_geo_tracks(country, max_pages=geo_pages, limit=per_page_limit))
        except Exception as e:
            print(f"[Last.fm] Skipping country={country} due to error: {e}")
            time.sleep(2)

    if not all_rows:
        raise RuntimeError("No rows collected from Last.fm.")

    df_raw = pd.DataFrame(all_rows)

    print(f"[Last.fm] Raw rows collected: {len(df_raw)}")
    print(f"[Last.fm] Raw columns: {list(df_raw.columns)}")

    raw_object = (
        f"temporal/structured/lastfm/raw/"
        f"run_date={run_date}/run_id={run_id}/lastfm_tracks_raw.csv"
    )

    upload_csv_to_minio(df_raw, MINIO_BUCKET, raw_object)
    write_raw_delta_to_minio(df_raw, DELTA_URI)

    print("\n[Last.fm] Pipeline finished successfully.")
    print(f"[Last.fm] Temporal raw CSV        -> s3://{MINIO_BUCKET}/{raw_object}")
    print(f"[Last.fm] Persistent raw Delta   -> {DELTA_URI}")


if __name__ == "__main__":
    main()