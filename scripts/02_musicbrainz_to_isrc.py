# pip install requests pandas python-dotenv deltalake pyarrow minio

import io
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

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

LASTFM_TRACKS_DELTA_URI = (
    f"s3://{MINIO_BUCKET}/persistent/structured/lastfm/delta/tracks_delta"
)
ISRC_CACHE_DELTA_URI = (
    f"s3://{MINIO_BUCKET}/persistent/structured/musicbrainz/delta/isrc_cache_delta"
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
# MusicBrainz configuration
# -----------------------------
MUSICBRAINZ_BASE = os.getenv("MUSICBRAINZ_BASE", "https://musicbrainz.org/ws/2")
MUSICBRAINZ_CONTACT_EMAIL = os.getenv("MUSICBRAINZ_CONTACT_EMAIL", "team@example.com")
MUSICBRAINZ_HEADERS = {
    "User-Agent": f"bdm-project/1.0 (student-project; contact: {MUSICBRAINZ_CONTACT_EMAIL})"
}

MB_SLEEP = 1.5
MAX_RETRIES_503 = 3
CHECKPOINT_EVERY = 20
MAX_MBIDS_PER_RUN = 100

# Fallback search tuning
SEARCH_LIMIT = 5
MIN_SEARCH_SCORE = 85  # heuristic, adjust if needed

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# -----------------------------
# HTTP session
# -----------------------------
session = requests.Session()
session.headers.update(MUSICBRAINZ_HEADERS)

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


def normalize_text(s: str) -> str:
    s = safe_str(s).lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)
    return s.strip()


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
        print(f"[MusicBrainz] Created Delta table: {delta_uri}")
        return

    write_deltalake(
        delta_uri,
        df,
        mode="append",
        storage_options=DELTA_STORAGE_OPTIONS,
    )
    print(f"[MusicBrainz] Appended {len(df)} rows to Delta table: {delta_uri}")


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
    print(f"[MusicBrainz] Uploaded CSV to s3://{bucket_name}/{object_name}")


def export_full_debug_csv_local(delta_uri: str, local_path: str) -> None:
    if not delta_table_exists(delta_uri):
        return

    df_check = load_delta_as_df(delta_uri)
    df_check.to_csv(local_path, index=False)
    print(f"[MusicBrainz] Debug CSV exported locally: {local_path}")


def extract_first_isrc(data: dict) -> Optional[str]:
    isrcs = data.get("isrcs", [])
    if isinstance(isrcs, list) and len(isrcs) > 0:
        return isrcs[0]
    return None


def get_isrc_from_recording_mbid(mbid: str, max_retries: int = MAX_RETRIES_503):
    """
    Direct MusicBrainz lookup:
    /ws/2/recording/{mbid}?inc=isrcs&fmt=json
    """
    mbid = safe_str(mbid)

    if not mbid or len(mbid) < 10:
        return None, "invalid_mbid", None

    for attempt in range(max_retries + 1):
        try:
            r = session.get(
                f"{MUSICBRAINZ_BASE}/recording/{mbid}",
                params={"inc": "isrcs", "fmt": "json"},
                timeout=15,
            )

            if r.status_code == 200:
                data = r.json()
                isrc = extract_first_isrc(data)
                matched_mbid = safe_str(data.get("id"))
                if isrc:
                    return isrc, "ok", matched_mbid
                return None, "no_isrc", matched_mbid

            if r.status_code == 503:
                if attempt < max_retries:
                    wait_time = 2 * (attempt + 1)
                    print(
                        f"[MusicBrainz][MBID] status=503 mbid={mbid} | "
                        f"retry {attempt + 1}/{max_retries} in {wait_time}s"
                    )
                    time.sleep(wait_time)
                    continue
                return None, "rate_limited_503", None

            if r.status_code == 404:
                return None, "not_found_404", None

            print(f"[MusicBrainz][MBID] status={r.status_code} mbid={mbid}")
            return None, "other_http_error", None

        except Exception as e:
            print(f"[MusicBrainz][MBID] error mbid={mbid}: {e}")
            if attempt < max_retries:
                wait_time = 2 * (attempt + 1)
                time.sleep(wait_time)
                continue
            return None, "request_exception", None

    return None, "request_exception", None


def choose_best_search_match(recordings: list, artist: str, track: str) -> Optional[dict]:
    """
    Heuristic match selection:
    1) exact normalized recording + exact normalized artist-credit phrase
    2) otherwise highest score above threshold
    """
    if not recordings:
        return None

    target_artist = normalize_text(artist)
    target_track = normalize_text(track)

    for rec in recordings:
        rec_title = normalize_text(rec.get("title"))
        if rec_title != target_track:
            continue

        artist_credit = rec.get("artist-credit", [])
        credit_names = []
        for item in artist_credit:
            if isinstance(item, dict):
                name = item.get("name") or item.get("artist", {}).get("name")
                if name:
                    credit_names.append(name)

        rec_artist = normalize_text(" ".join(credit_names))
        if target_artist and target_artist in rec_artist:
            return rec

    best = None
    best_score = -1
    for rec in recordings:
        try:
            score = int(rec.get("score", 0))
        except Exception:
            score = 0
        if score > best_score:
            best = rec
            best_score = score

    if best is not None and best_score >= MIN_SEARCH_SCORE:
        return best

    return None


def search_recording_and_get_isrc(
    artist: str,
    track: str,
    max_retries: int = MAX_RETRIES_503,
) -> Tuple[Optional[str], str, Optional[str], Optional[int]]:
    """
    MusicBrainz search fallback:
    /ws/2/recording?query=recording:"..." AND artist:"..."&fmt=json
    Then inspect best match and extract first ISRC.
    """
    artist = safe_str(artist)
    track = safe_str(track)

    if not artist or not track:
        return None, "search_no_match", None, None

    query = f'recording:"{track}" AND artist:"{artist}"'

    for attempt in range(max_retries + 1):
        try:
            r = session.get(
                f"{MUSICBRAINZ_BASE}/recording",
                params={
                    "query": query,
                    "fmt": "json",
                    "limit": SEARCH_LIMIT,
                },
                timeout=20,
            )

            if r.status_code == 200:
                data = r.json()
                recordings = data.get("recordings", [])
                best = choose_best_search_match(recordings, artist, track)

                if not best:
                    return None, "search_no_match", None, None

                matched_mbid = safe_str(best.get("id"))
                score = int(best.get("score", 0)) if safe_str(best.get("score")) else None
                isrc = extract_first_isrc(best)

                if isrc:
                    return isrc, "search_ok", matched_mbid, score

                return None, "search_no_isrc", matched_mbid, score

            if r.status_code == 503:
                if attempt < max_retries:
                    wait_time = 2 * (attempt + 1)
                    print(
                        f"[MusicBrainz][SEARCH] status=503 artist='{artist}' track='{track}' | "
                        f"retry {attempt + 1}/{max_retries} in {wait_time}s"
                    )
                    time.sleep(wait_time)
                    continue
                return None, "rate_limited_503", None, None

            print(f"[MusicBrainz][SEARCH] status={r.status_code} artist='{artist}' track='{track}'")
            return None, "search_request_exception", None, None

        except Exception as e:
            print(f"[MusicBrainz][SEARCH] error artist='{artist}' track='{track}': {e}")
            if attempt < max_retries:
                wait_time = 2 * (attempt + 1)
                time.sleep(wait_time)
                continue
            return None, "search_request_exception", None, None

    return None, "search_request_exception", None, None


def flush_batch(
    batch_rows: list,
    batch_number: int,
    stats: dict,
) -> list:
    if not batch_rows:
        return []

    df_batch = pd.DataFrame(batch_rows)

    # Safety dedup by original input MBID
    df_batch = df_batch.drop_duplicates(
        subset=["lastfm_track_mbid"], keep="last"
    ).reset_index(drop=True)

    temporal_object = (
        f"temporal/structured/musicbrainz/raw/"
        f"run_date={run_date}/run_id={run_id}/"
        f"musicbrainz_isrc_batch_{batch_number:05d}.csv"
    )
    upload_csv_to_minio(df_batch, MINIO_BUCKET, temporal_object)

    append_to_delta(df_batch, ISRC_CACHE_DELTA_URI)

    export_full_debug_csv_local(
        ISRC_CACHE_DELTA_URI,
        "musicbrainz_isrc_debug.csv"
    )

    print(
        f"[MusicBrainz] Checkpoint saved. Batch {batch_number} | "
        f"rows in batch: {len(df_batch)} | "
        f"temporal: s3://{MINIO_BUCKET}/{temporal_object}"
    )
    print("[MusicBrainz] Current status counts:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    return []


# -----------------------------
# Main
# -----------------------------
def main():
    print(f"[MusicBrainz] Run ID: {run_id}")
    print(f"[MusicBrainz] Run date: {run_date}")
    print(f"[MusicBrainz] MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"[MusicBrainz] Bucket: {MINIO_BUCKET}")
    print(f"[MusicBrainz] Loading Last.fm tracks from: {LASTFM_TRACKS_DELTA_URI}")

    df_tracks = load_delta_as_df(LASTFM_TRACKS_DELTA_URI)

    required_cols = ["lastfm_track_mbid", "lastfm_artist_name", "lastfm_track_name"]
    missing = [c for c in required_cols if c not in df_tracks.columns]
    if missing:
        raise ValueError(f"Missing required columns in tracks_delta: {missing}")

    df_tracks["lastfm_track_mbid"] = df_tracks["lastfm_track_mbid"].apply(safe_str)
    df_tracks = df_tracks[df_tracks["lastfm_track_mbid"] != ""].copy()

    df_tracks = df_tracks.drop_duplicates(subset=["lastfm_track_mbid"]).reset_index(drop=True)
    print(f"[MusicBrainz] Tracks with usable MBID: {len(df_tracks)}")

    if delta_table_exists(ISRC_CACHE_DELTA_URI):
        df_cache = load_delta_as_df(ISRC_CACHE_DELTA_URI)
        print(f"[MusicBrainz] Existing ISRC cache rows: {len(df_cache)}")
    else:
        df_cache = pd.DataFrame(columns=[
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
        ])
        print("[MusicBrainz] ISRC cache does not exist yet. It will be created.")

    resolved_mbids = (
        set(df_cache["lastfm_track_mbid"].astype(str).str.strip())
        if not df_cache.empty and "lastfm_track_mbid" in df_cache.columns
        else set()
    )

    df_to_process = df_tracks[
        ~df_tracks["lastfm_track_mbid"].isin(resolved_mbids)
    ].copy().reset_index(drop=True)

    df_to_process = df_to_process.head(MAX_MBIDS_PER_RUN).copy()
    print(f"[MusicBrainz] MBIDs to process this run (limited): {len(df_to_process)}")

    stats = {
        "ok": 0,
        "no_isrc": 0,
        "not_found_404": 0,
        "rate_limited_503": 0,
        "other_http_error": 0,
        "request_exception": 0,
        "invalid_mbid": 0,
        "search_ok": 0,
        "search_no_isrc": 0,
        "search_no_match": 0,
        "search_request_exception": 0,
    }

    batch_rows = []
    batch_number = 0

    try:
        for i, row in df_to_process.iterrows():
            artist = safe_str(row.get("lastfm_artist_name"))
            track = safe_str(row.get("lastfm_track_name"))
            mbid = safe_str(row.get("lastfm_track_mbid"))

            isrc, mb_status, matched_mbid = get_isrc_from_recording_mbid(mbid)
            search_score = None
            resolution_method = None

            if isrc:
                resolution_method = "mbid"
                stats[mb_status] += 1

            else:
                stats[mb_status] += 1

                # Fallback not only for 404, but also for transient request/network errors
                if mb_status in {"not_found_404", "request_exception", "rate_limited_503", "other_http_error"}:
                    search_isrc, search_status, search_mbid, search_score = search_recording_and_get_isrc(
                        artist=artist,
                        track=track,
                    )
                    stats[search_status] = stats.get(search_status, 0) + 1

                    if search_isrc:
                        isrc = search_isrc
                        matched_mbid = search_mbid
                        resolution_method = "search"
                        mb_status = search_status
                    else:
                        matched_mbid = search_mbid
                        resolution_method = f"mbid_then_{search_status}"
                        mb_status = search_status
                else:
                    resolution_method = f"mbid_{mb_status}"

            batch_rows.append({
                "lastfm_track_mbid": mbid,
                "lastfm_artist_name": artist,
                "lastfm_track_name": track,
                "resolution_method": resolution_method,
                "matched_recording_mbid": matched_mbid,
                "search_score": search_score,
                "resolved_mbid": mbid,
                "isrc": isrc,
                "mb_status": mb_status,
                "resolved_at_utc": datetime.now(timezone.utc).isoformat(),
                "run_id": run_id,
                "run_date": run_date,
            })

            print(
                f"[MusicBrainz][{i + 1}/{len(df_to_process)}] "
                f"artist='{artist}' | track='{track}' | "
                f"input_mbid={mbid} | method={resolution_method} | "
                f"matched_mbid={matched_mbid} | score={search_score} | isrc={isrc}"
            )

            if len(batch_rows) >= CHECKPOINT_EVERY:
                batch_number += 1
                batch_rows = flush_batch(batch_rows, batch_number, stats)

            time.sleep(MB_SLEEP)

    except KeyboardInterrupt:
        print("\n[MusicBrainz] Interrupted by user. Flushing remaining batch before exit...")
        if batch_rows:
            batch_number += 1
            batch_rows = flush_batch(batch_rows, batch_number, stats)
        print("[MusicBrainz] Partial progress saved.")
        return

    if batch_rows:
        batch_number += 1
        batch_rows = flush_batch(batch_rows, batch_number, stats)

    print("\n[MusicBrainz] Done.")
    print(f"[MusicBrainz] Processed MBIDs this run: {len(df_to_process)}")

    print("\n[MusicBrainz] Final status counts:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    print("\n[MusicBrainz] Final debug CSV exported locally: musicbrainz_isrc_debug.csv")
    print(f"[MusicBrainz] Persistent Delta in MinIO: {ISRC_CACHE_DELTA_URI}")
    print(
        f"[MusicBrainz] Temporal CSV batches in MinIO: "
        f"s3://{MINIO_BUCKET}/temporal/structured/musicbrainz/raw/run_date={run_date}/run_id={run_id}/"
    )


if __name__ == "__main__":
    main()