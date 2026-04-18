'''
This is only to generate the CSV, not needed for the actual architecture!
'''
from __future__ import annotations

from pathlib import Path
import os

import polars as pl


BASE_DIR = Path(__file__).resolve().parents[1]
SEED_DIR = BASE_DIR / "data_sources" / "trends_seed"

LASTFM_PATH = SEED_DIR / "01_lastfm_tracks_raw.csv"
MB_PATH = SEED_DIR / "02_musicbrainz_isrc_debug.csv"
RECCO_PATH = SEED_DIR / "03_reccobeats_audio_features_debug.csv"

OUTPUT_PARQUET = SEED_DIR / "trends_seed_enriched.parquet"
OUTPUT_CSV = SEED_DIR / "trends_seed_enriched.csv"

# Optional: create a smaller demo file too
WRITE_DEMO_SAMPLE = True
DEMO_SAMPLE_SIZE = 1000
OUTPUT_SAMPLE_CSV = SEED_DIR / "trends_seed_sample.csv"


def read_csv_safe(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    return pl.read_csv(path, infer_schema_length=10000)


def main() -> None:
    print("Loading source files...")
    lastfm = read_csv_safe(LASTFM_PATH)
    mb = read_csv_safe(MB_PATH)
    recco = read_csv_safe(RECCO_PATH)

    print(f"Last.fm rows: {lastfm.height}")
    print(f"MusicBrainz rows: {mb.height}")
    print(f"ReccoBeats rows: {recco.height}")

    # -----------------------------
    # 1) Keep only the latest useful record per track in each dataset
    # -----------------------------
    # Last.fm: deduplicate by track MBID if available, otherwise by track+artist
    lastfm_clean = (
        lastfm
        .with_columns([
            pl.col("lastfm_track_mbid").cast(pl.Utf8),
            pl.col("lastfm_track_name").cast(pl.Utf8),
            pl.col("lastfm_artist_name").cast(pl.Utf8),
            pl.col("lastfm_playcount").cast(pl.Float64, strict=False),
            pl.col("lastfm_listeners").cast(pl.Float64, strict=False),
            pl.col("lastfm_duration").cast(pl.Int64, strict=False),
            pl.col("run_date").cast(pl.Utf8),
            pl.col("ingested_at_utc").cast(pl.Utf8),
        ])
        .sort(["run_date", "ingested_at_utc"], descending=[True, True])
        .unique(
            subset=["lastfm_track_mbid", "lastfm_track_name", "lastfm_artist_name"],
            keep="first",
        )
    )

    # MusicBrainz: keep rows that successfully resolved an ISRC
    mb_clean = (
        mb
        .with_columns([
            pl.col("lastfm_track_mbid").cast(pl.Utf8),
            pl.col("lastfm_track_name").cast(pl.Utf8),
            pl.col("lastfm_artist_name").cast(pl.Utf8),
            pl.col("isrc").cast(pl.Utf8),
            pl.col("mb_status").cast(pl.Utf8),
            pl.col("resolved_at_utc").cast(pl.Utf8),
            pl.col("run_date").cast(pl.Utf8),
        ])
        .filter(
            pl.col("isrc").is_not_null() &
            (pl.col("isrc").str.len_chars() > 0)
        )
        .sort(["run_date", "resolved_at_utc"], descending=[True, True])
        .unique(
            subset=["lastfm_track_mbid", "isrc"],
            keep="first",
        )
    )

    # ReccoBeats: keep only rows with ISRC and latest record per ISRC
    recco_clean = (
        recco
        .with_columns([
            pl.col("rb_isrc").cast(pl.Utf8),
            pl.col("rb_danceability").cast(pl.Float64, strict=False),
            pl.col("rb_energy").cast(pl.Float64, strict=False),
            pl.col("rb_valence").cast(pl.Float64, strict=False),
            pl.col("rb_tempo").cast(pl.Float64, strict=False),
            pl.col("rb_acousticness").cast(pl.Float64, strict=False),
            pl.col("rb_instrumentalness").cast(pl.Float64, strict=False),
            pl.col("rb_liveness").cast(pl.Float64, strict=False),
            pl.col("rb_loudness").cast(pl.Float64, strict=False),
            pl.col("rb_speechiness").cast(pl.Float64, strict=False),
            pl.col("rb_mode").cast(pl.Int64, strict=False),
            pl.col("rb_key").cast(pl.Int64, strict=False),
            pl.col("rb_time_signature").cast(pl.Int64, strict=False),
            pl.col("rb_duration_ms").cast(pl.Int64, strict=False),
            pl.col("run_date").cast(pl.Utf8),
        ])
        .filter(
            pl.col("rb_isrc").is_not_null() &
            (pl.col("rb_isrc").str.len_chars() > 0)
        )
        .sort("run_date", descending=True)
        .unique(subset=["rb_isrc"], keep="first")
    )

    print(f"Last.fm cleaned rows: {lastfm_clean.height}")
    print(f"MusicBrainz cleaned rows: {mb_clean.height}")
    print(f"ReccoBeats cleaned rows: {recco_clean.height}")

    # -----------------------------
    # 2) Join Last.fm -> MusicBrainz via track MBID
    # -----------------------------
    lastfm_mb = lastfm_clean.join(
        mb_clean.select([
            "lastfm_track_mbid",
            "isrc",
            "resolution_method",
            "matched_recording_mbid",
            "search_score",
            "mb_status",
            "resolved_at_utc",
        ]),
        on="lastfm_track_mbid",
        how="left",
    )

    print(f"After Last.fm + MusicBrainz join: {lastfm_mb.height}")

    # -----------------------------
    # 3) Join with ReccoBeats via ISRC
    # -----------------------------
    enriched = lastfm_mb.join(
        recco_clean,
        left_on="isrc",
        right_on="rb_isrc",
        how="left",
    )

    print(f"After ReccoBeats join: {enriched.height}")

    # -----------------------------
    # 4) Keep only rows usable for the trends simulator
    # -----------------------------
    # For the simulator we want:
    # - track/artist metadata
    # - popularity
    # - ISRC
    # - at least some core audio features
    enriched_final = (
        enriched
        .filter(
            pl.col("isrc").is_not_null() &
            pl.col("lastfm_track_name").is_not_null() &
            pl.col("lastfm_artist_name").is_not_null() &
            pl.col("rb_danceability").is_not_null() &
            pl.col("rb_energy").is_not_null() &
            pl.col("rb_valence").is_not_null()
        )
        .select([
            # identity / linking
            "lastfm_track_mbid",
            "matched_recording_mbid",
            "isrc",

            # track metadata
            "lastfm_track_name",
            "lastfm_artist_name",
            "lastfm_url",
            "lastfm_image_url",

            # popularity / batch-side attributes
            "lastfm_duration",
            "lastfm_listeners",
            "lastfm_playcount",
            "lastfm_rank",
            "source_type",
            "source_value",
            "source_page",

            # audio features for post simulation
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

            # provenance
            "resolution_method",
            "search_score",
            "mb_status",
            "resolved_at_utc",
            "run_id",
            "run_date",
        ])
        .rename({
            "lastfm_track_name": "track_name",
            "lastfm_artist_name": "artist_name",
            "lastfm_duration": "lastfm_duration_sec",
            "lastfm_listeners": "listeners",
            "lastfm_playcount": "playcount",
            "lastfm_rank": "chart_rank",
            "rb_danceability": "danceability",
            "rb_energy": "energy",
            "rb_valence": "valence",
            "rb_tempo": "tempo",
            "rb_acousticness": "acousticness",
            "rb_instrumentalness": "instrumentalness",
            "rb_liveness": "liveness",
            "rb_loudness": "loudness",
            "rb_speechiness": "speechiness",
            "rb_mode": "mode",
            "rb_key": "musical_key",
            "rb_time_signature": "time_signature",
        })
        .sort("playcount", descending=True)
        .unique(subset=["isrc"], keep="first")
    )

    print(f"Final enriched rows: {enriched_final.height}")

    if enriched_final.height == 0:
        raise ValueError("No usable rows after joining. Check ISRC matching and input files.")

    # -----------------------------
    # 5) Write outputs
    # -----------------------------
    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    enriched_final.write_parquet(OUTPUT_PARQUET)
    enriched_final.write_csv(OUTPUT_CSV)

    print(f"Saved parquet: {OUTPUT_PARQUET}")
    print(f"Saved csv: {OUTPUT_CSV}")

    if WRITE_DEMO_SAMPLE:
        sample_n = min(DEMO_SAMPLE_SIZE, enriched_final.height)
        sample_df = enriched_final.sample(n=sample_n, shuffle=True, seed=42)
        sample_df.write_csv(OUTPUT_SAMPLE_CSV)
        print(f"Saved demo sample csv: {OUTPUT_SAMPLE_CSV} ({sample_n} rows)")

    # -----------------------------
    # 6) Quick stats
    # -----------------------------
    print("\nQuick quality check:")
    print(
        enriched_final.select([
            pl.len().alias("rows"),
            pl.col("isrc").n_unique().alias("unique_isrc"),
            pl.col("artist_name").n_unique().alias("unique_artists"),
            pl.col("track_name").n_unique().alias("unique_tracks"),
        ])
    )


if __name__ == "__main__":
    main()