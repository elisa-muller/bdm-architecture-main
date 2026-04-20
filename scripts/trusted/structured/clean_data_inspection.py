import os
from deltalake import DeltaTable


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ENDPOINT = MINIO_ENDPOINT.replace("http://", "").replace("https://", "").rstrip("/")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

storage_options = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "AWS_ENDPOINT_URL": f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}",
    "AWS_ALLOW_HTTP": "false" if MINIO_SECURE else "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


TABLES = {
    "tracks_clean": "s3://trusted/structured/lastfm/delta/tracks_clean_delta",
    "isrc_clean": "s3://trusted/structured/musicbrainz/delta/isrc_clean_delta",
    "audio_features_clean": "s3://trusted/structured/reccobeats/delta/audio_features_clean_delta",
}


def inspect_table(name: str, uri: str) -> None:
    print("\n" + "=" * 100)
    print(f"TABLE: {name}")
    print(f"URI: {uri}")
    print("=" * 100)

    try:
        dt = DeltaTable(uri, storage_options=storage_options)
        df = dt.to_pandas()
    except Exception as e:
        print(f"Could not read table: {e}")
        return

    print("\nHead:")
    print(df.head(10))

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nShape:")
    print(df.shape)

    print("\nDtypes:")
    print(df.dtypes)

    print("\nNull counts:")
    print(df.isna().sum())

    # Extra useful summaries depending on table
    if name == "tracks_clean":
        if "trusted_track_key" in df.columns:
            print("\nUnique trusted_track_key:")
            print(df["trusted_track_key"].nunique())

        if "track_mbid" in df.columns:
            print("\nRows with track_mbid:")
            print(df["track_mbid"].notna().sum())

        if "artist_mbid" in df.columns:
            print("\nRows with artist_mbid:")
            print(df["artist_mbid"].notna().sum())

    elif name == "isrc_clean":
        if "trusted_track_key" in df.columns:
            print("\nUnique trusted_track_key:")
            print(df["trusted_track_key"].nunique())

        if "is_valid_isrc" in df.columns:
            print("\nRows with valid ISRC:")
            print(df["is_valid_isrc"].fillna(False).sum())

        if "is_resolved" in df.columns:
            print("\nRows resolved:")
            print(df["is_resolved"].fillna(False).sum())

        if "mb_status" in df.columns:
            print("\nmb_status value counts:")
            print(df["mb_status"].value_counts(dropna=False))

        if "resolution_method" in df.columns:
            print("\nresolution_method value counts:")
            print(df["resolution_method"].value_counts(dropna=False))

    elif name == "audio_features_clean":
        if "isrc" in df.columns:
            print("\nUnique ISRC:")
            print(df["isrc"].nunique())

        feature_cols = [
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
        present_feature_cols = [col for col in feature_cols if col in df.columns]

        if present_feature_cols:
            print("\nDescriptive stats for audio features:")
            print(df[present_feature_cols].describe())

    print("\nDone inspecting table.")


def main():
    for name, uri in TABLES.items():
        inspect_table(name, uri)


if __name__ == "__main__":
    main()
