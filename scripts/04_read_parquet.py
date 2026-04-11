import pandas as pd

df = pd.read_parquet(
    "s3://bronze/persistent/structured/reccobeats/delta/audio_features_delta/part-00000-426d145c-3de3-4fab-8db5-f2a069c95aa4-c000.snappy.parquet",
    storage_options={
        "key": "minioadmin",
        "secret": "minioadmin",
        "client_kwargs": {"endpoint_url": "http://localhost:9000"},
        "use_ssl": False
    }
)

print(df.head())
print(df.columns.tolist())
print(df.shape)
print(df.dtypes)

df.to_csv("audio_features.csv", index=False)
print("Saved to audio_features.csv")