# Streamlit Dashboard

This dashboard is the consumption layer for the music intelligence platform. It reads curated Delta
tables from the Exploitation Zone in MinIO.

## Run

```bash
docker compose up -d streamlit-dashboard
```

Open:

```text
http://localhost:8501
```

## Recommended Upstream DAGs

Run these Airflow DAGs before using the dashboard with real data. Some downstream DAGs are triggered automatically after their upstream DAG succeeds.

For a quick trend-focused demo:

1. `00_init`
2. `01_raw_images`
3. `02_raw_trends`
4. `04_trusted_images`
5. `06_trusted_trends`
6. `07_trend_features`

For the full structured/recommender refresh:

1. `03_raw_music`
2. `08_trusted_music`
3. `09_song_features`
4. `10_recommender_features`
5. `11_song_index`
6. `12_raw_feedback`
7. `13_trusted_feedback`
8. `14_feedback_metrics`

`03_raw_music` is a full live-API batch and may take a long time.

The dashboard can still open before the tables exist. In that case it shows a waiting-for-data message.

## Dashboard Tabs

- `Trend Intelligence`: top songs, posts, views, engagement, hashtags and regions.
- `Music Features`: energy, valence, danceability and tempo exploration.
- `Recommendations`: acceptance rate, skip rate, satisfaction and feedback outcomes.
- `Data Status`: row counts and freshness for each Exploitation Zone dataset.
