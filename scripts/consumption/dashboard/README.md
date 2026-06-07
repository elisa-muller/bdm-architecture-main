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

Run these Airflow DAGs before using the dashboard with real data. Some downstream DAGs are triggered automatically after their upstream DAG succeeds:

1. `00_init`
2. `01_raw_images`
3. `02_raw_trends`
4. `03_raw_music`
5. `04_trusted_images`
6. `05_image_embeddings`
7. `06_trusted_trends`
8. `07_trend_features`
9. `08_trusted_music`
10. `09_song_features`
11. `10_recommender_features`
12. `11_song_index`
13. `12_raw_feedback`
14. `13_trusted_feedback`
15. `14_feedback_metrics`

The dashboard can still open before the tables exist. In that case it shows a waiting-for-data message.

## Dashboard Tabs

- `Trend Intelligence`: top songs, posts, views, engagement, hashtags and regions.
- `Music Features`: energy, valence, danceability and tempo exploration.
- `Recommendations`: acceptance rate, skip rate, satisfaction and feedback outcomes.
- `Data Status`: row counts and freshness for each Exploitation Zone dataset.
