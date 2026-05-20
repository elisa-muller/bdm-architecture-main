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

Run these Airflow DAGs before using the dashboard with real data:

1. `init_platform`
2. `structured_batch`
3. `trusted_structured_cleaning_pipeline`
4. `exploitation_structured_song_audio_features_pipeline`
5. `trusted_trends_pipeline`
6. `exploitation_recommender_song_features_pipeline`
7. `song_embeddings_to_milvus_pipeline`
8. `trusted_recommendation_feedback_pipeline`
9. `exploitation_recommendation_feedback_pipeline`

The dashboard can still open before the tables exist. In that case it shows a waiting-for-data message.

## Dashboard Tabs

- `Trend Intelligence`: top songs, posts, views, engagement, hashtags and regions.
- `Music Features`: energy, valence, danceability and tempo exploration.
- `Recommendations`: acceptance rate, skip rate, satisfaction and feedback outcomes.
- `Data Status`: row counts and freshness for each Exploitation Zone dataset.
