# BDM P2: Music Intelligence Lakehouse

This repository contains the second part of the Big Data Management architecture project. It extends the P1 Landing Zone into a complete local lakehouse-style platform with Bronze, Trusted, and Exploitation layers, orchestrated with Airflow and supported by Kafka, MinIO, Spark, Milvus, and a Streamlit dashboard.

The platform integrates four data families:

- Structured music metadata and audio features from Last.fm, MusicBrainz, and ReccoBeats.
- Semi-structured social trend events generated from enriched seed data.
- Unstructured image events used by the recommendation flow.
- Recommendation feedback events used to evaluate user response.

## Repository Structure

| Path | Purpose |
|---|---|
| `compose.yaml` | Docker Compose definition for the complete local platform |
| `docker/` | Dockerfiles for Airflow, Spark, Jupyter, producers, APIs, and dashboard services |
| `config/` | Airflow and Kafka UI configuration files |
| `dags/` | Airflow DAGs from `00_init` to `14_feedback_metrics` |
| `scripts/landing/` | Raw producers and batch ingestion scripts |
| `scripts/trusted/` | Trusted-zone cleaning jobs |
| `scripts/exploitation/` | Feature, embedding, indexing, and metric builders |
| `scripts/consumption/dashboard/` | Streamlit dashboard for the Exploitation outputs |
| `data_sources/` | Local preloaded images, trend seeds, and structured seed files |

## Architecture

The project is organized into the following zones:

- `Bronze`: raw and persistent ingested data from Kafka streams and batch APIs.
- `Trusted`: cleaned and standardized datasets for images, trends, music, and feedback.
- `Exploitation`: analytical outputs, recommender features, embeddings, Milvus indexes, and dashboard-ready metrics.

Airflow coordinates ingestion, cleaning, feature generation, embedding creation, Milvus indexing, and feedback analytics. Scheduled DAGs handle continuously arriving data, while heavier downstream transformations are manual or triggered after upstream success.

The architecture combines streaming and batch processing:

- Image and trend events are produced continuously into Kafka and consumed by scheduled Airflow DAGs.
- Structured music data is collected through batch jobs that combine Last.fm tracks, MusicBrainz ISRC enrichment, and ReccoBeats audio features.
- Trusted jobs standardize raw inputs and write cleaned Delta-style datasets to MinIO.
- Exploitation jobs create song audio features, trend aggregates, recommender features, embedding snapshots, and dashboard-ready metrics.
- Milvus stores song embeddings so image-based recommendation requests can retrieve similar songs.
- Feedback events close the loop by connecting recommendations with user actions and satisfaction scores.

## Main Components

| Component | Role |
|---|---|
| Kafka | Streaming buffer for images, trends, recommendation requests, and feedback |
| MinIO | S3-compatible storage for Bronze, Trusted, and Exploitation zones |
| Airflow | Orchestration of all DAGs |
| Spark | Batch transformations and feature generation |
| Milvus | Vector database for song embeddings |
| Streamlit | Consumption dashboard for analytics and validation |
| Jupyter | Optional exploration environment |

## Airflow DAG Groups

| DAG range | Purpose |
|---|---|
| `00_init` | Initialize MinIO buckets, folder structure, and Kafka topics |
| `01`-`03` | Raw ingestion for images, trends, and structured music data |
| `04`-`08` | Trusted-zone cleaning for images, trends, and music |
| `09`-`11` | Song features, recommender features, and Milvus song index |
| `12`-`14` | Recommendation feedback ingestion, cleaning, and metrics |

## Orchestration Summary

The project does not use one large master DAG. Each DAG keeps a specific responsibility and either runs on a schedule or is triggered after its upstream data is ready.

Main orchestration paths:

```text
03_raw_music -> 08_trusted_music -> 09_song_features -> 10_recommender_features -> 11_song_index
01_raw_images -> 04_trusted_images -> 05_image_embeddings
02_raw_trends -> 06_trusted_trends -> 07_trend_features
12_raw_feedback -> 13_trusted_feedback -> 14_feedback_metrics
```

This separation makes the system easier to validate because each layer can be checked independently before the next one runs.

## Data Flow

```text
Streaming image and trend producers
        -> Kafka topics
        -> Bronze raw storage in MinIO
        -> Trusted cleaning jobs
        -> Exploitation features and metrics
        -> Dashboard and recommender outputs

Structured music batch APIs
        -> Bronze structured storage
        -> Trusted music dataset
        -> Song audio features
        -> Recommender feature table
        -> Song embedding index in Milvus

Recommendation feedback stream
        -> Bronze feedback events
        -> Trusted feedback dataset
        -> Recommendation outcome metrics
        -> Dashboard KPIs
```

## Main Outputs

The final Exploitation Zone produces the assets used for analysis and consumption:

- Song audio feature tables built from structured music data.
- Song trend aggregate tables built from semi-structured trend events.
- Recommender feature tables joining music and trend information.
- Song embedding snapshots and Milvus indexes for similarity search.
- Trusted image embeddings for image-based recommendation experiments.
- Recommendation outcome and feedback summary tables for evaluation.

## Dashboard

The Streamlit dashboard consumes Exploitation outputs and provides a visual validation layer for the project. It is used to inspect:

- Music feature availability.
- Trend aggregation outputs.
- Recommendation outcomes.
- Feedback acceptance, skips, and satisfaction scores.
- Data availability across the final analytical tables.

The dashboard is not the orchestration layer. Airflow remains responsible for running and validating the pipeline.

## Data Sources

The repository expects the required environment and preload files described in the report:

- `.env`
- `config/airflow.cfg`
- `config/kafkaui_config.yml`
- `data_sources/images/`
- `data_sources/trends_seed/`

The `data_sources/` folder contains local inputs used to avoid repeating time-consuming preparation steps during evaluation.

## Validation Focus

The project is considered ready when:

- Airflow imports all DAGs without errors.
- `00_init` creates the required MinIO buckets, folder structure, and Kafka topics.
- Raw ingestion DAGs write data to Bronze.
- Trusted DAGs clean and standardize the raw inputs.
- Exploitation DAGs generate the expected feature, embedding, index, and metric outputs.
- The dashboard can read the Exploitation outputs without missing-table errors.

The report contains the exact validation order and the commands or UI actions to use.
