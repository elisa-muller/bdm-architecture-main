# BDM P1: Music Data Platform — Pipeline

Hands-on project implementing a data platform combining streaming (Kafka) and batch processing (Airflow), using MinIO as Bronze data lake storage.

---

## Services

| Service | Image | Ports | Role |
|---|---|---|---|
| `kafka` | `apache/kafka:4.2.0` | 9092 | Event streaming |
| `kafka-ui` | `provectuslabs/kafka-ui` | 8081 | Kafka monitoring |
| `minio` | `minio/minio` | 9000, 9001 | S3-compatible data lake (Bronze) |
| `airflow-apiserver` | built locally | 8080 | Airflow UI + API |
| `airflow-scheduler` | built locally | — | DAG scheduling |
| `airflow-worker` | built locally | — | Task execution |
| `postgres` | `postgres:16` | — | Airflow metadata DB |
| `redis` | `redis:7.2-bookworm` | — | Celery queue |
| `jupyter-playground` | built locally | 8888 | Exploration |
| `trends-api` | built locally | — | Simulated trends API |
| `image-producer` | built locally | — | Image streaming producer |
| `trends-producer` | built locally | — | Trends streaming producer |

---

## Quick start

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up --build -d
docker compose ps   # wait until all services are healthy
```

---

## UIs

| UI | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8081 | — |

---

## Initialization (first time only)

After starting the services:

1. Open Airflow UI  
2. Trigger the DAG: `00_init`

This will:

- create Kafka topics  
- create MinIO bucket (`bronze`)  
- initialize folder structure  

---

## Pipelines

### Image stream (hot path)

- Continuous producer (`RUN_FOREVER=true`)
- Sends 1 image every **3 seconds**
- Kafka topic: `music-images-raw`

| Component | Value |
|---|---|
| DAG | `01_raw_images` |
| Schedule | every 1 minute |
| Output | `bronze/temporal/unstructured/images/raw/` |

---

### Trends stream (hot path)

- `trends-api` simulates social media posts  
- `trends-producer` streams continuously  
- Kafka topic: `music-trends-raw`

| Component | Value |
|---|---|
| DAG | `02_raw_trends` |
| Schedule | every 5 minutes |
| Output | `bronze/temporal/semi_structured/trends/raw/` |

(JSONL files)

---

### Structured batch (cold path)

| Property | Value |
|---|---|
| DAG | `03_raw_music` |
| Schedule | daily (00:00) |
| Type | batch processing |

Pipeline:

```
Last.fm → MusicBrainz → ReccoBeats → enriched dataset
```

Used to:
- enrich music metadata  
- generate structured datasets  
- feed the trends simulation  

---

## DAGs

| DAG | Schedule | What it does |
|---|---|---|
| `00_init` | manual | Initialize Kafka topics, MinIO buckets and folder structure |
| `01_raw_images` | every 1 min | Ingest image events into Bronze |
| `02_raw_trends` | every 5 min | Ingest social trend events into Bronze |
| `03_raw_music` | daily/manual | Landing structured ingestion: Last.fm → MusicBrainz → ReccoBeats |
| `04_trusted_images` | every 15 min | Clean Bronze images into Trusted |
| `05_image_embeddings` | triggered/manual | Generate image embeddings from Trusted images |
| `06_trusted_trends` | every 15 min | Clean Bronze trend events into Trusted |
| `07_trend_features` | triggered/manual | Aggregate Trusted trends into song-level features |
| `08_trusted_music` | manual | Clean structured music data into Trusted |
| `09_song_features` | manual | Build song-level audio features |
| `10_recommender_features` | manual | Join audio features with trend features for recommendation |
| `11_song_index` | triggered/manual | Generate song embeddings and upsert them into Milvus |
| `12_raw_feedback` | every 5 min | Consume recommendation feedback into Bronze |
| `13_trusted_feedback` | triggered/manual | Clean recommendation feedback into Trusted |
| `14_feedback_metrics` | triggered/manual | Build feedback summary and outcome tables for the dashboard |

---

## Orchestration

The orchestration is defined through schedules and downstream DAG triggers, following the same style as the P1 pipeline. There is no extra master DAG. Each DAG keeps one clear responsibility, and the DAGs that produce inputs for the next layer trigger the next DAG when they finish successfully.

In Airflow, unpause the scheduled DAGs if you want them to run automatically. For a shorter demo, trigger them manually in the order shown below.

| Component | Behavior |
|---|---|
| Producers | Continuous |
| Kafka | Streaming buffer |
| Airflow consumers | Scheduled batch |
| Structured batch | Daily execution |
| Downstream triggers | Move data from one layer to the next after successful runs |

Execution flow:

```
03_raw_music → 08_trusted_music → 09_song_features → 10_recommender_features → 11_song_index
04_trusted_images → 05_image_embeddings
06_trusted_trends → 07_trend_features
12_raw_feedback → 13_trusted_feedback → 14_feedback_metrics
```

---

## Expected behavior

After initialization:

### Kafka

- `music-images-raw` receives continuous events  
- `music-trends-raw` receives continuous events  

### Airflow

- Image DAG runs every minute  
- Trends DAG runs every 5 minutes  
- Batch DAG runs daily  

### MinIO (Bronze Layer)

```
bronze/
 ├── temporal/
 │   ├── unstructured/
 │   │   └── images/raw/             ← temporary staging
 │   ├── semi_structured/
 │   │   └── trends/raw/             ← temporary staging
 │   └── structured/
 │       └── csv/                    ← temporary staging
 │
 ├── persistent/
 │   ├── unstructured/
 │   │   └── images/raw/             ← migrated from temporal
 │   ├── semi_structured/
 │   │   └── trends/raw/             ← migrated from temporal
 │   └── structured/
 │       ├── lastfm/
 │       ├── musicbrainz/
 │       └── reccobeats/
 │
 └── metadata/
     ├── unstructured/
     │   └── image/                  ← ingestion records
     └── semi_structured/
         └── jsonl/                  ← ingestion records
```

| Layer | Purpose | Retention |
|---|---|---|
| `temporal` | Temporary ingestion staging | Migrated immediately to persistent |
| `persistent` | Validated long-term Bronze storage | Kept for historical analysis |
| `metadata` | Data lineage and catalog records | Historical metadata tracking |

#### Temporal layer
- **Purpose**: Temporary staging zone for raw ingestion
- **Retention**: Data is **immediately migrated to persistent** after validation
- **Content**:
  - `unstructured/images/raw/` → image stream files (temporary)
  - `semi_structured/trends/raw/` → JSONL events (temporary)
  - `structured/csv/` → raw batch files (temporary)

#### Persistent layer
- **Purpose**: Long-term Bronze validated storage
- **Content**:
  - `unstructured/images/raw/` → migrated image files (validated, with metadata)
  - `semi_structured/trends/raw/` → migrated JSONL events (validated, with metadata)
  - `structured/` → batch processing results (Last.fm, MusicBrainz, ReccoBeats)

#### Metadata layer
- **Purpose**: Data catalog, lineage, and ingestion tracking
- **Records stored as JSON** with:
  - Ingestion timestamp (ISO 8601)
  - Data type and format
  - Source system
  - Storage paths (temporal → persistent migration)
  - Record count
  - Custom attributes (topic, migration status, etc.)
- **Location**: `metadata/{data_type}/{format}/` in Bronze bucket

#### Data flow

```
Streaming (images, trends)
        ↓
Kafka
        ↓
Airflow consumer DAG
        ↓
Bronze / TEMPORAL (staging)
        ↓
    [MIGRATE]
        ↓
Bronze / PERSISTENT
        ↓
Record metadata → Bronze / METADATA
        ↓
   [CLEANUP: Temporal cleared]

Batch APIs (Last.fm, MusicBrainz, ReccoBeats)
        ↓
Airflow batch DAG
        ↓
Bronze / PERSISTENT (structured)
        ↓
Record metadata
```

This ensures:
- **Temporal** is a true temporary staging zone
- **Unstructured & semi-structured data** are properly moved to persistent storage
- **Metadata** tracks all ingestions for data governance and lineage

---

## Validation

| Component | What to check |
|---|---|
| Kafka UI | Topics `music-images-raw`, `music-trends-raw` |
| Airflow | DAGs running, tasks in green |
| MinIO | New image files and `.jsonl` trend files |
