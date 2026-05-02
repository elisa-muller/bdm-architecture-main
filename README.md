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
2. Trigger the DAG: `init_platform`

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
| DAG | `consume_images_raw_to_bronze` |
| Schedule | every 1 minute |
| Output | `bronze/temporal/unstructured/images/raw/` |

---

### Trends stream (hot path)

- `trends-api` simulates social media posts  
- `trends-producer` streams continuously  
- Kafka topic: `music-trends-raw`

| Component | Value |
|---|---|
| DAG | `consume_trends_raw_to_bronze` |
| Schedule | every 5 minutes |
| Output | `bronze/temporal/semi_structured/trends/raw/` |

(JSONL files)

---

### Structured batch (cold path)

| Property | Value |
|---|---|
| DAG | `structured_batch` |
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
| `init_platform` | manual | Initialize Kafka topics and MinIO bucket |
| `consume_images_raw_to_bronze` | every 1 min | Ingest → temporal → migrate to persistent → record metadata |
| `consume_trends_raw_to_bronze` | every 5 min | Ingest → temporal → migrate to persistent → record metadata |
| `structured_batch` | daily | Batch ingestion + enrichment pipeline |
| `trusted_trends_pipeline` | every 15 min | Spark cleaning of persistent semistructured trends into Trusted Delta |

---

## Orchestration

| Component | Behavior |
|---|---|
| Producers | Continuous |
| Kafka | Streaming buffer |
| Airflow consumers | Scheduled batch |
| Structured batch | Daily execution |

Execution flow:

```
Streaming → Kafka → Airflow → MinIO (Bronze)
Batch → APIs → Airflow → structured outputs
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
