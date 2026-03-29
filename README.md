# Kafka and Airflow -- Hands-On Lab

Hands-on session introducing Kafka and Airflow.

## Services

| Service | Image | Ports | Role |
|---|---|---|---|
| `kafka` | `apache/kafka:4.2.0` | 9092 | Event streaming (KRaft) |
| `kafka-producer` | built locally | — | Synthetic data generator |
| `minio` | `minio/minio` | 9000, 9001 | S3-compatible object storage |
| `jupyter` | built locally | 8888 | Interactive notebooks |
| `airflow-apiserver` | built locally | 8080 | Airflow UI + API |
| `airflow-scheduler` | built locally | — | DAG trigger scheduling |
| `airflow-worker` | built locally | — | Task execution |
| `postgres` | `postgres:16` | — | Airflow metadata DB |
| `redis` | `redis:7.2-bookworm` | — | Celery task queue |

## Quick start

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up --build -d
docker compose ps   # wait for (healthy) on all services
```

| UI | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| JupyterLab | http://localhost:8888 | token: playground |

## DAGs

| DAG | Schedule | What it does |
|---|---|---|
| `ingest_server_metrics` | every 10 min | Consume Kafka → raw Delta table → per-server aggregates |
| `cleanup_server_metrics` | daily 02:00 UTC | ACID DELETE of bad sensor readings |
