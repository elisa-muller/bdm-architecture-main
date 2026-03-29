# Lightweight Python-based Jupyter image
FROM quay.io/jupyter/base-notebook:python-3.13

USER root
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends curl wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

# ── Python packages ────────────────────────────────────────────────────────────
# deltalake  – Python bindings for delta-rs (Rust implementation, no Spark/JVM)
# polars     – high-performance DataFrame library built on Arrow
# pyarrow    – shared columnar runtime used by both deltalake and polars
# duckdb     – in-process SQL engine that can query Delta tables directly
# boto3      – S3 client and filesystem adapter (for MinIO)
RUN pip install --no-cache-dir \
        "deltalake" \
        "polars" \
        "pyarrow" \
        "duckdb" \
        "pandas" \
        "boto3" \
        "kafka-python==2.3.0"

WORKDIR /home/jovyan
