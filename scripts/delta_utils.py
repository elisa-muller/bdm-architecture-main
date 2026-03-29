"""
delta_utils.py
──────────────
Shared helpers for Delta Lake operations used by both Jupyter notebooks
and Airflow DAGs.

All table operations go through the ``deltalake`` Python library (delta-rs),
a Rust-based implementation that needs no JVM / Spark.

Storage backend: MinIO (S3-compatible).

Usage
-----
    import sys; sys.path.insert(0, "/home/jovyan/scripts")   # Jupyter
    # or
    import sys; sys.path.insert(0, "/opt/airflow/scripts")   # Airflow

    from delta_utils import storage_options, delta_path, write_delta, read_delta
"""

import os
from typing import Any, Dict, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

# ── MinIO / S3-compatible storage options ────────────────────────────────────
# delta-rs uses these when interacting with the object store.
storage_options: Dict[str, str] = {
    "endpoint_url":       os.getenv("MINIO_ENDPOINT",   "http://minio:9000"),
    "access_key_id":      os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_access_key":  os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "region":             os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    # Required for MinIO (path-style access + plain HTTP)
    "allow_http":                   "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME":   "true",
    "AWS_ALLOW_HTTP":               "true",
}

# ── Bucket / path constants ───────────────────────────────────────────────────
BUCKET_DELTA = "s3://deltalake"
BUCKET_RAW   = "s3://raw-data"


def delta_path(table_name: str, bucket: str = BUCKET_DELTA) -> str:
    """Return the S3 path for a Delta table."""
    return f"{bucket}/{table_name}"


# ── Read helpers ──────────────────────────────────────────────────────────────

def read_delta(
    table_name: str,
    version: Optional[int] = None,
    as_polars: bool = False,
):
    """
    Read a Delta table.

    Parameters
    ----------
    table_name : str
        Logical table name (e.g. ``"yellow_taxi"``).
    version : int, optional
        Read a specific historical version (time travel).
    as_polars : bool
        If True, return a ``polars.LazyFrame``; otherwise return a
        ``pyarrow.Table``.
    """
    path = delta_path(table_name)
    dt = DeltaTable(path, storage_options=storage_options, version=version)

    if as_polars:
        import polars as pl
        return pl.scan_delta(path, delta_table_options={"storage_options": storage_options})

    return dt.to_pyarrow_table()


def open_delta(table_name: str, version: Optional[int] = None) -> DeltaTable:
    """Return a raw ``DeltaTable`` object for full control."""
    return DeltaTable(
        delta_path(table_name),
        storage_options=storage_options,
        version=version,
    )


# ── Write helpers ─────────────────────────────────────────────────────────────

def write_delta(
    table_name: str,
    data: Any,
    mode: str = "append",
    partition_by: Optional[list] = None,
    schema_mode: Optional[str] = None,
    **kwargs,
) -> None:
    """
    Write an Arrow Table (or pandas/polars DataFrame) to a Delta table.

    Parameters
    ----------
    table_name : str
        Logical table name.
    data :
        ``pyarrow.Table``, ``pandas.DataFrame``, or ``polars.DataFrame``.
    mode : str
        ``"append"`` | ``"overwrite"`` | ``"error"`` | ``"ignore"``
    partition_by : list[str], optional
        Column names to partition by.
    schema_mode : str, optional
        ``"merge"`` to enable schema evolution on append.
    """
    # Normalise input to PyArrow table
    if hasattr(data, "to_arrow"):          # polars
        data = data.to_arrow()
    elif hasattr(data, "to_pyarrow"):      # pandas via ArrowDtype accessor
        data = data.to_pyarrow()
    elif hasattr(data, "values"):          # pandas DataFrame
        import pandas as pd
        data = pa.Table.from_pandas(data)

    write_deltalake(
        delta_path(table_name),
        data,
        mode=mode,
        partition_by=partition_by,
        schema_mode=schema_mode,
        storage_options=storage_options,
        **kwargs,
    )


# ── Utility helpers ───────────────────────────────────────────────────────────

def table_history(table_name: str, limit: int = 20):
    """Print the commit history of a Delta table as a pandas DataFrame."""
    import pandas as pd
    dt = open_delta(table_name)
    history = dt.history(limit=limit)
    return pd.DataFrame(history)


def table_info(table_name: str) -> dict:
    """Return basic metadata about a Delta table."""
    dt = open_delta(table_name)
    meta = dt.metadata()
    return {
        "table_name":   table_name,
        "id":           meta.id,
        "description":  meta.description,
        "format":       meta.format.provider,
        "version":      dt.version(),
        "files":        len(dt.files()),
        "partitions":   meta.partition_columns,
        "schema":       dt.schema().json(),
    }


def optimize(table_name: str, target_size: int = 256 * 1024 * 1024) -> dict:
    """
    Compact small files in a Delta table (equivalent to OPTIMIZE in SQL).

    Returns a dict with metrics (numFilesAdded, numFilesRemoved, …).
    """
    dt = open_delta(table_name)
    return dt.optimize.compact(target_size=target_size)


def vacuum(table_name: str, retention_hours: int = 168) -> list:
    """
    Remove old data files no longer needed by the Delta table.

    ``retention_hours`` defaults to 7 days (Delta Lake minimum = 168 h).
    Returns the list of deleted files.
    """
    dt = open_delta(table_name)
    return dt.vacuum(retention_hours=retention_hours, dry_run=False)
