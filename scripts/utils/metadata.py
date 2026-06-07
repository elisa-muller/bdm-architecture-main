"""Operational metadata helpers for Bronze ingestion.

Bronze ingestion jobs all follow the same pattern: read from an external
source, write a temporal raw artifact, migrate or persist it, and record a
small JSON run report. Later zones keep transformation-specific metadata in
their own scripts because their quality and lineage details differ by job.
"""

import json
from datetime import datetime, timezone
from typing import Any


SCHEMA_VERSION = "1.0"


def utc_now_iso() -> str:
    """Return a UTC timestamp in a stable JSON-friendly format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def create_metadata_record(
    data_type: str,  # "unstructured", "semi_structured", "structured"
    format_name: str,  # "image", "jsonl", "csv", etc.
    source: str,  # "kafka", "batch"
    temporal_path: str,
    persistent_path: str,
    record_count: int,
    attributes: dict[str, Any] | None = None,
    *,
    dataset_name: str | None = None,
    source_system: str | None = None,
    run_id: str | None = None,
    source_path: str | None = None,
    quality_summary: dict[str, Any] | None = None,
) -> dict:
    """Create a standardized Bronze ingestion metadata record.

    The original field names are kept for compatibility with the existing
    DAGs, while the explicit zone/stage/source/target fields make the record
    easier to use as governance metadata.
    """
    timestamp = utc_now_iso()
    source_uri = source_path or source_system or source
    temporal_uri = temporal_path if temporal_path.startswith("s3://") else temporal_path
    persistent_uri = (
        persistent_path if persistent_path.startswith("s3://") else persistent_path
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "metadata_type": "operational_ingestion_run",
        "zone": "bronze",
        "stage": "landing_ingestion",
        "timestamp": timestamp,
        "timestamp_utc": timestamp,
        "run_id": run_id,
        "dataset_name": dataset_name,
        "data_type": data_type,
        "format": format_name,
        "source": source,
        "source_system": source_system or source,
        "temporal_path": temporal_path,
        "persistent_path": persistent_path,
        "source_uris": [source_uri] if source_uri else [],
        "target_uris": [temporal_uri, persistent_uri],
        "record_count": record_count,
        "quality_summary": quality_summary or {},
        "attributes": attributes or {},
    }


def metadata_object_key(prefix: str, metadata: dict) -> str:
    """Build a deterministic object key for a metadata JSON document."""
    safe_timestamp = metadata["timestamp"].replace(":", "-")
    run_id = metadata.get("run_id") or safe_timestamp
    return f"{prefix.rstrip('/')}/{run_id}.json"


def metadata_json_bytes(metadata: dict) -> bytes:
    """Serialize metadata with stable formatting."""
    return json.dumps(metadata, indent=2, sort_keys=True).encode("utf-8")


def write_metadata_boto3(s3_client: Any, bucket: str, key: str, metadata: dict) -> str:
    """Write a metadata record with a boto3 S3 client."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=metadata_json_bytes(metadata),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def write_metadata_minio(minio_client: Any, bucket: str, key: str, metadata: dict) -> str:
    """Write a metadata record with the MinIO Python client."""
    from io import BytesIO

    body = metadata_json_bytes(metadata)
    minio_client.put_object(
        bucket_name=bucket,
        object_name=key,
        data=BytesIO(body),
        length=len(body),
        content_type="application/json",
    )
    return f"s3://{bucket}/{key}"
