"""Minimal metadata tracking for Bronze ingestion."""

import json
from datetime import datetime


def create_metadata_record(
    data_type: str,  # "unstructured", "semi_structured", "structured"
    format_name: str,  # "image", "jsonl", "csv", etc.
    source: str,  # "kafka", "batch"
    temporal_path: str,
    persistent_path: str,
    record_count: int,
    attributes: dict | None = None,
) -> dict:
    """Create a metadata record for ingested data."""
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "data_type": data_type,
        "format": format_name,
        "source": source,
        "temporal_path": temporal_path,
        "persistent_path": persistent_path,
        "record_count": record_count,
        "attributes": attributes or {},
    }
