from __future__ import annotations

import base64
import json
import mimetypes
import os
import random
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from kafka import KafkaProducer
from PIL import Image


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IMAGES_RAW = os.getenv("TOPIC_IMAGES_RAW", "music-images-raw")

IMAGES_SOURCE_PATH = Path(
    os.getenv("IMAGES_SOURCE_PATH", "/opt/airflow/data_sources/images")
)

MAX_IMAGES = int(os.getenv("MAX_IMAGES", "10"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1"))
SHUFFLE_IMAGES = os.getenv("SHUFFLE_IMAGES", "true").lower() == "true"

# Optional resizing/compression for safer Kafka demo traffic
RESIZE_MAX_DIM = int(os.getenv("RESIZE_MAX_DIM", "512"))
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "75"))

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


def compress_image_for_stream(image_path: Path) -> tuple[bytes, str, str]:
    """
    Open image, resize it to a reasonable max dimension, and encode as JPEG bytes.
    Returns: (binary_content, file_extension, mime_type)
    """
    with Image.open(image_path) as img:
        img = img.convert("RGB")
        img.thumbnail((RESIZE_MAX_DIM, RESIZE_MAX_DIM))

        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=JPEG_QUALITY, optimize=True)
        content = buffer.getvalue()

    return content, ".jpg", "image/jpeg"


def build_event(image_path: Path) -> dict:
    content, file_extension, mime_type = compress_image_for_stream(image_path)

    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "simulated_image_stream",
        "platform": "mock_social",
        "image_name": image_path.name,
        "original_extension": image_path.suffix.lower(),
        "file_extension": file_extension,
        "mime_type": mime_type,
        "content_b64": base64.b64encode(content).decode("utf-8"),
    }


def main() -> None:
    if not IMAGES_SOURCE_PATH.exists():
        raise FileNotFoundError(f"Images folder not found: {IMAGES_SOURCE_PATH}")

    image_files = [
        p for p in IMAGES_SOURCE_PATH.iterdir()
        if p.is_file() and p.suffix.lower() in ALLOWED_EXTENSIONS
    ]

    if not image_files:
        print(f"No valid image files found in {IMAGES_SOURCE_PATH}")
        return

    if SHUFFLE_IMAGES:
        random.shuffle(image_files)

    selected_images = image_files[:MAX_IMAGES]

    print(f"Found {len(image_files)} images total")
    print(f"Sending {len(selected_images)} images")
    print(f"SLEEP_SECONDS={SLEEP_SECONDS}")
    print(f"RESIZE_MAX_DIM={RESIZE_MAX_DIM}, JPEG_QUALITY={JPEG_QUALITY}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
        request_timeout_ms=30000,
        max_block_ms=30000,
    )

    try:
        for i, image_path in enumerate(selected_images, start=1):
            event = build_event(image_path)
            producer.send(TOPIC_IMAGES_RAW, value=event)
            print(f"[{i}/{len(selected_images)}] Sent: {image_path.name}")
            time.sleep(SLEEP_SECONDS)

        producer.flush()
        print(f"Done. Sent {len(selected_images)} image events to '{TOPIC_IMAGES_RAW}'.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()


""" 
in order to trigger the image streaming: keep it manual (for now)
docker compose exec -e MAX_IMAGES=5 -e SLEEP_SECONDS=1 airflow-worker python /opt/airflow/scripts/produce_images.py
"""