from __future__ import annotations

import base64
import json
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
TOPIC_RECOMMENDATION_REQUESTS = os.getenv(
    "TOPIC_RECOMMENDATION_REQUESTS",
    "music-recommendation-requests",
)

IMAGES_SOURCE_PATH = Path(
    os.getenv("RECOMMENDATION_IMAGES_SOURCE_PATH", "/opt/airflow/data_sources/images")
)

MAX_REQUESTS = int(os.getenv("MAX_RECOMMENDATION_REQUESTS", "5"))
RUN_FOREVER = os.getenv("RUN_FOREVER", "false").lower() == "true"
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1"))
SHUFFLE_IMAGES = os.getenv("SHUFFLE_IMAGES", "true").lower() == "true"

RESIZE_MAX_DIM = int(os.getenv("RESIZE_MAX_DIM", "512"))
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "75"))

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


def compress_image_for_stream(image_path: Path) -> tuple[bytes, str, str]:
    with Image.open(image_path) as img:
        img = img.convert("RGB")
        img.thumbnail((RESIZE_MAX_DIM, RESIZE_MAX_DIM))

        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=JPEG_QUALITY, optimize=True)
        content = buffer.getvalue()

    return content, ".jpg", "image/jpeg"


def build_event(image_path: Path) -> dict:
    content, file_extension, mime_type = compress_image_for_stream(image_path)
    request_id = str(uuid.uuid4())

    return {
        "request_id": request_id,
        "event_id": request_id,
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "simulated_recommendation_request",
        "platform": "mock_app",
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
        path for path in IMAGES_SOURCE_PATH.iterdir()
        if path.is_file() and path.suffix.lower() in ALLOWED_EXTENSIONS
    ]

    if not image_files:
        print(f"No valid image files found in {IMAGES_SOURCE_PATH}")
        return

    print(f"Found {len(image_files)} images total")
    print(f"Producing recommendation requests to Kafka topic: {TOPIC_RECOMMENDATION_REQUESTS}")
    print(f"RUN_FOREVER={RUN_FOREVER}")
    print(f"MAX_RECOMMENDATION_REQUESTS={MAX_REQUESTS}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=100,
        request_timeout_ms=30000,
        max_block_ms=30000,
    )

    sent_count = 0

    try:
        while True:
            current_batch = image_files[:]
            if SHUFFLE_IMAGES:
                random.shuffle(current_batch)

            for image_path in current_batch:
                event = build_event(image_path)
                producer.send(TOPIC_RECOMMENDATION_REQUESTS, value=event)

                sent_count += 1
                print(f"[{sent_count}] Sent request: {image_path.name} -> {event['request_id']}")

                time.sleep(SLEEP_SECONDS)

                if not RUN_FOREVER and sent_count >= MAX_REQUESTS:
                    producer.flush()
                    print(
                        f"Done. Sent {sent_count} recommendation requests to "
                        f"'{TOPIC_RECOMMENDATION_REQUESTS}'."
                    )
                    return

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
