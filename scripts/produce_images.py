from _future_ import annotations

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
TOPIC_IMAGES_RAW = os.getenv("TOPIC_IMAGES_RAW", "music-images-raw")

IMAGES_SOURCE_PATH = Path(
    os.getenv("IMAGES_SOURCE_PATH", "/opt/airflow/data_sources/images")
)

MAX_IMAGES = int(os.getenv("MAX_IMAGES", "10"))
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

    print(f"Found {len(image_files)} images total")
    print(f"RUN_FOREVER={RUN_FOREVER}")
    print(f"MAX_IMAGES={MAX_IMAGES}")
    print(f"SLEEP_SECONDS={SLEEP_SECONDS}")
    print(f"RESIZE_MAX_DIM={RESIZE_MAX_DIM}, JPEG_QUALITY={JPEG_QUALITY}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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
                producer.send(TOPIC_IMAGES_RAW, value=event)

                sent_count += 1
                print(f"[{sent_count}] Sent: {image_path.name}")

                time.sleep(SLEEP_SECONDS)

                if not RUN_FOREVER and sent_count >= MAX_IMAGES:
                    producer.flush()
                    print(
                        f"Done. Sent {sent_count} image events to '{TOPIC_IMAGES_RAW}'."
                    )
                    return

    finally:
        producer.flush()
        producer.close()


if _name_ == "_main_":
    main()