from __future__ import annotations

import json
import os
import time

import requests
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TRENDS_RAW = os.getenv("TOPIC_TRENDS_RAW", "music-trends-raw")

TRENDS_API_BASE_URL = os.getenv("TRENDS_API_BASE_URL", "http://trends-api:8000")
STREAM_ENDPOINT = os.getenv("TRENDS_API_STREAM_ENDPOINT", "/stream")

MAX_POSTS = int(os.getenv("MAX_POSTS", "10"))
RUN_FOREVER = os.getenv("RUN_FOREVER", "false").lower() == "true"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
RETRY_SLEEP_SECONDS = float(os.getenv("RETRY_SLEEP_SECONDS", "3"))


def main() -> None:
    stream_url = f"{TRENDS_API_BASE_URL.rstrip('/')}{STREAM_ENDPOINT}"

    print(f"Connecting to API stream: {stream_url}")
    print(f"Producing to Kafka topic: {TOPIC_TRENDS_RAW}")
    print(f"RUN_FOREVER={RUN_FOREVER}, MAX_POSTS={MAX_POSTS}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
        request_timeout_ms=30000,
        max_block_ms=30000,
    )

    sent = 0

    try:
        while True:
            try:
                with requests.get(stream_url, stream=True, timeout=REQUEST_TIMEOUT) as response:
                    response.raise_for_status()

                    for line in response.iter_lines(decode_unicode=True):
                        if not line:
                            continue

                        event = json.loads(line)
                        producer.send(TOPIC_TRENDS_RAW, value=event)
                        sent += 1

                        print(
                            f"[{sent}] Sent trends event for "
                            f"{event.get('artist')} - {event.get('track')} "
                            f"({event.get('scene')})"
                        )

                        if not RUN_FOREVER and sent >= MAX_POSTS:
                            producer.flush()
                            print(f"Done. Sent {sent} trend events.")
                            return

            except requests.RequestException as e:
                print(f"API connection error: {e}")
                print(f"Retrying in {RETRY_SLEEP_SECONDS} seconds...")
                time.sleep(RETRY_SLEEP_SECONDS)

    finally:
        producer.close()


if __name__ == "__main__":
    main()