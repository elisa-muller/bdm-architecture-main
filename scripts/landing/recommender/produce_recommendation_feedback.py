from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, time as datetime_time, timedelta, timezone

import boto3
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RECOMMENDATION_FEEDBACK = os.getenv(
    "TOPIC_RECOMMENDATION_FEEDBACK",
    "music-recommendation-feedback",
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
CONSUMPTION_BUCKET = os.getenv("CONSUMPTION_BUCKET", "consumption")
RECOMMENDATION_EVENTS_PREFIX = os.getenv(
    "RECOMMENDATION_EVENTS_PREFIX",
    "recommendations/recommendation_events/",
).strip("/")

MAX_FEEDBACK_EVENTS = int(os.getenv("MAX_FEEDBACK_EVENTS", "5"))
RUN_FOREVER = os.getenv("RUN_FOREVER", "false").lower() == "true"
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1"))
FEEDBACK_EVENT_DAYS_BACK = int(os.getenv("FEEDBACK_EVENT_DAYS_BACK", "0"))

ACCEPT_PROBABILITY = float(os.getenv("FEEDBACK_ACCEPT_PROBABILITY", "0.55"))
PICK_LOWER_RANK_PROBABILITY = float(os.getenv("FEEDBACK_PICK_LOWER_RANK_PROBABILITY", "0.25"))
SKIP_PROBABILITY = float(os.getenv("FEEDBACK_SKIP_PROBABILITY", "0.15"))


def simulated_event_timestamp() -> str:
    now = datetime.now(timezone.utc)
    if FEEDBACK_EVENT_DAYS_BACK <= 0:
        return now.isoformat()

    days_back = random.randint(0, FEEDBACK_EVENT_DAYS_BACK)
    event_date = (now - timedelta(days=days_back)).date()
    event_time = datetime_time(
        hour=random.randint(8, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        tzinfo=timezone.utc,
    )
    return datetime.combine(event_date, event_time).isoformat()


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def list_recommendation_event_keys(client) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=CONSUMPTION_BUCKET, Prefix=RECOMMENDATION_EVENTS_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)
    return sorted(keys)


def load_recommendation_event(client, key: str) -> dict:
    response = client.get_object(Bucket=CONSUMPTION_BUCKET, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def choose_feedback_action(recommendation: dict) -> tuple[str, dict | None]:
    candidates = recommendation.get("recommendations") or []
    if not candidates:
        return "skipped", None

    roll = random.random()
    if roll < ACCEPT_PROBABILITY:
        return "accepted", candidates[0]
    if roll < ACCEPT_PROBABILITY + PICK_LOWER_RANK_PROBABILITY and len(candidates) > 1:
        lower_candidates = candidates[1:]
        weights = [1 / max(1, int(candidate.get("rank", 1))) for candidate in lower_candidates]
        return "selected_candidate", random.choices(lower_candidates, weights=weights, k=1)[0]
    if roll < ACCEPT_PROBABILITY + PICK_LOWER_RANK_PROBABILITY + SKIP_PROBABILITY:
        return "skipped", None

    return "external_selection", None


def build_feedback_event(recommendation: dict, recommendation_event_key: str) -> dict:
    action, selected = choose_feedback_action(recommendation)
    system_selected = {
        "isrc": recommendation.get("selected_isrc") or "",
        "track_name": recommendation.get("selected_track_name") or "",
        "artist_name": recommendation.get("selected_artist_name") or "",
        "rank": recommendation.get("selected_rank"),
        "similarity_score": recommendation.get("selected_similarity_score"),
    }

    if action == "accepted":
        selected = {
            "isrc": system_selected["isrc"],
            "track_name": system_selected["track_name"],
            "artist_name": system_selected["artist_name"],
            "rank": system_selected["rank"],
        }
    elif action == "skipped":
        selected = None
    elif action == "external_selection":
        selected = {
            "isrc": os.getenv("FEEDBACK_EXTERNAL_ISRC", ""),
            "track_name": os.getenv("FEEDBACK_EXTERNAL_TRACK_NAME", ""),
            "artist_name": os.getenv("FEEDBACK_EXTERNAL_ARTIST_NAME", ""),
            "rank": None,
        }

    selected_rank = selected.get("rank") if selected else None
    candidate_count = int(recommendation.get("candidate_count") or 0)
    accepted = action == "accepted"
    skipped = action == "skipped"

    return {
        "feedback_id": str(uuid.uuid4()),
        "request_id": str(recommendation.get("request_id") or ""),
        "event_ts": simulated_event_timestamp(),
        "source": "simulated_recommendation_feedback",
        "platform": str(recommendation.get("platform") or "mock_app"),
        "user_id": os.getenv("FEEDBACK_USER_ID", "mock_user_001"),
        "action": action,
        "selected_isrc": selected.get("isrc", "") if selected else "",
        "selected_track_name": selected.get("track_name", "") if selected else "",
        "selected_artist_name": selected.get("artist_name", "") if selected else "",
        "selected_rank": selected_rank,
        "candidate_count": candidate_count,
        "top_k": int(recommendation.get("top_k") or candidate_count or 0),
        "system_selected_isrc": system_selected["isrc"],
        "system_selected_track_name": system_selected["track_name"],
        "system_selected_artist_name": system_selected["artist_name"],
        "system_selected_rank": system_selected["rank"],
        "system_selected_similarity_score": system_selected["similarity_score"],
        "accepted_system_recommendation": accepted,
        "was_candidate_selection": selected_rank is not None,
        "dwell_time_ms": random.randint(1200, 25000) if not skipped else random.randint(200, 5000),
        "satisfaction_score": (
            random.randint(4, 5)
            if accepted
            else random.randint(2, 4)
            if action == "selected_candidate"
            else random.randint(1, 2)
            if skipped
            else random.randint(2, 5)
        ),
        "recommendation_event_key": recommendation_event_key,
    }


def main() -> None:
    client = build_s3_client()
    keys = list_recommendation_event_keys(client)
    if not keys:
        print(
            "No recommendation events found. Run the recommender first so feedback "
            f"can be simulated from s3://{CONSUMPTION_BUCKET}/{RECOMMENDATION_EVENTS_PREFIX}"
        )
        return

    print(f"Found {len(keys)} recommendation events")
    print(f"Producing feedback events to Kafka topic: {TOPIC_RECOMMENDATION_FEEDBACK}")
    print(f"RUN_FOREVER={RUN_FOREVER}")
    print(f"MAX_FEEDBACK_EVENTS={MAX_FEEDBACK_EVENTS}")
    print(f"FEEDBACK_EVENT_DAYS_BACK={FEEDBACK_EVENT_DAYS_BACK}")

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
            random.shuffle(keys)
            for key in keys:
                recommendation = load_recommendation_event(client, key)
                feedback = build_feedback_event(recommendation, key)
                producer.send(TOPIC_RECOMMENDATION_FEEDBACK, value=feedback)

                sent_count += 1
                print(
                    f"[{sent_count}] Sent feedback: request={feedback['request_id']} "
                    f"action={feedback['action']} selected_rank={feedback['selected_rank']}"
                )

                time.sleep(SLEEP_SECONDS)
                if not RUN_FOREVER and sent_count >= MAX_FEEDBACK_EVENTS:
                    producer.flush()
                    print(
                        f"Done. Sent {sent_count} feedback events to "
                        f"'{TOPIC_RECOMMENDATION_FEEDBACK}'."
                    )
                    return
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
