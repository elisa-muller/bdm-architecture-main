from __future__ import annotations

import json
import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from fastapi import FastAPI
from fastapi.responses import StreamingResponse


TRENDS_SEED_PATH = Path(
    os.getenv(
        "TRENDS_SEED_PATH",
        "/opt/airflow/data_sources/trends_seed/trends_seed_enriched.parquet",
    )
)

MAX_TRACKS_IN_MEMORY = int(os.getenv("MAX_TRACKS_IN_MEMORY", "5000"))
STREAM_DELAY_SECONDS = float(os.getenv("STREAM_DELAY_SECONDS", "2"))


SCENES = {
    "beach": {
        "captions": [
            "barefoot and unbothered 🌊",
            "this song is literally the color blue",
            "vitamin sea ☀️🌊",
            "sunset + this track = enough said",
            "coastal mood activated",
            "waves, wind, and the right soundtrack",
        ],
        "hashtags": ["#beach", "#summer", "#ocean", "#sunset", "#vibes", "#coastal"],
        "time_weights": {"morning": 0.2, "afternoon": 0.5, "evening": 0.25, "night": 0.05},
    },
    "party": {
        "captions": [
            "last night was everything 🪩✨",
            "we don't stop dancing 💃",
            "the squad when this came on 😭🔥",
            "this song should be played at every party forever",
            "the whole room lost it when this dropped 🎉",
        ],
        "hashtags": ["#party", "#nightout", "#dancing", "#club", "#friends", "#fyp"],
        "time_weights": {"morning": 0.02, "afternoon": 0.08, "evening": 0.3, "night": 0.6},
    },
    "cozy": {
        "captions": [
            "sunday reset mode activated ☕",
            "slow mornings are underrated",
            "this song and a warm drink is all i need 🍵",
            "rainy day playlist loading 🌧️",
            "do not disturb, i am in my cozy era",
        ],
        "hashtags": ["#cozy", "#homevibe", "#calm", "#slowliving", "#peaceful"],
        "time_weights": {"morning": 0.35, "afternoon": 0.3, "evening": 0.25, "night": 0.1},
    },
    "gym": {
        "captions": [
            "no days off 💪",
            "this song makes me lift heavier i swear",
            "running to this on repeat until my legs give out",
            "beast mode: activated 🔥",
        ],
        "hashtags": ["#gym", "#workout", "#fitness", "#motivation", "#running"],
        "time_weights": {"morning": 0.45, "afternoon": 0.3, "evening": 0.2, "night": 0.05},
    },
    "travel": {
        "captions": [
            "somewhere new, same playlist 🌍",
            "airport energy with this on 🛫",
            "every trip needs a soundtrack",
            "window seat, headphones in, world off",
        ],
        "hashtags": ["#travel", "#roadtrip", "#explore", "#wanderlust", "#journey"],
        "time_weights": {"morning": 0.3, "afternoon": 0.35, "evening": 0.25, "night": 0.1},
    },
    "urban": {
        "captions": [
            "city nights hit different 🌆",
            "the streets were made for this track",
            "neon lights and this song on repeat",
            "metropolitan mood 🏙️",
        ],
        "hashtags": ["#city", "#urban", "#downtown", "#citylife", "#nights"],
        "time_weights": {"morning": 0.05, "afternoon": 0.2, "evening": 0.35, "night": 0.4},
    },
    "emotional": {
        "captions": [
            "not okay but in a musical way 😭",
            "this song knows things about me",
            "crying in the car again 🚗💧",
            "therapy but make it a playlist 🎵",
        ],
        "hashtags": ["#emotional", "#feelings", "#mood", "#deepfeels", "#relatable"],
        "time_weights": {"morning": 0.1, "afternoon": 0.2, "evening": 0.3, "night": 0.4},
    },
    "nostalgic": {
        "captions": [
            "this takes me back to a simpler time 🥹",
            "some songs are time machines",
            "core memory unlocked 🔓",
            "hearing this is like finding an old photo album",
        ],
        "hashtags": ["#nostalgic", "#throwback", "#memories", "#classic", "#timeless"],
        "time_weights": {"morning": 0.2, "afternoon": 0.3, "evening": 0.3, "night": 0.2},
    },
}

REGIONS = ["US", "ES", "FR", "DE", "BR", "UK", "MX", "IT", "JP", "KR"]

app = FastAPI(title="Fake Trends API", version="1.0.0")


def get_time_of_day() -> str:
    hour = datetime.now(timezone.utc).hour
    if 6 <= hour < 12:
        return "morning"
    if 12 <= hour < 18:
        return "afternoon"
    if 18 <= hour < 23:
        return "evening"
    return "night"


def pick_scene(weights: dict[str, float]) -> str:
    scenes = list(weights.keys())
    values = list(weights.values())
    total = sum(values)
    if total <= 0:
        return random.choice(list(SCENES.keys()))
    normalized = [v / total for v in values]
    return random.choices(scenes, weights=normalized, k=1)[0]


def pick_scene_with_time(weights: dict[str, float]) -> str:
    tod = get_time_of_day()
    adjusted = {}
    for scene, weight in weights.items():
        adjusted[scene] = weight * SCENES[scene]["time_weights"][tod]
    return pick_scene(adjusted)


def get_scene_weights(
    energy: float | None = None,
    valence: float | None = None,
    acousticness: float | None = None,
    danceability: float | None = None,
    instrumentalness: float | None = None,
) -> dict[str, float]:
    w = {scene: 1.0 for scene in SCENES.keys()}

    if energy is not None:
        if energy >= 0.75:
            w["party"] += 2.5
            w["gym"] += 2.0
            w["urban"] += 1.2
        elif energy <= 0.35:
            w["cozy"] += 1.7
            w["emotional"] += 1.3
            w["nostalgic"] += 1.0

    if valence is not None:
        if valence >= 0.7:
            w["beach"] += 1.8
            w["party"] += 1.5
            w["travel"] += 1.2
        elif valence <= 0.35:
            w["emotional"] += 2.0
            w["nostalgic"] += 1.6
            w["urban"] += 0.8

    if acousticness is not None and acousticness >= 0.6:
        w["cozy"] += 1.8
        w["nostalgic"] += 1.2
        w["travel"] += 0.8

    if danceability is not None and danceability >= 0.7:
        w["party"] += 2.0
        w["gym"] += 1.0
        w["urban"] += 0.8

    if instrumentalness is not None and instrumentalness >= 0.4:
        w["travel"] += 1.4
        w["cozy"] += 1.0
        w["emotional"] += 0.8

    return w


def load_tracks() -> list[dict]:
    if not TRENDS_SEED_PATH.exists():
        raise FileNotFoundError(f"Trends seed file not found: {TRENDS_SEED_PATH}")

    if TRENDS_SEED_PATH.suffix.lower() == ".parquet":
        df = pl.read_parquet(TRENDS_SEED_PATH)
    else:
        df = pl.read_csv(TRENDS_SEED_PATH, infer_schema_length=10000)

    required_cols = {
        "track_name",
        "artist_name",
        "playcount",
        "listeners",
        "isrc",
        "energy",
        "valence",
        "acousticness",
        "danceability",
        "instrumentalness",
        "tempo",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in trends seed: {sorted(missing)}")

    if df.height > MAX_TRACKS_IN_MEMORY:
        df = df.head(MAX_TRACKS_IN_MEMORY)

    rows = df.to_dicts()
    if not rows:
        raise ValueError("Trends seed file is empty after loading.")

    return rows


TRACKS = load_tracks()


def pick_track() -> dict:
    weights = []
    for row in TRACKS:
        playcount = row.get("playcount") or 0
        listeners = row.get("listeners") or 0
        weight = max(float(playcount), 1.0) ** 0.5 + max(float(listeners), 1.0) ** 0.3
        weights.append(weight)
    return random.choices(TRACKS, weights=weights, k=1)[0]


def build_post(row: dict) -> dict:
    scene_weights = get_scene_weights(
        energy=row.get("energy"),
        valence=row.get("valence"),
        acousticness=row.get("acousticness"),
        danceability=row.get("danceability"),
        instrumentalness=row.get("instrumentalness"),
    )
    scene = pick_scene_with_time(scene_weights)
    scene_data = SCENES[scene]

    caption = random.choice(scene_data["captions"])
    hashtags = random.sample(
        scene_data["hashtags"],
        random.randint(2, min(5, len(scene_data["hashtags"]))),
    )

    is_viral = random.random() < 0.05
    if is_viral:
        views = random.randint(500_000, 5_000_000)
        likes = int(views * random.uniform(0.05, 0.15))
    else:
        views = max(100, int(random.expovariate(1 / 15_000)))
        likes = int(views * random.uniform(0.03, 0.12))

    comments = max(0, int(likes * random.uniform(0.02, 0.09)))
    shares = max(0, int(views * random.uniform(0.002, 0.02)))

    return {
        "post_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "simulated_trends_api",
        "user_id": f"user_{random.randint(1000, 99999)}",
        "artist": row["artist_name"],
        "track": row["track_name"],
        "isrc": row["isrc"],
        "caption": caption,
        "hashtags": hashtags,
        "scene": scene,
        "region": random.choice(REGIONS),
        "is_viral": is_viral,
        "views": views,
        "likes": likes,
        "comments": comments,
        "shares": shares,
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "tracks_loaded": len(TRACKS)}


@app.get("/post")
def get_post() -> dict:
    row = pick_track()
    return build_post(row)


@app.get("/tracks")
def get_tracks(limit: int = 20) -> list[dict]:
    limit = max(1, min(limit, 100))
    return random.sample(TRACKS, min(limit, len(TRACKS)))


@app.get("/stream")
def stream_posts():
    async def generator():
        while True:
            row = pick_track()
            post = build_post(row)
            yield json.dumps(post, ensure_ascii=False) + "\n"

            import asyncio
            await asyncio.sleep(STREAM_DELAY_SECONDS)

    return StreamingResponse(generator(), media_type="application/x-ndjson")