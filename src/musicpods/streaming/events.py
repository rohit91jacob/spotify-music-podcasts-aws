"""Listen-event envelope (schema v1) and its validation.

Each real ListenBrainz listen becomes one event. ``event_id`` is a deterministic hash of
(user_key, listened_at, recording_msid), so replaying the same listens produces the same ids
and the Iceberg sink can drop duplicates (at-least-once transport, exactly-once table).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1
REQUIRED = ("schema_version", "event_id", "user_key", "listened_at", "track_metadata")


def event_id(user_key: str, listened_at: int, recording_msid: str | None) -> str:
    raw = f"{user_key}|{int(listened_at)}|{recording_msid or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def to_event(listen: dict[str, Any]) -> dict[str, Any]:
    ts = int(listen["listened_at"])
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "listen",
        "event_id": event_id(listen["user_key"], ts, listen.get("recording_msid")),
        "user_key": listen["user_key"],
        "listened_at": ts,
        "inserted_at": listen.get("inserted_at"),
        "recording_msid": listen.get("recording_msid"),
        "track_metadata": listen.get("track_metadata") or {},
    }


def encode(event: dict[str, Any]) -> bytes:
    """Firehose concatenates records; a trailing newline keeps the S3 objects JSON Lines."""
    return (json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def validate(event: Any) -> str | None:
    """Return a reason string when the event is unusable, else None."""
    if not isinstance(event, dict):
        return "not_an_object"
    missing = [k for k in REQUIRED if k not in event or event[k] in (None, "")]
    if missing:
        return "missing:" + ",".join(missing)
    if event["schema_version"] != SCHEMA_VERSION:
        return f"unsupported_schema_version:{event['schema_version']}"
    if not isinstance(event["listened_at"], int) or event["listened_at"] <= 0:
        return "bad_listened_at"
    if not isinstance(event["track_metadata"], dict):
        return "bad_track_metadata"
    if not event["track_metadata"].get("track_name") and not event["track_metadata"].get(
        "artist_name"
    ):
        return "empty_track_metadata"
    expected = event_id(event["user_key"], event["listened_at"], event.get("recording_msid"))
    if event["event_id"] != expected:
        return "event_id_mismatch"
    return None


def _ms(value: Any) -> int | None:
    try:
        v = int(float(value))
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _non_negative(value: Any) -> int | None:
    try:
        v = int(float(value))
    except (TypeError, ValueError):
        return None
    return v if v >= 0 else None


def flatten(event: dict[str, Any]) -> dict[str, Any]:
    """Event -> one row of ``raw.listen_events``."""
    tm = event["track_metadata"]
    info = tm.get("additional_info") or {}
    mapping = tm.get("mbid_mapping") or {}
    artist_mbids = list(mapping.get("artist_mbids") or info.get("artist_mbids") or [])
    duration = _ms(info.get("duration_ms"))
    if duration is None and info.get("duration") is not None:
        seconds = _ms(info.get("duration"))
        duration = seconds * 1000 if seconds else None
    # Observed playback time. Only Spotify's extended streaming history (imported into
    # ListenBrainz) reports it, as ``ms_played``. Other clients' ``duration_played`` is the
    # scrobble checkpoint (30-second steps), not how long the track actually played.
    played = _non_negative(info.get("ms_played"))
    inserted = event.get("inserted_at")
    return {
        "event_id": event["event_id"],
        "user_key": event["user_key"],
        "listened_at": datetime.fromtimestamp(event["listened_at"], UTC).replace(tzinfo=None),
        "inserted_at": (
            datetime.fromtimestamp(int(inserted), UTC).replace(tzinfo=None)
            if isinstance(inserted, int | float)
            else None
        ),
        "recording_msid": event.get("recording_msid"),
        "artist_name": tm.get("artist_name"),
        "track_name": tm.get("track_name"),
        "release_name": tm.get("release_name"),
        "recording_mbid": mapping.get("recording_mbid") or info.get("recording_mbid"),
        "release_mbid": mapping.get("release_mbid") or info.get("release_mbid"),
        "release_group_mbid": mapping.get("release_group_mbid"),
        "primary_artist_mbid": artist_mbids[0] if artist_mbids else None,
        "artist_mbids": artist_mbids,
        "duration_ms": duration,
        "played_ms": played,
        "submission_client": info.get("submission_client"),
        "music_service": info.get("music_service") or info.get("music_service_name"),
    }
