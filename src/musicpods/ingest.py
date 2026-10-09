"""Ingest entrypoint: live APIs -> bronze, or a fixture tree -> bronze (``--from-dir``)."""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.http import HttpClient
from musicpods.sources.listenbrainz import ListenBrainzIngest
from musicpods.sources.musicbrainz import MusicBrainzIngest
from musicpods.sources.optional import OptionalIngest
from musicpods.sources.podcasts import PodcastIngest

log = logging.getLogger(__name__)

SOURCES = ("listenbrainz", "musicbrainz", "podcasts", "optional")


def http_client(settings: Settings) -> HttpClient:
    return HttpClient(
        settings.user_agent,
        timeout_s=settings.http_timeout_s,
        retries=settings.http_retries,
        min_interval_s={
            "musicbrainz.org": settings.mb_min_interval_s,
            "itunes.apple.com": 3.0,  # Apple's documented guidance is ~20 calls per minute
            "api.listenbrainz.org": 0.05,
        },
    )


def _top_artist_names(bronze: Bronze, n: int = 50) -> list[str]:
    counts: Counter = Counter()
    for _, payload in bronze.iter_payloads("listenbrainz", "listens"):
        for listen in payload.get("listens", []):
            name = (listen.get("track_metadata") or {}).get("artist_name")
            if name:
                counts[name] += 1
    return [name for name, _ in counts.most_common(n)]


def _feed_urls(bronze: Bronze) -> dict[str, str]:
    urls: dict[str, str] = {}
    for f in bronze.latest("apple", "lookup"):
        for r in bronze.read(f).get("results", []):
            if r.get("feedUrl"):
                urls[str(r["collectionId"])] = r["feedUrl"]
    return urls


def run_ingest(
    settings: Settings,
    sources: tuple[str, ...] = SOURCES,
    from_dir: Path | None = None,
    today: date | None = None,
    lb_backfill: bool = False,
) -> dict[str, Any]:
    bronze = Bronze(settings.bronze_dir)
    if from_dir is not None:
        copied = bronze.import_tree(from_dir)
        log.info("imported %d fixture files from %s", copied, from_dir)
        return {"mode": "fixtures", "files": copied}
    today = today or datetime.now(UTC).date()
    client = http_client(settings)
    result: dict[str, Any] = {"mode": "live", "date": today.isoformat()}
    if "listenbrainz" in sources:
        result["listenbrainz"] = ListenBrainzIngest(settings, client, bronze, today).run(
            backfill=lb_backfill
        )
    if "musicbrainz" in sources:
        result["musicbrainz"] = MusicBrainzIngest(settings, client, bronze, today).run()
    if "podcasts" in sources:
        result["podcasts"] = PodcastIngest(settings, client, bronze, today).run()
    if "optional" in sources:
        result["optional"] = OptionalIngest(settings, client, bronze, today).run(
            _feed_urls(bronze), _top_artist_names(bronze)
        )
    result["http_requests"] = client.requests_made
    settings.ops_dir.mkdir(parents=True, exist_ok=True)
    with (settings.ops_dir / "ingest_runs.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(result, default=str) + "\n")
    return result
