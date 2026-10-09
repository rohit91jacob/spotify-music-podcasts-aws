"""MusicBrainz web service (core data CC0; tags/genres CC BY-NC-SA 3.0).

Lookups are batched through the search endpoint (``arid:a OR arid:b ...``), 50 ids per request,
paced at one request per ``mb_min_interval_s`` (MusicBrainz allows 1 req/s and asks for a
descriptive User-Agent). Which ids are looked up is decided from the landed listens: the most
listened artists, recordings and release groups first, capped by ``MUSICPODS_MB_MAX_*``. The
release groups of the most-played fresh releases (``MUSICPODS_MB_FRESH_RELEASE_GROUPS``) are
looked up too, for the release-date reconciliation in docs/verification.md.
Ids fetched within ``mb_refresh_days`` are not fetched again (``ops/mb_fetched.json``).
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import date, timedelta
from typing import Any

from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.http import HttpClient

log = logging.getLogger(__name__)

WS = "https://musicbrainz.org/ws/2"
SOURCE = "musicbrainz"
BATCH = 50

ENTITIES = {
    # entity: (search field, bronze dataset, kept keys)
    "artist": (
        "arid",
        "artists",
        ("id", "name", "sort-name", "type", "country", "life-span", "tags", "disambiguation"),
    ),
    "recording": (
        "rid",
        "recordings",
        ("id", "title", "length", "first-release-date", "artist-credit", "video"),
    ),
    "release-group": (
        "rgid",
        "release_groups",
        ("id", "title", "primary-type", "secondary-types", "first-release-date", "artist-credit"),
    ),
}


def _slim_credit(credit: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        {"artist_mbid": (c.get("artist") or {}).get("id"), "name": c.get("name")}
        for c in credit or []
    ]


def listened_ids(bronze: Bronze) -> dict[str, Counter]:
    """Count listens per artist / recording / release-group MBID across all landed listens."""
    counts = {"artist": Counter(), "recording": Counter(), "release-group": Counter()}
    for _, payload in bronze.iter_payloads("listenbrainz", "listens"):
        for listen in payload.get("listens", []):
            mapping = (listen.get("track_metadata") or {}).get("mbid_mapping") or {}
            for mbid in mapping.get("artist_mbids") or []:
                counts["artist"][mbid] += 1
            if mapping.get("recording_mbid"):
                counts["recording"][mapping["recording_mbid"]] += 1
            if mapping.get("release_group_mbid"):
                counts["release-group"][mapping["release_group_mbid"]] += 1
    return counts


def fresh_release_groups(bronze: Bronze, n: int) -> list[str]:
    """Release groups of the ``n`` most-played releases in the latest fresh-releases snapshot."""
    latest = bronze.latest("listenbrainz", "fresh_releases")
    releases = [r for f in latest for r in bronze.read(f).get("releases", [])]
    ranked = sorted(
        (r for r in releases if r.get("release_group_mbid")),
        key=lambda r: (-(r.get("listen_count") or 0), r["release_group_mbid"]),
    )
    out: list[str] = []
    for r in ranked:
        if r["release_group_mbid"] not in out:
            out.append(r["release_group_mbid"])
        if len(out) >= n:
            break
    return out


class MusicBrainzIngest:
    def __init__(self, settings: Settings, client: HttpClient, bronze: Bronze, today: date):
        self.s = settings
        self.http = client
        self.bronze = bronze
        self.today = today
        self.state_path = settings.ops_dir / "mb_fetched.json"

    def genres(self) -> int:
        existing = self.bronze.files(SOURCE, "genres")
        if existing and (self.today - max(f.dt for f in existing)).days < self.s.mb_refresh_days:
            return 0
        resp = self.http.get(f"{WS}/genre/all", params={"fmt": "txt"})
        names = sorted({line.strip() for line in resp.text.splitlines() if line.strip()})
        self.bronze.write(SOURCE, "genres", self.today, "all", {"genres": names})
        return len(names)

    def _search(self, entity: str, ids: list[str]) -> list[dict[str, Any]]:
        field, _, keep = ENTITIES[entity]
        query = " OR ".join(f"{field}:{i}" for i in ids)
        body = self.http.get_json(
            f"{WS}/{entity}/", params={"query": query, "fmt": "json", "limit": 100}
        )
        key = {"artist": "artists", "recording": "recordings", "release-group": "release-groups"}[
            entity
        ]
        wanted = set(ids)
        out = []
        for item in (body or {}).get(key, []):
            if item.get("id") not in wanted:
                continue  # search can return fuzzy neighbours; keep exact id hits only
            slim = {k: item.get(k) for k in keep if k in item}
            if "artist-credit" in slim:
                slim["artist-credit"] = _slim_credit(slim["artist-credit"])
            if "tags" in slim:
                slim["tags"] = [
                    {"name": t["name"], "count": t.get("count", 0)} for t in slim["tags"]
                ]
            out.append(slim)
        return out

    def run(self) -> dict[str, int]:
        stats = {"genres": self.genres()}
        state: dict[str, dict[str, str]] = (
            json.loads(self.state_path.read_text("utf-8")) if self.state_path.exists() else {}
        )
        fresh_after = self.today - timedelta(days=self.s.mb_refresh_days)
        counts = listened_ids(self.bronze)
        caps = {
            "artist": self.s.mb_max_artists,
            "recording": self.s.mb_max_recordings,
            "release-group": self.s.mb_max_release_groups,
        }
        for entity, (_, dataset, _) in ENTITIES.items():
            done = state.setdefault(entity, {})
            ranked = [i for i, _ in sorted(counts[entity].items(), key=lambda kv: (-kv[1], kv[0]))]
            wanted = ranked[: caps[entity]]
            if entity == "release-group":
                extra = fresh_release_groups(self.bronze, self.s.mb_fresh_release_groups)
                wanted += [i for i in extra if i not in set(wanted)]
            todo = [i for i in wanted if i not in done or date.fromisoformat(done[i]) < fresh_after]
            fetched = 0
            for n in range(0, len(todo), BATCH):
                batch = todo[n : n + BATCH]
                items = self._search(entity, batch)
                self.bronze.write(
                    SOURCE, dataset, self.today, f"batch-{n // BATCH:04d}-{batch[0][:8]}", items
                )
                for i in batch:
                    done[i] = self.today.isoformat()
                fetched += len(items)
                self.state_path.parent.mkdir(parents=True, exist_ok=True)
                self.state_path.write_text(json.dumps(state), encoding="utf-8")
            stats[dataset] = fetched
            log.info("musicbrainz %s: %d requested, %d found", entity, len(todo), fetched)
        return stats
