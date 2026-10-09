"""Glue-style batch jobs: bronze JSON -> typed Arrow -> DQ gate -> Iceberg ``raw.*``.

Each job rebuilds its table from the complete bronze history and replaces it in one Iceberg
snapshot (``overwrite``), so a rerun with the same bronze produces the same table and a
failed DQ gate leaves the previous snapshot in place. Bronze is the immutable history (every
daily chart snapshot, every MusicBrainz fetch), so nothing is lost by rebuilding.

In AWS each builder runs as a Glue Python shell job (``glue/entrypoint.py``); locally the same
functions run in-process.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa

from musicpods import lakehouse
from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.glue import schemas
from musicpods.quality.checks import TABLE_CHECKS, run_checks

log = logging.getLogger(__name__)


def _ts(epoch: Any) -> datetime | None:
    if epoch in (None, ""):
        return None
    return datetime.fromtimestamp(int(epoch), UTC).replace(tzinfo=None)


def _iso_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        from email.utils import parsedate_to_datetime

        try:
            dt = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def _date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _latest_by(rows: list[dict[str, Any]], key: str, order: str) -> list[dict[str, Any]]:
    best: dict[Any, dict[str, Any]] = {}
    for r in rows:
        k = r[key]
        if k not in best or r[order] >= best[k][order]:
            best[k] = r
    return [best[k] for k in sorted(best)]


# --------------------------------------------------------------------------- MusicBrainz
def _genres(bronze: Bronze) -> set[str]:
    latest = bronze.latest("musicbrainz", "genres")
    return set(bronze.read(latest[0])["genres"]) if latest else set()


def build_mb_artists(bronze: Bronze) -> pa.Table:
    rows = []
    for f, items in bronze.iter_payloads("musicbrainz", "artists"):
        for a in items:
            rows.append(
                {
                    "artist_mbid": a["id"],
                    "name": a.get("name"),
                    "sort_name": a.get("sort-name"),
                    "artist_type": a.get("type"),
                    "country": a.get("country"),
                    "begin_date": (a.get("life-span") or {}).get("begin"),
                    "disambiguation": a.get("disambiguation") or None,
                    "fetched_on": f.dt,
                }
            )
    return pa.Table.from_pylist(_latest_by(rows, "artist_mbid", "fetched_on"), schemas.MB_ARTISTS)


def build_mb_artist_tags(bronze: Bronze) -> pa.Table:
    genres = _genres(bronze)
    latest: dict[str, tuple[date, list[dict[str, Any]]]] = {}
    for f, items in bronze.iter_payloads("musicbrainz", "artists"):
        for a in items:
            if a["id"] not in latest or f.dt >= latest[a["id"]][0]:
                latest[a["id"]] = (f.dt, a.get("tags") or [])
    rows = []
    for mbid in sorted(latest):
        merged: dict[str, int] = {}
        for t in latest[mbid][1]:
            name = (t.get("name") or "").strip().lower()
            if name:
                merged[name] = merged.get(name, 0) + int(t.get("count") or 0)
        rows.extend(
            {"artist_mbid": mbid, "tag": name, "votes": votes, "is_genre": name in genres}
            for name, votes in sorted(merged.items())
        )
    return pa.Table.from_pylist(rows, schemas.MB_ARTIST_TAGS)


def build_mb_recordings(bronze: Bronze) -> pa.Table:
    rows = [
        {
            "recording_mbid": r["id"],
            "title": r.get("title"),
            "length_ms": r.get("length"),
            "first_release_date": r.get("first-release-date") or None,
            "is_video": bool(r.get("video")),
            "fetched_on": f.dt,
        }
        for f, items in bronze.iter_payloads("musicbrainz", "recordings")
        for r in items
    ]
    return pa.Table.from_pylist(
        _latest_by(rows, "recording_mbid", "fetched_on"), schemas.MB_RECORDINGS
    )


def build_mb_release_groups(bronze: Bronze) -> pa.Table:
    rows = [
        {
            "release_group_mbid": r["id"],
            "title": r.get("title"),
            "primary_type": r.get("primary-type"),
            "secondary_types": r.get("secondary-types") or [],
            "first_release_date": r.get("first-release-date") or None,
            "artist_credit": ", ".join(c.get("name") or "" for c in r.get("artist-credit") or [])
            or None,
            "fetched_on": f.dt,
        }
        for f, items in bronze.iter_payloads("musicbrainz", "release_groups")
        for r in items
    ]
    return pa.Table.from_pylist(
        _latest_by(rows, "release_group_mbid", "fetched_on"), schemas.MB_RELEASE_GROUPS
    )


# --------------------------------------------------------------------------- ListenBrainz
def build_lb_fresh_releases(bronze: Bronze) -> pa.Table:
    rows: dict[tuple[date, str], dict[str, Any]] = {}
    for f, payload in bronze.iter_payloads("listenbrainz", "fresh_releases"):
        for r in payload.get("releases", []):
            if not r.get("release_mbid"):
                continue
            mbids = r.get("artist_mbids") or []
            rows[(f.dt, r["release_mbid"])] = {
                "snapshot_date": f.dt,
                "release_mbid": r["release_mbid"],
                "release_group_mbid": r.get("release_group_mbid"),
                "release_name": r.get("release_name"),
                "artist_credit_name": r.get("artist_credit_name"),
                "primary_artist_mbid": mbids[0] if mbids else None,
                "release_date": _date(r.get("release_date")),
                "primary_type": r.get("release_group_primary_type"),
                "sitewide_listen_count": r.get("listen_count"),
            }
    return pa.Table.from_pylist([rows[k] for k in sorted(rows)], schemas.LB_FRESH_RELEASES)


def build_lb_sitewide_artists(bronze: Bronze) -> pa.Table:
    rows = []
    for f, p in bronze.iter_payloads("listenbrainz", "sitewide_artists"):
        rows.extend(
            {
                "snapshot_date": f.dt,
                "stats_range": p.get("range", f.name),
                "rank": i + 1,
                "artist_mbid": a.get("artist_mbid"),
                "artist_name": a.get("artist_name"),
                "listen_count": a.get("listen_count"),
                "from_ts": _ts(p.get("from_ts")),
                "to_ts": _ts(p.get("to_ts")),
                "last_updated": _ts(p.get("last_updated")),
            }
            for i, a in enumerate(p.get("artists", []))
        )
    return pa.Table.from_pylist(rows, schemas.LB_SITEWIDE_ARTISTS)


def build_lb_user_daily_activity(bronze: Bronze) -> pa.Table:
    rows = []
    for f, p in bronze.iter_payloads("listenbrainz", "user_activity"):
        for day in p.get("listening_activity", []):
            rows.append(
                {
                    "snapshot_date": f.dt,
                    "user_key": p["user_key"],
                    "activity_date": datetime.fromtimestamp(int(day["from_ts"]), UTC).date(),
                    "published_listen_count": int(day.get("listen_count") or 0),
                    "stats_last_updated": _ts(p.get("last_updated")),
                }
            )
    return pa.Table.from_pylist(rows, schemas.LB_USER_DAILY_ACTIVITY)


def build_lb_user_top_artists(bronze: Bronze) -> pa.Table:
    rows = []
    for f, p in bronze.iter_payloads("listenbrainz", "user_top_artists"):
        rows.extend(
            {
                "snapshot_date": f.dt,
                "user_key": p["user_key"],
                "rank": i + 1,
                "artist_mbid": a.get("artist_mbid"),
                "artist_name": a.get("artist_name"),
                "published_listen_count": a.get("listen_count"),
                "from_ts": _ts(p.get("from_ts")),
                "to_ts": _ts(p.get("to_ts")),
                "stats_last_updated": _ts(p.get("last_updated")),
            }
            for i, a in enumerate(p.get("artists", []))
        )
    return pa.Table.from_pylist(rows, schemas.LB_USER_TOP_ARTISTS)


# --------------------------------------------------------------------------- podcasts
def build_podcast_chart_entries(bronze: Bronze) -> pa.Table:
    rows = []
    for f, p in bronze.iter_payloads("apple", "charts"):
        country = (p.get("country") or f.name).lower()
        updated = _iso_ts(p.get("updated"))
        rows.extend(
            {
                "snapshot_date": f.dt,
                "country": country,
                "chart_rank": int(r["rank"]),
                "podcast_id": str(r["id"]),
                "podcast_name": r.get("name"),
                "publisher": r.get("artistName"),
                "chart_genre": (r.get("genres") or [None])[0],
                "chart_updated_at": updated,
            }
            for r in p.get("results", [])
        )
    return pa.Table.from_pylist(rows, schemas.PODCAST_CHART_ENTRIES)


def build_podcast_shows(bronze: Bronze) -> pa.Table:
    rows = []
    for f, p in bronze.iter_payloads("apple", "lookup"):
        rows.extend(
            {
                "lookup_date": f.dt,
                "country": (p.get("country") or f.name).lower(),
                "podcast_id": str(r["collectionId"]),
                "podcast_name": r.get("collectionName"),
                "publisher": r.get("artistName"),
                "primary_genre": r.get("primaryGenreName"),
                "published_episode_count": r.get("trackCount"),
                "latest_release_at": _iso_ts(r.get("releaseDate")),
                "explicitness": r.get("collectionExplicitness"),
                "has_feed_url": bool(r.get("feedUrl")),
            }
            for r in p.get("results", [])
        )
    return pa.Table.from_pylist(rows, schemas.PODCAST_SHOWS)


def build_podcast_episodes(bronze: Bronze) -> pa.Table:
    rows = []
    for f, p in bronze.iter_payloads("rss", "episodes"):
        seen: set[str] = set()
        for item in p.get("items", []):
            if item["episode_key"] in seen:
                continue  # some feeds repeat an item; keep the first
            seen.add(item["episode_key"])
            rows.append(
                {
                    "fetched_date": f.dt,
                    "podcast_id": str(p["podcast_id"]),
                    "episode_key": item["episode_key"],
                    "published_at": _iso_ts(item.get("published_at")),
                    "duration_seconds": item.get("duration_seconds"),
                    "episode_type": item.get("episode_type"),
                    "feed_status": p.get("status"),
                }
            )
    return pa.Table.from_pylist(rows, schemas.PODCAST_EPISODES)


# --------------------------------------------------------------------------- optional
def build_lastfm_top_artists(bronze: Bronze) -> pa.Table:
    rows = [
        {
            "snapshot_date": f.dt,
            "rank": a["rank"],
            "artist_name": a.get("name"),
            "artist_mbid": a.get("mbid"),
            "playcount": a.get("playcount"),
            "listeners": a.get("listeners"),
        }
        for f, p in bronze.iter_payloads("lastfm", "top_artists")
        for a in p.get("artists", [])
    ]
    return pa.Table.from_pylist(rows, schemas.LASTFM_TOP_ARTISTS)


def build_podcastindex_feeds(bronze: Bronze) -> pa.Table:
    rows = [
        {
            "snapshot_date": f.dt,
            "podcast_id": str(x["podcast_id"]),
            "podcastindex_id": x.get("podcastindex_id"),
            "episode_count": x.get("episode_count"),
            "newest_item_at": _ts(x.get("newest_item_pubdate")),
            "language": x.get("language"),
        }
        for f, p in bronze.iter_payloads("podcastindex", "feeds")
        for x in p.get("feeds", [])
    ]
    return pa.Table.from_pylist(rows, schemas.PODCASTINDEX_FEEDS)


def build_spotify_artists(bronze: Bronze) -> pa.Table:
    rows = [
        {
            "snapshot_date": f.dt,
            "query_name": a["query_name"],
            "spotify_id": a.get("spotify_id"),
            "name": a.get("name"),
            "genres": a.get("genres") or [],
        }
        for f, p in bronze.iter_payloads("spotify", "artists")
        for a in p.get("artists", [])
    ]
    return pa.Table.from_pylist(rows, schemas.SPOTIFY_ARTISTS)


Builder = Callable[[Bronze], pa.Table]

JOBS: dict[str, Builder] = {
    "mb_artists": build_mb_artists,
    "mb_artist_tags": build_mb_artist_tags,
    "mb_recordings": build_mb_recordings,
    "mb_release_groups": build_mb_release_groups,
    "lb_fresh_releases": build_lb_fresh_releases,
    "lb_sitewide_artists": build_lb_sitewide_artists,
    "lb_user_daily_activity": build_lb_user_daily_activity,
    "lb_user_top_artists": build_lb_user_top_artists,
    "podcast_chart_entries": build_podcast_chart_entries,
    "podcast_shows": build_podcast_shows,
    "podcast_episodes": build_podcast_episodes,
    "lastfm_top_artists": build_lastfm_top_artists,
    "podcastindex_feeds": build_podcastindex_feeds,
    "spotify_artists": build_spotify_artists,
}

PARTITIONS = {
    "podcast_chart_entries": ("snapshot_date", "identity"),
    "podcast_episodes": ("fetched_date", "identity"),
}


def run_jobs(settings: Settings, names: list[str] | None = None, cat=None) -> dict[str, int]:
    """Build every table first and run all DQ gates; commit only if every gate passed."""
    bronze = Bronze(settings.bronze_dir)
    cat = cat or lakehouse.catalog(settings)
    built: dict[str, pa.Table] = {}
    for name in names or list(JOBS):
        table = JOBS[name](bronze)
        run_checks(f"raw.{name}", table, TABLE_CHECKS.get(name, []), settings)
        built[name] = table
    for name, table in built.items():
        lakehouse.overwrite(cat, name, table, PARTITIONS.get(name))
        log.info("raw.%s: %d rows", name, table.num_rows)
    counts = {name: t.num_rows for name, t in built.items()}
    settings.ops_dir.mkdir(parents=True, exist_ok=True)
    (settings.ops_dir / "glue_last_run.json").write_text(json.dumps(counts, indent=1), "utf-8")
    return counts
