"""Apple Podcasts top charts (Apple's public RSS generator), iTunes Lookup metadata, and the
podcasts' own RSS feeds for episode cadence.

Apple's chart feed is "Copyright Apple Inc. All rights reserved" and podcast feeds belong to
their publishers, so none of this data is committed to the repository: it is downloaded at run
time into the (git-ignored) data directory. Test fixtures for these sources are synthetic.

Charts are snapshots: one file per country per UTC day; a second run on the same day keeps the
first snapshot and makes no request. Chart longevity therefore accumulates one data point per
daily refresh.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from defusedxml import ElementTree

from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.http import HttpClient, HttpError

log = logging.getLogger(__name__)

CHART_URL = "https://rss.marketingtools.apple.com/api/v2/{cc}/podcasts/top/{n}/podcasts.json"
LOOKUP_URL = "https://itunes.apple.com/lookup"
ITUNES_NS = "{http://www.itunes.com/dtds/podcast-1.0.dtd}"
LOOKUP_KEYS = (
    "collectionId",
    "collectionName",
    "artistName",
    "feedUrl",
    "primaryGenreName",
    "genres",
    "trackCount",
    "releaseDate",
    "country",
    "collectionExplicitness",
    "contentAdvisoryRating",
)


def parse_duration(raw: str | None) -> int | None:
    """``itunes:duration`` comes as seconds, ``MM:SS`` or ``HH:MM:SS``."""
    if not raw:
        return None
    raw = raw.strip()
    if re.fullmatch(r"\d+(\.\d+)?", raw):
        return int(float(raw))
    parts = raw.split(":")
    if all(p.isdigit() for p in parts) and 1 < len(parts) <= 3:
        seconds = 0
        for p in parts:
            seconds = seconds * 60 + int(p)
        return seconds
    return None


def parse_pub_date(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat(timespec="seconds")


def parse_feed(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Episode metadata only: a hash of the guid, publish time, duration, type."""
    root = ElementTree.fromstring(xml_bytes)
    channel = root.find("channel")
    if channel is None:
        return []
    items = []
    for item in channel.findall("item"):
        guid = (item.findtext("guid") or "").strip()
        enclosure = item.find("enclosure")
        url = enclosure.get("url") if enclosure is not None else None
        ident = guid or url or (item.findtext("title") or "")
        if not ident:
            continue
        items.append(
            {
                "episode_key": hashlib.sha256(ident.encode("utf-8")).hexdigest()[:16],
                "published_at": parse_pub_date(item.findtext("pubDate")),
                "duration_seconds": parse_duration(item.findtext(f"{ITUNES_NS}duration")),
                "episode_type": (item.findtext(f"{ITUNES_NS}episodeType") or "full").strip(),
                "has_enclosure": enclosure is not None,
            }
        )
    return items


class PodcastIngest:
    def __init__(self, settings: Settings, client: HttpClient, bronze: Bronze, today: date):
        self.s = settings
        self.http = client
        self.bronze = bronze
        self.today = today

    def charts(self) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {}
        for cc in self.s.chart_countries:
            if self.bronze.exists("apple", "charts", self.today, cc):
                payload = self.bronze.read(self.bronze.path_for("apple", "charts", self.today, cc))
            else:
                try:
                    feed = self.http.get_json(CHART_URL.format(cc=cc, n=self.s.chart_size))["feed"]
                except (HttpError, KeyError, TypeError) as exc:
                    log.warning("chart %s unavailable: %s", cc, exc)
                    continue
                payload = {
                    "country": feed.get("country", cc),
                    "updated": feed.get("updated"),
                    "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    "results": [
                        {
                            "rank": i + 1,
                            "id": r.get("id"),
                            "name": r.get("name"),
                            "artistName": r.get("artistName"),
                            "genres": [g.get("name") for g in r.get("genres", [])],
                            "url": r.get("url"),
                        }
                        for i, r in enumerate(feed.get("results", []))
                    ],
                }
                self.bronze.write("apple", "charts", self.today, cc, payload)
            out[cc] = payload["results"]
        return out

    def lookups(self, charts: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
        shows: dict[str, dict[str, Any]] = {}
        for cc, results in charts.items():
            if self.bronze.exists("apple", "lookup", self.today, cc):
                payload = self.bronze.read(self.bronze.path_for("apple", "lookup", self.today, cc))
            else:
                ids = [r["id"] for r in results if r.get("id")]
                found: list[dict[str, Any]] = []
                try:
                    for n in range(0, len(ids), 100):
                        body = self.http.get_json(
                            LOOKUP_URL,
                            params={
                                "id": ",".join(ids[n : n + 100]),
                                "country": cc,
                                "entity": "podcast",
                            },
                        )
                        found.extend(
                            {k: r.get(k) for k in LOOKUP_KEYS}
                            for r in (body or {}).get("results", [])
                            if r.get("kind") == "podcast"
                        )
                except HttpError as exc:
                    # Nothing is landed for this storefront today, so the next run retries it.
                    log.warning("iTunes lookup for %s unavailable: %s", cc, exc)
                    continue
                payload = {"country": cc, "results": found}
                self.bronze.write("apple", "lookup", self.today, cc, payload)
            for r in payload["results"]:
                shows.setdefault(str(r["collectionId"]), r)
        return shows

    def feeds(self, charts: dict[str, list[dict[str, Any]]], shows: dict[str, dict]) -> int:
        chosen: list[str] = []
        for results in charts.values():
            for r in results[: self.s.rss_top_n_per_country]:
                if r["id"] not in chosen:
                    chosen.append(r["id"])
        chosen = chosen[: self.s.rss_max_feeds]
        fetched = 0
        for pid in chosen:
            show = shows.get(str(pid))
            if not show or not show.get("feedUrl"):
                continue
            if self.bronze.exists("rss", "episodes", self.today, str(pid)):
                continue
            try:
                resp = self.http.get(show["feedUrl"], max_bytes=self.s.rss_max_bytes)
                items = parse_feed(resp.content)
                status = "ok"
            except (HttpError, ElementTree.ParseError, ValueError) as exc:
                log.warning("feed for podcast %s failed: %s", pid, exc)
                items, status = [], f"error: {type(exc).__name__}"
            self.bronze.write(
                "rss",
                "episodes",
                self.today,
                str(pid),
                {
                    "podcast_id": str(pid),
                    "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    "status": status,
                    "items": items,
                },
            )
            fetched += 1
        return fetched

    def run(self) -> dict[str, int]:
        charts = self.charts()
        shows = self.lookups(charts)
        return {
            "chart_countries": len(charts),
            "chart_entries": sum(len(v) for v in charts.values()),
            "shows_looked_up": len(shows),
            "feeds_fetched": self.feeds(charts, shows),
        }
