"""Key-gated optional sources. Each runs only when its credentials are set and is skipped
(not failed) otherwise; the pipeline and the results site work without any of them.

* Last.fm ``chart.getTopArtists`` (LASTFM_API_KEY): a global popularity benchmark.
* Podcast Index ``podcasts/byfeedurl`` (PODCASTINDEX_API_KEY + _SECRET): independent episode
  counts and newest-episode times for the feeds we track.
* Spotify Web API (SPOTIFY_CLIENT_ID + _SECRET, client-credentials flow): catalogue metadata
  only. Since November 2024 new apps cannot call audio-features, audio-analysis or
  recommendations, so nothing here depends on them.

These clients are covered by unit tests with mocked HTTP; they have not been run against the
live services in this repository's recorded runs (no keys were used).
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date
from typing import Any

from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.http import HttpClient

log = logging.getLogger(__name__)

LASTFM_URL = "https://ws.audioscrobbler.com/2.0/"
PODCASTINDEX_URL = "https://api.podcastindex.org/api/1.0/podcasts/byfeedurl"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"  # noqa: S105 - endpoint, not a secret
SPOTIFY_SEARCH_URL = "https://api.spotify.com/v1/search"


def podcastindex_headers(key: str, secret: str, now: int | None = None) -> dict[str, str]:
    ts = str(now if now is not None else int(time.time()))
    digest = hashlib.sha1((key + secret + ts).encode(), usedforsecurity=False).hexdigest()
    return {"X-Auth-Key": key, "X-Auth-Date": ts, "Authorization": digest}


class OptionalIngest:
    def __init__(self, settings: Settings, client: HttpClient, bronze: Bronze, today: date):
        self.s = settings
        self.http = client
        self.bronze = bronze
        self.today = today

    def lastfm(self) -> int | None:
        if not self.s.lastfm_api_key:
            return None
        body = self.http.get_json(
            LASTFM_URL,
            params={
                "method": "chart.gettopartists",
                "api_key": self.s.lastfm_api_key,
                "format": "json",
                "limit": 100,
            },
        )
        artists = [
            {
                "rank": i + 1,
                "name": a.get("name"),
                "mbid": a.get("mbid") or None,
                "playcount": int(a.get("playcount") or 0),
                "listeners": int(a.get("listeners") or 0),
            }
            for i, a in enumerate(((body or {}).get("artists") or {}).get("artist", []))
        ]
        self.bronze.write("lastfm", "top_artists", self.today, "global", {"artists": artists})
        return len(artists)

    def podcastindex(self, feed_urls: dict[str, str]) -> int | None:
        if not (self.s.podcastindex_api_key and self.s.podcastindex_api_secret):
            return None
        out = []
        for pid, url in sorted(feed_urls.items()):
            body = self.http.get_json(
                PODCASTINDEX_URL,
                params={"url": url},
                headers=podcastindex_headers(
                    self.s.podcastindex_api_key, self.s.podcastindex_api_secret
                ),
                allow_404=True,
            )
            feed = (body or {}).get("feed") or {}
            if feed:
                out.append(
                    {
                        "podcast_id": pid,
                        "podcastindex_id": feed.get("id"),
                        "episode_count": feed.get("episodeCount"),
                        "newest_item_pubdate": feed.get("newestItemPubdate"),
                        "language": feed.get("language"),
                    }
                )
        self.bronze.write("podcastindex", "feeds", self.today, "tracked", {"feeds": out})
        return len(out)

    def spotify(self, artist_names: list[str]) -> int | None:
        if not (self.s.spotify_client_id and self.s.spotify_client_secret):
            return None
        token_resp = self.http.session.post(
            SPOTIFY_TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self.s.spotify_client_id, self.s.spotify_client_secret),
            timeout=self.http.timeout_s,
        )
        token_resp.raise_for_status()
        headers = {"Authorization": f"Bearer {token_resp.json()['access_token']}"}
        out = []
        for name in artist_names:
            body = self.http.get_json(
                SPOTIFY_SEARCH_URL,
                params={"q": f'artist:"{name}"', "type": "artist", "limit": 1},
                headers=headers,
            )
            items = (((body or {}).get("artists") or {}).get("items")) or []
            if items:
                a = items[0]
                out.append(
                    {
                        "query_name": name,
                        "spotify_id": a.get("id"),
                        "name": a.get("name"),
                        "genres": a.get("genres") or [],
                    }
                )
        self.bronze.write("spotify", "artists", self.today, "top", {"artists": out})
        return len(out)

    def run(self, feed_urls: dict[str, str], artist_names: list[str]) -> dict[str, Any]:
        result = {
            "lastfm": self.lastfm(),
            "podcastindex": self.podcastindex(feed_urls),
            "spotify": self.spotify(artist_names),
        }
        for name, value in result.items():
            if value is None:
                log.info("optional source %s skipped (credentials not set)", name)
        return result
