from __future__ import annotations

import json
import re
from datetime import date

import responses

from musicpods.bronze import Bronze
from musicpods.http import HttpClient
from musicpods.sources import listenbrainz as lb
from musicpods.sources import musicbrainz as mb
from musicpods.sources import podcasts as pc
from musicpods.sources.optional import OptionalIngest, podcastindex_headers

TODAY = date(2026, 10, 9)
NOW = 1_791_590_400  # 2026-10-09T00:00:00Z


def _http() -> HttpClient:
    return HttpClient("test/1.0", retries=0, sleep=lambda _s: None)


# ----------------------------------------------------------------------------- ListenBrainz
def test_user_key_is_salted_and_stable():
    assert lb.user_key("alice", "s1") == lb.user_key("alice", "s1")
    assert lb.user_key("alice", "s1") != lb.user_key("alice", "s2")
    assert re.fullmatch(r"u_[0-9a-f]{12}", lb.user_key("alice", "s1"))


def test_scrub_listen_drops_identity_and_unused_metadata():
    listen = {
        "user_name": "alice",
        "listened_at": 1,
        "recording_msid": "m",
        "track_metadata": {
            "artist_name": "A",
            "track_name": "T",
            "additional_info": {
                "duration_ms": 1000,
                "ms_played": 400,
                "origin_url": "https://example.org/alice/123",
                "spotify_id": "x",
            },
            "mbid_mapping": {"recording_mbid": "r", "caa_id": 1, "url_rels": [1]},
        },
    }
    out = lb.scrub_listen(listen, "u_1")
    assert "user_name" not in out
    assert out["user_key"] == "u_1"
    assert out["track_metadata"]["additional_info"] == {"duration_ms": 1000, "ms_played": 400}
    assert out["track_metadata"]["mbid_mapping"] == {"recording_mbid": "r"}
    assert "alice" not in json.dumps(out)
    assert listen["user_name"] == "alice"  # the input is not mutated


def test_activity_plausibility():
    ok = {"listening_activity": [{"listen_count": 10}, {"listen_count": 0}]}
    farm = {"listening_activity": [{"listen_count": 2000}]}
    idle = {"listening_activity": [{"listen_count": 0}]}
    assert lb._activity_is_plausible(ok, 480)
    assert not lb._activity_is_plausible(farm, 480)
    assert not lb._activity_is_plausible(idle, 480)
    assert not lb._activity_is_plausible(None, 480)


def _listen(ts: int, n: int) -> dict:
    return {
        "listened_at": ts,
        "inserted_at": ts + 5,
        "recording_msid": f"msid-{n}",
        "user_name": "alice",
        "track_metadata": {"artist_name": "A", "track_name": f"T{n}"},
    }


@responses.activate
def test_listenbrainz_ingest_end_to_end(settings):
    api = lb.API
    responses.get(
        f"{api}/stats/sitewide/artists",
        json={
            "payload": {
                "range": "week",
                "artists": [
                    {"artist_mbid": f"a{i}", "artist_name": f"A{i}", "listen_count": 100 - i}
                    for i in range(6)
                ],
            }
        },
    )
    for mbid in ("a0", "a3"):
        responses.get(
            f"{api}/stats/artist/{mbid}/listeners",
            json={"payload": {"listeners": [{"user_name": "alice"}, {"user_name": "farm"}]}},
        )
    responses.get(
        f"{api}/stats/user/alice/listening-activity",
        json={
            "payload": {
                "user_id": "alice",
                "listening_activity": [{"from_ts": NOW - 86400, "listen_count": 3}],
            }
        },
    )
    responses.get(
        f"{api}/stats/user/farm/listening-activity",
        json={"payload": {"listening_activity": [{"from_ts": NOW, "listen_count": 5000}]}},
    )
    responses.get(
        f"{api}/stats/user/alice/artists",
        json={
            "payload": {"user_id": "alice", "artists": [{"artist_name": "A", "listen_count": 3}]}
        },
    )
    responses.get(
        f"{api}/user/alice/listens",
        json={"payload": {"listens": [_listen(NOW - 100 * i, i) for i in range(3)]}},
    )
    responses.get(
        f"{api}/explore/fresh-releases/",
        json={
            "payload": {
                "releases": [
                    {
                        "release_mbid": "rel1",
                        "release_group_mbid": "rg1",
                        "release_date": "2026-10-01",
                        "listen_count": 5,
                        "caa_id": 99,
                    }
                ]
            }
        },
    )
    bronze = Bronze(settings.bronze_dir)
    ingest = lb.ListenBrainzIngest(
        settings.__class__(**{**settings.__dict__, "lb_users": 2}), _http(), bronze, TODAY
    )
    stats = ingest.run()
    assert stats["panel_size"] == 1
    assert stats["rejected_implausible"] == 1
    assert stats["listens_fetched"] == 3
    assert stats["fresh_releases"] == 1
    dumped = json.dumps([p for _, p in bronze.iter_payloads("listenbrainz", "listens")])
    assert "alice" not in dumped
    assert "alice" not in json.dumps(
        [p for _, p in bronze.iter_payloads("listenbrainz", "user_activity")]
    )
    # The cursor is saved: a second run asks only for newer listens and lands nothing new.
    responses.replace(responses.GET, f"{api}/user/alice/listens", json={"payload": {"listens": []}})
    assert ingest.listens(ingest._load_panel(), now_ts=NOW + 10) == 0


@responses.activate
def test_errors_never_carry_user_names(settings):
    responses.get(f"{lb.API}/user/alice/listens", status=500, body="alice is unavailable")
    ingest = lb.ListenBrainzIngest(settings, _http(), Bronze(settings.bronze_dir), TODAY)
    ingest._known("alice")
    try:
        ingest._get("/user/alice/listens")
    except lb.HttpError as exc:
        assert "alice" not in str(exc)
        assert lb.user_key("alice", settings.pseudonym_salt) in str(exc)
    else:
        raise AssertionError("expected an HttpError")


# ----------------------------------------------------------------------------- MusicBrainz
def _land_listens(bronze: Bronze) -> None:
    mapping = {"artist_mbids": ["art-1"], "recording_mbid": "rec-1", "release_group_mbid": "rg-1"}
    bronze.write(
        "listenbrainz",
        "listens",
        TODAY,
        "u-1",
        {"user_key": "u", "listens": [{"track_metadata": {"mbid_mapping": mapping}}] * 2},
    )
    bronze.write(
        "listenbrainz",
        "fresh_releases",
        TODAY,
        "past-30d",
        {"releases": [{"release_group_mbid": "rg-fresh", "listen_count": 9}]},
    )


@responses.activate
def test_musicbrainz_batches_exact_ids_and_remembers_fetches(settings):
    responses.get(f"{mb.WS}/genre/all", body="rock\npop\n")
    responses.get(
        f"{mb.WS}/artist/",
        json={
            "artists": [
                {"id": "art-1", "name": "Artist", "tags": [{"name": "Rock", "count": 3}]},
                {"id": "fuzzy-neighbour", "name": "Other"},
            ]
        },
    )
    responses.get(f"{mb.WS}/recording/", json={"recordings": [{"id": "rec-1", "length": 1000}]})
    responses.get(
        f"{mb.WS}/release-group/",
        json={
            "release-groups": [
                {"id": "rg-1", "first-release-date": "2020"},
                {"id": "rg-fresh", "first-release-date": "2026-10-01"},
            ]
        },
    )
    bronze = Bronze(settings.bronze_dir)
    _land_listens(bronze)
    stats = mb.MusicBrainzIngest(settings, _http(), bronze, TODAY).run()
    assert stats == {"genres": 2, "artists": 1, "recordings": 1, "release_groups": 2}
    query = responses.calls[-1].request.params["query"]
    assert "rgid:rg-1" in query and "rgid:rg-fresh" in query
    artists = [a for _, items in bronze.iter_payloads("musicbrainz", "artists") for a in items]
    assert [a["id"] for a in artists] == ["art-1"]
    assert artists[0]["tags"] == [{"name": "Rock", "count": 3}]
    calls = len(responses.calls)
    # Within mb_refresh_days nothing is fetched again.
    assert mb.MusicBrainzIngest(settings, _http(), bronze, TODAY).run() == {
        "genres": 0,
        "artists": 0,
        "recordings": 0,
        "release_groups": 0,
    }
    assert len(responses.calls) == calls


# ----------------------------------------------------------------------------- podcasts
RSS = b"""<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"><channel>
<item><guid>ep-2</guid><pubDate>Wed, 08 Oct 2026 05:00:00 +0200</pubDate>
  <itunes:duration>01:02:03</itunes:duration><enclosure url="https://x/2.mp3"/></item>
<item><guid>ep-1</guid><pubDate>2026-10-01T05:00:00Z</pubDate>
  <itunes:duration>1800</itunes:duration><itunes:episodeType>trailer</itunes:episodeType></item>
<item><title></title></item>
</channel></rss>"""


def test_parse_duration_and_dates():
    assert pc.parse_duration("3725") == 3725
    assert pc.parse_duration("12:05") == 725
    assert pc.parse_duration("1:00:00") == 3600
    assert pc.parse_duration("about an hour") is None
    assert pc.parse_pub_date("Wed, 08 Oct 2026 05:00:00 +0200") == "2026-10-08T03:00:00+00:00"
    assert pc.parse_pub_date("not a date") is None


def test_parse_feed_keeps_metadata_only():
    items = pc.parse_feed(RSS)
    assert len(items) == 2
    assert items[0]["duration_seconds"] == 3723
    assert items[0]["published_at"] == "2026-10-08T03:00:00+00:00"
    assert items[1]["episode_type"] == "trailer"
    assert items[1]["has_enclosure"] is False
    assert set(items[0]) == {
        "episode_key",
        "published_at",
        "duration_seconds",
        "episode_type",
        "has_enclosure",
    }


def test_parse_feed_rejects_entity_expansion():
    bomb = (
        b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa">]><rss><channel>&a;</channel></rss>'
    )
    try:
        pc.parse_feed(bomb)
    except Exception as exc:  # defusedxml raises EntitiesForbidden
        assert "Entit" in type(exc).__name__
    else:
        raise AssertionError("entity declarations must be refused")


@responses.activate
def test_podcast_ingest_charts_lookup_and_feeds(settings):
    s = settings.__class__(
        **{**settings.__dict__, "chart_countries": ("us", "gb"), "rss_top_n_per_country": 1}
    )
    responses.get(
        pc.CHART_URL.format(cc="us", n=s.chart_size),
        json={
            "feed": {
                "country": "us",
                "updated": "2026-10-09T05:00:00Z",
                "results": [
                    {"id": "11", "name": "Show", "artistName": "Pub", "genres": [{"name": "News"}]}
                ],
            }
        },
    )
    responses.get(pc.CHART_URL.format(cc="gb", n=s.chart_size), status=503)
    responses.get(
        pc.LOOKUP_URL,
        json={
            "results": [
                {
                    "kind": "podcast",
                    "collectionId": 11,
                    "collectionName": "Show",
                    "feedUrl": "https://feeds.example/11.xml",
                    "trackCount": 2,
                }
            ]
        },
    )
    responses.get("https://feeds.example/11.xml", body=RSS)
    bronze = Bronze(s.bronze_dir)
    out = pc.PodcastIngest(s, _http(), bronze, TODAY).run()
    assert out == {
        "chart_countries": 1,  # gb failed and is retried by the next run
        "chart_entries": 1,
        "shows_looked_up": 1,
        "feeds_fetched": 1,
    }
    calls = len(responses.calls)
    # Same UTC day: charts, lookups and feeds come from bronze; only gb is retried.
    pc.PodcastIngest(s, _http(), bronze, TODAY).run()
    assert [c.request.url for c in responses.calls[calls:]] == [
        pc.CHART_URL.format(cc="gb", n=s.chart_size)
    ]


@responses.activate
def test_failed_lookup_skips_country(settings):
    s = settings.__class__(**{**settings.__dict__, "chart_countries": ("us",)})
    responses.get(pc.LOOKUP_URL, status=500)
    shows = pc.PodcastIngest(s, _http(), Bronze(s.bronze_dir), TODAY).lookups({"us": [{"id": "1"}]})
    assert shows == {}


# ----------------------------------------------------------------------------- optional
def test_optional_sources_skip_without_keys(settings):
    out = OptionalIngest(settings, _http(), Bronze(settings.bronze_dir), TODAY).run({}, ["A"])
    assert out == {"lastfm": None, "podcastindex": None, "spotify": None}


def test_podcastindex_auth_header():
    h = podcastindex_headers("key", "secret", now=1000)
    assert h["X-Auth-Date"] == "1000"
    assert h["X-Auth-Key"] == "key"
    assert len(h["Authorization"]) == 40


@responses.activate
def test_lastfm_when_key_set(settings):
    s = settings.__class__(**{**settings.__dict__, "lastfm_api_key": "k"})
    responses.get(
        "https://ws.audioscrobbler.com/2.0/",
        json={
            "artists": {"artist": [{"name": "A", "mbid": "", "playcount": "5", "listeners": "2"}]}
        },
    )
    bronze = Bronze(s.bronze_dir)
    assert OptionalIngest(s, _http(), bronze, TODAY).lastfm() == 1
    [(_, payload)] = list(bronze.iter_payloads("lastfm", "top_artists"))
    assert payload["artists"][0] == {
        "rank": 1,
        "name": "A",
        "mbid": None,
        "playcount": 5,
        "listeners": 2,
    }


@responses.activate
def test_pagination_keeps_listens_sharing_the_boundary_second(settings):
    """max_ts is exclusive: three listens at the same second straddle a page boundary."""
    page_size = lb.PAGE_SIZE
    first = [_listen(NOW - i, i) for i in range(page_size - 1)] + [_listen(NOW - 5000, 9001)]
    second = [_listen(NOW - 5000, 9002), _listen(NOW - 5000, 9003), _listen(NOW - 6000, 9004)]
    responses.get(f"{lb.API}/user/alice/listens", json={"payload": {"listens": first}})
    responses.get(f"{lb.API}/user/alice/listens", json={"payload": {"listens": second}})
    ingest = lb.ListenBrainzIngest(settings, _http(), Bronze(settings.bronze_dir), TODAY)
    got = ingest._user_listens("alice", floor=NOW - 10_000, now_ts=NOW)
    assert len(got) == page_size + 3
    assert responses.calls[1].request.params["max_ts"] == str(NOW - 5000 + 1)
