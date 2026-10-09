"""Arrow schemas of the raw Iceberg tables (Glue catalog database ``raw`` in AWS).

Timestamps are UTC without a zone (Iceberg ``timestamp``), which DuckDB and Athena read
identically.
"""

from __future__ import annotations

import pyarrow as pa

TS = pa.timestamp("us")
STR = pa.string()

LISTEN_EVENTS = pa.schema(
    [
        ("event_id", STR),
        ("user_key", STR),
        ("listened_at", TS),
        ("inserted_at", TS),
        ("recording_msid", STR),
        ("artist_name", STR),
        ("track_name", STR),
        ("release_name", STR),
        ("recording_mbid", STR),
        ("release_mbid", STR),
        ("release_group_mbid", STR),
        ("primary_artist_mbid", STR),
        ("artist_mbids", pa.list_(STR)),
        ("duration_ms", pa.int64()),
        ("played_ms", pa.int64()),
        ("submission_client", STR),
        ("music_service", STR),
        ("ingested_at", TS),
    ]
)

MB_ARTISTS = pa.schema(
    [
        ("artist_mbid", STR),
        ("name", STR),
        ("sort_name", STR),
        ("artist_type", STR),
        ("country", STR),
        ("begin_date", STR),
        ("disambiguation", STR),
        ("fetched_on", pa.date32()),
    ]
)

MB_ARTIST_TAGS = pa.schema(
    [
        ("artist_mbid", STR),
        ("tag", STR),
        ("votes", pa.int64()),
        ("is_genre", pa.bool_()),
    ]
)

MB_RECORDINGS = pa.schema(
    [
        ("recording_mbid", STR),
        ("title", STR),
        ("length_ms", pa.int64()),
        ("first_release_date", STR),
        ("is_video", pa.bool_()),
        ("fetched_on", pa.date32()),
    ]
)

MB_RELEASE_GROUPS = pa.schema(
    [
        ("release_group_mbid", STR),
        ("title", STR),
        ("primary_type", STR),
        ("secondary_types", pa.list_(STR)),
        ("first_release_date", STR),
        ("artist_credit", STR),
        ("fetched_on", pa.date32()),
    ]
)

LB_FRESH_RELEASES = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("release_mbid", STR),
        ("release_group_mbid", STR),
        ("release_name", STR),
        ("artist_credit_name", STR),
        ("primary_artist_mbid", STR),
        ("release_date", pa.date32()),
        ("primary_type", STR),
        ("sitewide_listen_count", pa.int64()),
    ]
)

LB_SITEWIDE_ARTISTS = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("stats_range", STR),
        ("rank", pa.int32()),
        ("artist_mbid", STR),
        ("artist_name", STR),
        ("listen_count", pa.int64()),
        ("from_ts", TS),
        ("to_ts", TS),
        ("last_updated", TS),
    ]
)

LB_USER_DAILY_ACTIVITY = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("user_key", STR),
        ("activity_date", pa.date32()),
        ("published_listen_count", pa.int64()),
        ("stats_last_updated", TS),
    ]
)

LB_USER_TOP_ARTISTS = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("user_key", STR),
        ("rank", pa.int32()),
        ("artist_mbid", STR),
        ("artist_name", STR),
        ("published_listen_count", pa.int64()),
        ("from_ts", TS),
        ("to_ts", TS),
        ("stats_last_updated", TS),
    ]
)

PODCAST_CHART_ENTRIES = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("country", STR),
        ("chart_rank", pa.int32()),
        ("podcast_id", STR),
        ("podcast_name", STR),
        ("publisher", STR),
        ("chart_genre", STR),
        ("chart_updated_at", TS),
    ]
)

PODCAST_SHOWS = pa.schema(
    [
        ("lookup_date", pa.date32()),
        ("country", STR),
        ("podcast_id", STR),
        ("podcast_name", STR),
        ("publisher", STR),
        ("primary_genre", STR),
        ("published_episode_count", pa.int64()),
        ("latest_release_at", TS),
        ("explicitness", STR),
        ("has_feed_url", pa.bool_()),
    ]
)

PODCAST_EPISODES = pa.schema(
    [
        ("fetched_date", pa.date32()),
        ("podcast_id", STR),
        ("episode_key", STR),
        ("published_at", TS),
        ("duration_seconds", pa.int64()),
        ("episode_type", STR),
        ("feed_status", STR),
    ]
)

LASTFM_TOP_ARTISTS = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("rank", pa.int32()),
        ("artist_name", STR),
        ("artist_mbid", STR),
        ("playcount", pa.int64()),
        ("listeners", pa.int64()),
    ]
)

PODCASTINDEX_FEEDS = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("podcast_id", STR),
        ("podcastindex_id", pa.int64()),
        ("episode_count", pa.int64()),
        ("newest_item_at", TS),
        ("language", STR),
    ]
)

SPOTIFY_ARTISTS = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("query_name", STR),
        ("spotify_id", STR),
        ("name", STR),
        ("genres", pa.list_(STR)),
    ]
)
