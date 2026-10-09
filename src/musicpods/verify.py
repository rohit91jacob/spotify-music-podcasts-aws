"""Reconcile pipeline outputs against figures the sources publish independently.

1. **Listens per user per day** (Firehose -> Iceberg -> dbt) vs ListenBrainz's published
   listening-activity statistics for the same users and UTC days.
2. **Top-10 artists per user for the stats week** vs ListenBrainz's published per-user
   top-artist statistics (listen counts).
3. **Episodes per show** parsed from RSS vs the episode count Apple publishes (iTunes Lookup
   ``trackCount``), and the latest episode time vs ``releaseDate``.
4. **Release dates** ListenBrainz lists for fresh releases vs MusicBrainz release-group
   first-release-dates.

Mismatches are reported, not hidden; docs/verification.md explains the causes seen.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from musicpods.config import Settings
from musicpods.warehouse import connect


def _rows(con, sql: str) -> list[dict[str, Any]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _one(con, sql: str) -> dict[str, Any]:
    rows = _rows(con, sql)
    return rows[0] if rows else {}


def _daily_summary(con, where: str) -> dict[str, Any]:
    return _one(
        con,
        f"""
        SELECT count(*) AS user_days,
               count(DISTINCT user_key) AS users,
               sum(CASE WHEN difference = 0 THEN 1 ELSE 0 END) AS exact_matches,
               sum(CASE WHEN abs(difference) <= greatest(1, 0.01 * published_listen_count)
                        THEN 1 ELSE 0 END) AS within_1pct,
               sum(published_listen_count) AS published_listens,
               sum(our_listen_count) AS our_listens,
               sum(abs(difference)) AS abs_difference
        FROM marts.ver_user_daily_listens WHERE {where}
        """,
    )


def user_daily(con) -> dict[str, Any]:
    """Settled days (before the statistics snapshot cutoff) are the reconciliation; the tail
    of the published range is reported separately because it is expected to differ."""
    settled = _daily_summary(con, "is_settled")
    tail = _daily_summary(con, "is_comparable AND NOT is_settled")
    worst = _rows(
        con,
        """
        SELECT user_key, activity_date, published_listen_count, our_listen_count, difference,
               is_settled
        FROM marts.ver_user_daily_listens
        WHERE is_comparable AND difference <> 0
        ORDER BY is_settled DESC, abs(difference) DESC, user_key, activity_date LIMIT 15
        """,
    )
    return {
        **settled,
        "tail_user_days": tail.get("user_days"),
        "tail_exact_matches": tail.get("exact_matches"),
        "tail_published_listens": tail.get("published_listens"),
        "tail_our_listens": tail.get("our_listens"),
        "largest_differences": worst,
    }


def user_top_artists(con, top_n: int = 10) -> dict[str, Any]:
    """Each user's published top-``top_n`` artists for the stats week versus our count of that
    user's listens crediting the artist (any position in the artist credit, as ListenBrainz
    attributes listens) inside the same [from_ts, to_ts) window."""
    rows = _rows(
        con,
        f"""
        WITH top AS (
            SELECT user_key, rank, artist_mbid, artist_name, published_listen_count,
                   from_ts, to_ts, stats_last_updated
            FROM staging.stg_lb__user_top_artists
            WHERE rank <= {int(top_n)} AND artist_mbid IS NOT NULL
        ), coverage AS (
            SELECT user_key, min(listened_at) AS first_listen FROM staging.stg_lb__listens
            GROUP BY user_key
        ), ours AS (
            SELECT t.user_key, t.rank, count(l.event_id) AS our_listen_count
            FROM top t
            LEFT JOIN staging.stg_lb__listens l
              ON l.user_key = t.user_key
             AND list_contains(l.artist_mbids, t.artist_mbid)
             AND l.listened_at >= t.from_ts AND l.listened_at < t.to_ts
            GROUP BY t.user_key, t.rank
        )
        SELECT t.user_key, t.rank, t.artist_name, t.published_listen_count, o.our_listen_count,
               o.our_listen_count - t.published_listen_count AS difference,
               t.from_ts, t.to_ts
        FROM top t
        JOIN ours o USING (user_key, rank)
        JOIN coverage c USING (user_key)
        WHERE c.first_listen < t.from_ts AND t.to_ts <= t.stats_last_updated
        ORDER BY t.user_key, t.rank
        """,
    )
    n = len(rows)

    def share_within(pct: float) -> int:
        return sum(
            1
            for r in rows
            if abs(r["difference"]) <= max(1, pct * (r["published_listen_count"] or 0))
        )

    return {
        "user_artist_pairs": n,
        "users": len({r["user_key"] for r in rows}),
        "exact_matches": sum(1 for r in rows if r["difference"] == 0),
        "within_5pct": share_within(0.05),
        "within_20pct": share_within(0.20),
        "published_listens": sum(r["published_listen_count"] or 0 for r in rows),
        "our_listens": sum(r["our_listen_count"] or 0 for r in rows),
        "largest_differences": sorted(rows, key=lambda r: -abs(r["difference"]))[:10],
    }


def episode_counts(con) -> dict[str, Any]:
    s = _one(
        con,
        """
        SELECT count(*) AS shows,
               sum(CASE WHEN episode_count_difference = 0 THEN 1 ELSE 0 END) AS exact_matches,
               sum(CASE WHEN episode_count_difference < 0 THEN 1 ELSE 0 END) AS feed_has_fewer,
               sum(CASE WHEN episode_count_difference > 0 THEN 1 ELSE 0 END) AS feed_has_more,
               sum(CASE WHEN abs(latest_episode_hours_difference) <= 1 THEN 1 ELSE 0 END)
                   AS latest_episode_within_1h
        FROM marts.ver_podcast_episode_counts
        """,
    )
    rows = _rows(
        con,
        """
        SELECT podcast_name, itunes_episode_count, rss_episode_count, episode_count_difference,
               round(latest_episode_hours_difference, 2) AS latest_episode_hours_difference
        FROM marts.ver_podcast_episode_counts
        ORDER BY abs(episode_count_difference) DESC, podcast_name LIMIT 15
        """,
    )
    return {**s, "rows": rows}


def release_dates(con) -> dict[str, Any]:
    s = _one(
        con,
        """
        SELECT count(*) AS releases,
               sum(CASE WHEN days_difference = 0 THEN 1 ELSE 0 END) AS exact_matches,
               sum(CASE WHEN days_difference > 0 THEN 1 ELSE 0 END) AS group_released_earlier,
               sum(CASE WHEN days_difference < 0 THEN 1 ELSE 0 END) AS group_date_later,
               sum(CASE WHEN musicbrainz_first_release_date IS NULL THEN 1 ELSE 0 END)
                   AS partial_or_missing_group_date
        FROM marts.ver_release_dates
        """,
    )
    rows = _rows(
        con,
        """
        SELECT release_name, listenbrainz_release_date, musicbrainz_first_release_date_raw,
               days_difference
        FROM marts.ver_release_dates
        WHERE days_difference IS NULL OR days_difference <> 0
        ORDER BY abs(coalesce(days_difference, 0)) DESC, release_name LIMIT 10
        """,
    )
    return {**s, "rows": rows}


def run_verification(settings: Settings, out: Path | None = None) -> dict[str, Any]:
    con = connect(settings, read_only=True)
    try:
        checks = {
            "user_daily_listens": user_daily(con),
            "user_top_artists": user_top_artists(con),
            "podcast_episode_counts": episode_counts(con),
            "release_dates": release_dates(con),
        }
    finally:
        con.close()
    summary = {
        name: {k: v for k, v in c.items() if not isinstance(v, list)} for name, c in checks.items()
    }
    result = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "summary": summary,
        "details": checks,
    }
    settings.ops_dir.mkdir(parents=True, exist_ok=True)
    for path in [settings.ops_dir / "verification.json", out]:
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(result, indent=1, default=str), encoding="utf-8")
    return result
