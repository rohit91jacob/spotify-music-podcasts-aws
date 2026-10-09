"""Data-quality gates between layers.

Every raw table written by a Glue-style job (and every Firehose batch the sink commits) is
checked *before* the Iceberg commit. A failed ``error`` check stops the step (exit code 3) and
nothing is written; ``warn`` checks are recorded only. Marts are gated by the dbt tests.

Each check is a predicate selecting *violating* rows (or a query returning a violation count),
evaluated with DuckDB over the Arrow batch. Results are appended to ``ops/dq_results.jsonl``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import duckdb
import pyarrow as pa

from musicpods.config import Settings

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    def __init__(self, failures: list[CheckResult]):
        names = ", ".join(f"{f.table}:{f.check}({f.violations})" for f in failures)
        super().__init__(f"data-quality checks failed: {names}")
        self.failures = failures


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    severity: str = "error"
    kind: str = "rows"  # rows: SELECT count(*) FROM t WHERE <sql>; query: <sql> returns a count


@dataclass
class CheckResult:
    table: str
    check: str
    severity: str
    violations: int
    rows: int
    passed: bool
    checked_at: str


def not_null(*cols: str, severity: str = "error") -> Check:
    return Check(f"not_null({','.join(cols)})", " OR ".join(f"{c} IS NULL" for c in cols), severity)


def unique(*cols: str, severity: str = "error") -> Check:
    key = ", ".join(cols)
    return Check(
        f"unique({key})",
        f"SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM t GROUP BY {key}) WHERE n > 1",
        severity,
        "query",
    )


def in_range(col: str, lo=None, hi=None, severity: str = "error") -> Check:
    parts = []
    if lo is not None:
        parts.append(f"{col} < {lo}")
    if hi is not None:
        parts.append(f"{col} > {hi}")
    return Check(f"range({col},{lo},{hi})", " OR ".join(parts), severity)


def accepted(col: str, values: tuple[str, ...], severity: str = "error") -> Check:
    vals = ", ".join(f"'{v}'" for v in values)
    return Check(f"accepted({col})", f"{col} IS NOT NULL AND {col} NOT IN ({vals})", severity)


def min_rows(n: int, severity: str = "error") -> Check:
    return Check(
        f"min_rows({n})",
        f"SELECT CASE WHEN count(*) < {n} THEN 1 ELSE 0 END FROM t",
        severity,
        "query",
    )


def expr(name: str, predicate: str, severity: str = "error") -> Check:
    return Check(name, predicate, severity)


UUID_RE = "'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'"


TABLE_CHECKS: dict[str, list[Check]] = {
    "listen_events": [
        min_rows(1),
        not_null("event_id", "user_key", "listened_at"),
        unique("event_id"),
        expr("listened_at_not_in_future", "listened_at > now()::TIMESTAMP + INTERVAL 1 DAY"),
        expr("listened_at_after_2002", "listened_at < TIMESTAMP '2002-01-01'"),
        expr(
            "recording_mbid_is_uuid",
            f"recording_mbid IS NOT NULL AND NOT regexp_matches(recording_mbid, {UUID_RE})",
        ),
        in_range("duration_ms", 1, 4 * 3600 * 1000, severity="warn"),
        in_range("played_ms", 0, 24 * 3600 * 1000, severity="warn"),
        Check(
            "mbid_mapping_coverage_below_50pct",
            "SELECT CASE WHEN avg(CASE WHEN recording_mbid IS NULL THEN 0 ELSE 1 END) < 0.5 THEN 1 ELSE 0 END FROM t",
            "warn",
            "query",
        ),
    ],
    "mb_artists": [
        not_null("artist_mbid", "name"),
        unique("artist_mbid"),
        expr("artist_mbid_is_uuid", f"NOT regexp_matches(artist_mbid, {UUID_RE})"),
    ],
    "mb_artist_tags": [
        not_null("artist_mbid", "tag"),
        unique("artist_mbid", "tag"),
        in_range("votes", -1000, None, severity="warn"),
    ],
    "mb_recordings": [
        not_null("recording_mbid"),
        unique("recording_mbid"),
        in_range("length_ms", 1, 24 * 3600 * 1000, severity="warn"),
    ],
    "mb_release_groups": [
        not_null("release_group_mbid"),
        unique("release_group_mbid"),
        expr(
            "first_release_date_format",
            "first_release_date IS NOT NULL AND first_release_date <> '' AND NOT regexp_matches(first_release_date, '^\\d{4}(-\\d{2}(-\\d{2})?)?$')",
        ),
    ],
    "lb_fresh_releases": [
        not_null("snapshot_date", "release_mbid", "release_date"),
        unique("snapshot_date", "release_mbid"),
    ],
    "lb_sitewide_artists": [
        not_null("snapshot_date", "rank", "artist_name", "listen_count"),
        unique("snapshot_date", "stats_range", "rank"),
        in_range("listen_count", 0),
    ],
    "lb_user_daily_activity": [
        not_null("user_key", "activity_date", "published_listen_count"),
        unique("snapshot_date", "user_key", "activity_date"),
        in_range("published_listen_count", 0),
    ],
    "lb_user_top_artists": [
        not_null("user_key", "rank"),
        unique("snapshot_date", "user_key", "rank"),
    ],
    "podcast_chart_entries": [
        min_rows(1),
        not_null("snapshot_date", "country", "chart_rank", "podcast_id"),
        unique("snapshot_date", "country", "chart_rank"),
        unique("snapshot_date", "country", "podcast_id", severity="warn"),
        in_range("chart_rank", 1, 200),
        expr("country_is_iso2", "NOT regexp_matches(country, '^[a-z]{2}$')"),
    ],
    "podcast_shows": [
        not_null("lookup_date", "country", "podcast_id"),
        unique("lookup_date", "country", "podcast_id"),
        in_range("published_episode_count", 0, severity="warn"),
    ],
    "podcast_episodes": [
        not_null("fetched_date", "podcast_id", "episode_key"),
        unique("fetched_date", "podcast_id", "episode_key", severity="warn"),
        expr(
            "published_at_not_in_future", "published_at > now()::TIMESTAMP + INTERVAL 2 DAY", "warn"
        ),
        in_range("duration_seconds", 0, 24 * 3600, severity="warn"),
    ],
    "lastfm_top_artists": [unique("snapshot_date", "rank")],
    "podcastindex_feeds": [unique("snapshot_date", "podcast_id")],
    "spotify_artists": [unique("snapshot_date", "query_name")],
}


def evaluate(table: pa.Table, checks: list[Check]) -> list[tuple[Check, int]]:
    con = duckdb.connect()
    try:
        con.register("t", table)
        out = []
        for check in checks:
            if table.num_rows == 0 and check.kind == "rows":
                out.append((check, 0))
                continue
            sql = (
                check.sql if check.kind == "query" else f"SELECT count(*) FROM t WHERE {check.sql}"
            )
            out.append((check, int(con.execute(sql).fetchone()[0] or 0)))
        return out
    finally:
        con.close()


def run_checks(
    label: str, table: pa.Table, checks: list[Check], settings: Settings
) -> list[CheckResult]:
    now = datetime.now(UTC).isoformat(timespec="seconds")
    results = [
        CheckResult(label, c.name, c.severity, v, table.num_rows, v == 0, now)
        for c, v in evaluate(table, checks)
    ]
    settings.ops_dir.mkdir(parents=True, exist_ok=True)
    with (settings.ops_dir / "dq_results.jsonl").open("a", encoding="utf-8") as fh:
        for r in results:
            fh.write(json.dumps(asdict(r)) + "\n")
    for r in results:
        if not r.passed:
            (log.error if r.severity == "error" else log.warning)(
                "DQ %s %s: %d violating of %d rows", r.table, r.check, r.violations, r.rows
            )
    failures = [r for r in results if not r.passed and r.severity == "error"]
    if failures and settings.dq_fail_on_error:
        raise DataQualityError(failures)
    return results
