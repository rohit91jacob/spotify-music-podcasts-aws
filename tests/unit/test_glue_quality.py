from __future__ import annotations

import json
from datetime import date

import pyarrow as pa
import pytest

from musicpods import lakehouse
from musicpods.bronze import Bronze
from musicpods.glue import jobs, schemas
from musicpods.quality.checks import (
    TABLE_CHECKS,
    DataQualityError,
    accepted,
    in_range,
    min_rows,
    not_null,
    run_checks,
    unique,
)

D1, D2 = date(2026, 10, 8), date(2026, 10, 9)


def _charts(bronze: Bronze, dt: date, ranks: list[tuple[int, str]], cc: str = "us") -> None:
    bronze.write(
        "apple",
        "charts",
        dt,
        cc,
        {
            "country": cc,
            "updated": f"{dt.isoformat()}T05:00:00Z",
            "results": [
                {"rank": r, "id": pid, "name": f"Show {pid}", "genres": ["News"]}
                for r, pid in ranks
            ],
        },
    )


# ----------------------------------------------------------------------------- checks
def test_check_helpers_count_violations(settings):
    t = pa.table({"k": [1, 1, 2, None], "v": [5, 50, 7, 1], "c": ["a", "b", "z", "a"]})
    results = run_checks(
        "t",
        t,
        [
            not_null("k", severity="warn"),
            unique("k", severity="warn"),
            in_range("v", 1, 10, severity="warn"),
            accepted("c", ("a", "b"), severity="warn"),
            min_rows(10, severity="warn"),
        ],
        settings,
    )
    assert [r.violations for r in results] == [1, 1, 1, 1, 1]
    logged = (settings.ops_dir / "dq_results.jsonl").read_text().splitlines()
    assert len(logged) == 5 and json.loads(logged[0])["table"] == "t"


def test_error_check_raises_unless_disabled(settings):
    t = pa.table({"k": [None]})
    with pytest.raises(DataQualityError):
        run_checks("t", t, [not_null("k")], settings)
    relaxed = settings.__class__(**{**settings.__dict__, "dq_fail_on_error": False})
    assert not run_checks("t", t, [not_null("k")], relaxed)[0].passed


def test_every_job_has_checks():
    assert set(jobs.JOBS) <= set(TABLE_CHECKS)


# ----------------------------------------------------------------------------- builders
def test_chart_builder_and_rerun_is_a_noop(settings):
    bronze = Bronze(settings.bronze_dir)
    _charts(bronze, D1, [(1, "10"), (2, "20")])
    _charts(bronze, D2, [(1, "20"), (2, "30")])
    first = jobs.run_jobs(settings, ["podcast_chart_entries"])
    assert first == {"podcast_chart_entries": 4}
    cat = lakehouse.catalog(settings)
    table = lakehouse.read(cat, "podcast_chart_entries")
    assert table.schema.names == schemas.PODCAST_CHART_ENTRIES.names
    before = sorted(table.to_pylist(), key=lambda r: (r["snapshot_date"], r["chart_rank"]))
    jobs.run_jobs(settings, ["podcast_chart_entries"])
    after = lakehouse.read(cat, "podcast_chart_entries").to_pylist()
    assert sorted(after, key=lambda r: (r["snapshot_date"], r["chart_rank"])) == before


def test_failed_gate_keeps_previous_snapshot(settings):
    bronze = Bronze(settings.bronze_dir)
    _charts(bronze, D1, [(1, "10"), (2, "20")])
    jobs.run_jobs(settings, ["podcast_chart_entries"])
    cat = lakehouse.catalog(settings)
    snap = cat.load_table("raw.podcast_chart_entries").current_snapshot().snapshot_id
    _charts(bronze, D2, [(1, "10"), (1, "20")])  # two shows at rank 1: an error-level failure
    with pytest.raises(DataQualityError, match="unique"):
        jobs.run_jobs(settings, ["podcast_chart_entries"])
    assert cat.load_table("raw.podcast_chart_entries").current_snapshot().snapshot_id == snap


def test_musicbrainz_builders_keep_latest_fetch_and_flag_genres(settings):
    bronze = Bronze(settings.bronze_dir)
    bronze.write("musicbrainz", "genres", D1, "all", {"genres": ["rock"]})
    a = "a1b2c3d4-0000-4000-8000-000000000001"
    bronze.write("musicbrainz", "artists", D1, "b0", [{"id": a, "name": "Old name"}])
    bronze.write(
        "musicbrainz",
        "artists",
        D2,
        "b0",
        [{"id": a, "name": "New name", "tags": [{"name": "Rock", "count": 2}, {"name": "90s"}]}],
    )
    artists = jobs.build_mb_artists(bronze).to_pylist()
    assert [(r["name"], r["fetched_on"]) for r in artists] == [("New name", D2)]
    tags = {r["tag"]: r for r in jobs.build_mb_artist_tags(bronze).to_pylist()}
    assert tags["rock"]["is_genre"] is True
    assert tags["90s"]["is_genre"] is False


def test_episode_builder_drops_repeated_items(settings):
    bronze = Bronze(settings.bronze_dir)
    item = {"episode_key": "e1", "published_at": "2026-10-01T00:00:00+00:00"}
    bronze.write(
        "rss", "episodes", D2, "11", {"podcast_id": "11", "status": "ok", "items": [item, item]}
    )
    rows = jobs.build_podcast_episodes(bronze).to_pylist()
    assert len(rows) == 1
    assert rows[0]["published_at"].isoformat() == "2026-10-01T00:00:00"


def test_iso_timestamp_parsing():
    assert jobs._iso_ts("2026-10-01T05:00:00Z").hour == 5
    assert jobs._iso_ts("Wed, 01 Oct 2026 05:00:00 +0200").hour == 3
    assert jobs._iso_ts("garbage") is None
    assert jobs._date("2026") is None
