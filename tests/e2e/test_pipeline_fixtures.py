"""End-to-end: the Step Functions definition, interpreted locally, over the committed fixtures.

One pipeline run is shared by the module (it takes about a minute); later tests rerun it and
check that nothing changes (idempotency), replay every listen again (at-least-once delivery),
and break a source file to prove the data-quality gate stops the run without touching the
published marts.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import duckdb
import pytest

from musicpods import cli, lakehouse
from musicpods.config import Settings
from musicpods.orchestration.runner import PipelineFailed, execute
from musicpods.report import build_site
from musicpods.verify import run_verification

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "bronze"

MARTS = [
    "dim_artist",
    "fct_listens",
    "fct_listening_sessions",
    "mart_artist_momentum",
    "mart_genre_share",
    "mart_listening_sessions_daily",
    "mart_short_plays_weekly",
    "mart_new_release_share",
    "mart_release_discovery_lag",
    "dim_podcast",
    "mart_podcast_chart_movement",
    "mart_podcast_chart_longevity",
    "mart_podcast_cross_country",
    "mart_episode_cadence",
]


def _fixture_listens() -> int:
    from musicpods.bronze import Bronze

    bronze = Bronze(FIXTURES)
    return sum(len(p["listens"]) for _, p in bronze.iter_payloads("listenbrainz", "listens"))


LISTENS = _fixture_listens()


def _ctx(full_replay: bool = False, from_dir: Path = FIXTURES) -> dict:
    return {
        "run_date": "2026-10-09",
        "run_date_obj": None,
        "from_dir": from_dir,
        "full_replay": full_replay,
        "dbt_target": "ci",
    }


def _fingerprint(settings: Settings) -> dict[str, tuple[int, str]]:
    """Row count and an order-independent content hash of every mart."""
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        out = {}
        for name in MARTS:
            rows = con.execute(f"SELECT * FROM marts.{name}").fetchall()
            digest = hashlib.sha256(
                "\n".join(sorted(repr(r) for r in rows)).encode("utf-8")
            ).hexdigest()
            out[name] = (len(rows), digest)
        return out
    finally:
        con.close()


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    data = tmp_path_factory.mktemp("e2e") / "data"
    settings = Settings(data_dir=data, pseudonym_salt="e2e")
    ex = execute(settings, _ctx())
    return settings, ex


def test_every_state_succeeds(run):
    _, ex = run
    states = [h["state"] for h in ex.history]
    assert states[-1] == "Done"
    assert all(h["status"] in {"succeeded", "skipped_wait"} for h in ex.history)
    assert LISTENS > 5000
    assert ex.outputs["replay-listens"]["events"] == LISTENS
    assert ex.outputs["listen-events-to-iceberg"]["table_rows"] == LISTENS


def test_marts_are_populated(run):
    settings, _ = run
    fp = _fingerprint(settings)
    empty = [name for name, (n, _) in fp.items() if n == 0]
    assert not empty, f"empty marts: {empty}"
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        # Sessionisation: every listen is in exactly one session.
        listens, in_sessions = con.execute(
            "SELECT (SELECT count(*) FROM marts.fct_listens),"
            " (SELECT sum(listens) FROM marts.fct_listening_sessions)"
        ).fetchone()
        assert listens == in_sessions
        # Observed playback from Spotify history imports is used where present.
        bases = dict(
            con.execute(
                "SELECT short_play_basis, count(*) FROM marts.fct_listens GROUP BY 1"
            ).fetchall()
        )
        assert bases.get("played_ms", 0) > 0 and bases.get("next_listen", 0) > 0
    finally:
        con.close()


def test_raw_dq_results_are_recorded(run):
    settings, _ = run
    results = [
        json.loads(x) for x in (settings.ops_dir / "dq_results.jsonl").read_text().splitlines()
    ]
    tables = {r["table"] for r in results}
    assert "raw.listen_events (batch)" in tables
    assert "raw.podcast_chart_entries" in tables
    assert not [r for r in results if r["severity"] == "error" and not r["passed"]]


def test_verification_reconciles_settled_days_exactly(run):
    settings, _ = run
    summary = run_verification(settings)["summary"]
    daily = summary["user_daily_listens"]
    assert daily["user_days"] > 0
    assert daily["exact_matches"] == daily["user_days"]
    assert summary["release_dates"]["releases"] > 0


def test_report_site(run, tmp_path):
    settings, _ = run
    run_verification(settings)
    out = build_site(settings, tmp_path / "site")
    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    assert "Artist momentum" in html or "artist momentum" in html.lower()
    assert "simulated" in html.lower()  # podcast fixtures are synthetic and must say so
    summary = json.loads((tmp_path / "site" / "summary.json").read_text())
    assert summary["listens"] == out["listens"] > 0


def test_rerun_is_idempotent(run):
    settings, _ = run
    before = _fingerprint(settings)
    cat = lakehouse.catalog(settings)
    snaps_before = len(list(cat.load_table("raw.listen_events").snapshots()))
    ex = execute(settings, _ctx())
    assert ex.outputs["replay-listens"]["events"] == 0
    assert ex.outputs["listen-events-to-iceberg"]["appended"] == 0
    assert _fingerprint(settings) == before
    assert len(list(cat.load_table("raw.listen_events").snapshots())) == snaps_before


def test_full_replay_is_deduplicated(run):
    settings, _ = run
    before = _fingerprint(settings)
    ex = execute(settings, _ctx(full_replay=True))
    assert ex.outputs["replay-listens"]["events"] == LISTENS
    sink = ex.outputs["listen-events-to-iceberg"]
    assert sink["appended"] == 0
    assert sink["already_in_table"] == LISTENS
    assert _fingerprint(settings) == before


def test_dq_gate_stops_bad_data_and_keeps_marts(run, tmp_path):
    settings, _ = run
    before = _fingerprint(settings)
    broken = tmp_path / "broken"
    shutil.copytree(FIXTURES, broken)
    # A new day's chart where two shows share rank 1: an error-level check on the raw table.
    chart = json.loads((broken / "apple/charts/dt=2026-10-08/us.json").read_text())
    chart["results"][1]["rank"] = 1
    target = broken / "apple/charts/dt=2026-10-10/us.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(chart))
    with pytest.raises(PipelineFailed) as err:
        execute(settings, _ctx(from_dir=broken))
    assert "unique(snapshot_date, country, chart_rank)" in str(err.value.cause)
    assert _fingerprint(settings) == before
    alerts = (settings.ops_dir / "alerts.jsonl").read_text().splitlines()
    assert json.loads(alerts[-1])["error"]["Error"] == "DataQualityError"
    # The same failure through the CLI exits 3.
    code = cli.main(
        [
            "--data-dir",
            str(settings.data_dir),
            "run",
            "--from-dir",
            str(broken),
            "--dbt-target",
            "ci",
        ]
    )
    assert code == cli.EXIT_DQ
