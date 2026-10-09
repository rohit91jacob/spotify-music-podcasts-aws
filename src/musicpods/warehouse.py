"""DuckDB warehouse (local stand-in for Athena) and the dbt runner.

``load_raw`` exposes the *current snapshot* of every Iceberg ``raw.*`` table to DuckDB as
``raw.<table>`` - the role the Glue Data Catalog plays for Athena. dbt then builds staging,
intermediate and mart models on top (``dbt/``), with the same SQL compiled for Athena in CI.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import duckdb

from musicpods import lakehouse
from musicpods.config import Settings

log = logging.getLogger(__name__)

DBT_DIR = Path(os.environ.get("MUSICPODS_DBT_DIR") or Path(__file__).resolve().parents[2] / "dbt")


class DbtError(RuntimeError):
    pass


class DbtTestFailure(DbtError):
    """dbt ran but at least one data test failed: a data-quality gate (exit code 3)."""


def connect(settings: Settings, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(settings.warehouse_path), read_only=read_only)


def load_raw(settings: Settings, cat=None) -> dict[str, int]:
    cat = cat or lakehouse.catalog(settings)
    counts: dict[str, int] = {}
    con = connect(settings)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        for name in lakehouse.list_tables(cat):
            arrow = lakehouse.read(cat, name)
            con.register("_src", arrow)
            con.execute(f"CREATE OR REPLACE TABLE raw.{name} AS SELECT * FROM _src")
            con.unregister("_src")
            counts[name] = arrow.num_rows
    finally:
        con.close()
    log.info("loaded %d raw tables into %s", len(counts), settings.warehouse_path)
    return counts


def dbt_env(settings: Settings) -> dict[str, str]:
    return {
        "MUSICPODS_DUCKDB_PATH": str(settings.warehouse_path.resolve()),
        "MUSICPODS_SESSION_GAP_MINUTES": str(settings.session_gap_minutes),
    }


def _dbt_command(args: list[str]) -> list[str]:
    # A child process: dbt-duckdb keeps its DuckDB connection open for the life of the process,
    # which would lock the warehouse for the verification and report steps that follow.
    code = "import sys; from dbt.cli.main import cli; sys.exit(cli())"
    return [sys.executable, "-c", code, *args]


def _failed_nodes(target: Path) -> tuple[list[str], list[str]]:
    """(failed or erroring tests, erroring non-test nodes) from dbt's run_results.json."""
    path = target / "run_results.json"
    if not path.exists():
        return [], []
    results = json.loads(path.read_text("utf-8")).get("results", [])
    tests = [
        r["unique_id"]
        for r in results
        if r["unique_id"].startswith("test.") and r.get("status") in {"fail", "error"}
    ]
    errors = [
        r["unique_id"]
        for r in results
        if not r["unique_id"].startswith("test.") and r.get("status") == "error"
    ]
    return tests, errors


def run_dbt(settings: Settings, args: list[str]) -> None:
    """Run dbt in a subprocess. Raises DbtTestFailure when only data tests failed (a DQ gate)
    and DbtError for anything else."""
    env = {**os.environ, **dbt_env(settings)}
    full = [*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    log.info("dbt %s", " ".join(args))
    target = DBT_DIR / "target"
    (target / "run_results.json").unlink(missing_ok=True)
    proc = subprocess.run(_dbt_command(full), env=env, cwd=DBT_DIR, check=False)  # noqa: S603
    if proc.returncode == 0:
        return
    tests, errors = _failed_nodes(target)
    if tests and not errors:
        raise DbtTestFailure(f"dbt tests failed: {', '.join(tests[:10])}")
    raise DbtError(
        f"dbt {' '.join(args)} failed (exit {proc.returncode}): {errors[:10] or 'see dbt output'}"
    )
