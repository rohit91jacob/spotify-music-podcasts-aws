"""``musicpods`` command line.

Exit codes: 0 ok, 1 unexpected error, 2 invalid configuration, 3 data-quality gate failed
(raw-layer DQ checks or dbt tests), 4 dbt failed for another reason (compile/runtime).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from musicpods import logs
from musicpods.config import ConfigError, Settings
from musicpods.quality.checks import DataQualityError

log = logging.getLogger("musicpods")

EXIT_CONFIG = 2
EXIT_DQ = 3
EXIT_DBT = 4


def _print_json(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def cmd_ingest(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.ingest import SOURCES, run_ingest

    sources = tuple(a.source) if a.source else SOURCES
    _print_json(run_ingest(s, sources=sources, from_dir=a.from_dir, lb_backfill=a.lb_backfill))
    return 0


def cmd_replay(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.aws import aws_clients
    from musicpods.bronze import Bronze
    from musicpods.streaming.replay import replay

    with aws_clients(s) as aws:
        result = replay(
            aws,
            s,
            Bronze(s.bronze_dir),
            full=a.full,
            limit=a.limit,
            duplicate_every=a.duplicate_every,
        )
    _print_json(result.as_dict())
    return 0


def cmd_sink(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.orchestration.steps import listen_events_to_iceberg

    _print_json(listen_events_to_iceberg(s, {}))
    return 0


def cmd_glue(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.glue.jobs import run_jobs

    _print_json(run_jobs(s, a.job or None))
    return 0


def cmd_load(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.warehouse import load_raw

    _print_json(load_raw(s))
    return 0


def cmd_dbt(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.warehouse import load_raw, run_dbt

    args = a.dbt_args or ["build"]
    target = args[args.index("--target") + 1] if "--target" in args[:-1] else None
    if args[0] in {"build", "run", "test", "seed", "snapshot"} and target != "athena":
        # DuckDB reads a copy of the Iceberg raw tables; refresh it so dbt sees the latest data.
        load_raw(s)
    run_dbt(s, args)
    return 0


def cmd_run(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.orchestration.runner import execute

    run_date = a.run_date or datetime.now(UTC).date()
    ctx = {
        "run_date": run_date.isoformat(),
        "run_date_obj": None if a.from_dir else run_date,
        "from_dir": a.from_dir,
        "full_replay": a.full_replay,
        "dbt_target": a.dbt_target,
    }
    ex = execute(s, ctx)
    _print_json(
        {
            "execution_id": ex.execution_id,
            "states": [(h["state"], h["status"], h["seconds"]) for h in ex.history],
            "outputs": ex.outputs,
        }
    )
    return 0


def cmd_verify(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.verify import run_verification

    result = run_verification(s, a.out)
    _print_json(result["summary"])
    return 0


def cmd_report(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.report import build_site

    _print_json(build_site(s, a.out))
    return 0


def cmd_sql(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.warehouse import connect

    con = connect(s, read_only=True)
    try:
        if a.list:
            rows = con.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema IN ('raw','marts') ORDER BY 1, 2"
            ).fetchall()
            for schema, name in rows:
                print(f"{schema}.{name}")
            return 0
        rel = con.sql(a.query)
        print(rel.limit(a.max_rows) if rel is not None else "")
    finally:
        con.close()
    return 0


def cmd_asl(s: Settings, a: argparse.Namespace) -> int:
    from musicpods.orchestration.runner import definition_text, render, validate

    defn = render(definition_text(), job_prefix=a.job_prefix)
    if a.action == "render":
        _print_json(defn)
        return 0
    if a.action == "register":
        from musicpods.aws import aws_clients
        from musicpods.orchestration.runner import register

        with aws_clients(s) as aws:
            _print_json(register(aws, defn))
        return 0
    problems = validate(defn)
    for p in problems:
        print(p, file=sys.stderr)
    if not problems:
        print("state machine definition is valid")
    return 1 if problems else 0


def cmd_maintain(s: Settings, a: argparse.Namespace) -> int:
    from musicpods import lakehouse

    _print_json(lakehouse.expire_snapshots(lakehouse.catalog(s), s.snapshot_retention_days))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="musicpods", description=__doc__.splitlines()[0])
    p.add_argument("--data-dir", type=Path, help="override MUSICPODS_DATA_DIR")
    sub = p.add_subparsers(dest="command", required=True)

    i = sub.add_parser("ingest", help="download sources into bronze (or import a fixture tree)")
    i.add_argument(
        "--source",
        action="append",
        choices=["listenbrainz", "musicbrainz", "podcasts", "optional"],
        help="repeatable; default: all",
    )
    i.add_argument("--from-dir", type=Path, help="import a bronze-shaped fixture tree instead")
    i.add_argument(
        "--lb-backfill",
        action="store_true",
        help="re-fetch the whole ListenBrainz window, ignoring saved cursors (sink deduplicates)",
    )
    i.set_defaults(func=cmd_ingest)

    r = sub.add_parser("replay", help="replay new listens into Kinesis Data Firehose")
    r.add_argument("--full", action="store_true", help="replay every landed listen again")
    r.add_argument("--limit", type=int, help="send only the first N events (no state update)")
    r.add_argument("--duplicate-every", type=int, default=0, help="re-send every Nth event")
    r.set_defaults(func=cmd_replay)

    sub.add_parser(
        "sink", help="commit Firehose S3 output to Iceberg raw.listen_events"
    ).set_defaults(func=cmd_sink)

    g = sub.add_parser("glue", help="run the Glue-style batch jobs (bronze -> Iceberg raw)")
    g.add_argument("--job", action="append", help="repeatable; default: all jobs")
    g.set_defaults(func=cmd_glue)

    sub.add_parser("load", help="expose Iceberg raw tables to DuckDB").set_defaults(func=cmd_load)

    d = sub.add_parser("dbt", help="run dbt against the DuckDB warehouse")
    d.add_argument("dbt_args", nargs=argparse.REMAINDER)
    d.set_defaults(func=cmd_dbt)

    run = sub.add_parser("run", help="execute the Step Functions definition locally, end to end")
    run.add_argument("--from-dir", type=Path, help="use a fixture tree instead of the network")
    run.add_argument("--run-date", type=date.fromisoformat)
    run.add_argument("--full-replay", action="store_true")
    run.add_argument("--dbt-target", default="dev")
    run.set_defaults(func=cmd_run)

    v = sub.add_parser("verify", help="reconcile outputs against published source figures")
    v.add_argument("--out", type=Path, help="also write verification.json here")
    v.set_defaults(func=cmd_verify)

    rep = sub.add_parser("report", help="build the static results site")
    rep.add_argument("--out", type=Path, default=Path("site"))
    rep.set_defaults(func=cmd_report)

    q = sub.add_parser("sql", help="query the DuckDB warehouse (read-only)")
    q.add_argument("query", nargs="?", default="SELECT 1")
    q.add_argument("--list", action="store_true", help="list raw and mart tables")
    q.add_argument("--max-rows", type=int, default=50)
    q.set_defaults(func=cmd_sql)

    asl = sub.add_parser("asl", help="validate or render the state machine definition")
    asl.add_argument(
        "action",
        choices=["validate", "render", "register"],
        help="register: create the state machine + daily schedule in the AWS emulator",
    )
    asl.add_argument("--job-prefix", default="musicpods-local")
    asl.set_defaults(func=cmd_asl)

    sub.add_parser("maintain", help="expire old Iceberg snapshots").set_defaults(func=cmd_maintain)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    if args.data_dir:
        settings = settings.with_data_dir(args.data_dir)
    logs.configure(settings.log_level, settings.log_format)

    from musicpods.orchestration.runner import PipelineFailed
    from musicpods.warehouse import DbtError, DbtTestFailure

    try:
        return args.func(settings, args)
    except PipelineFailed as exc:
        log.error("%s", exc)
        if isinstance(exc.cause, DataQualityError | DbtTestFailure):
            return EXIT_DQ
        if isinstance(exc.cause, DbtError):
            return EXIT_DBT
        return 1
    except (DataQualityError, DbtTestFailure) as exc:
        log.error("%s", exc)
        return EXIT_DQ
    except DbtError as exc:
        log.error("%s", exc)
        return EXIT_DBT
