"""Entrypoint for the deployed Glue jobs (one job per pipeline step, see infra/modules/glue_jobs).

Glue starts every job with this script and ``--step <name>``. Glue workers are ephemeral, so
the file-based state the steps keep locally (bronze landing zone, ``ops/`` cursors, panel,
DQ results) lives in the lake bucket and is synced around the step:

1. download ``s3://<lake>/bronze/`` and ``s3://<lake>/state/ops/`` into a scratch data dir;
2. run ``musicpods.orchestration.steps.STEPS[step]`` with the Glue Data Catalog as the Iceberg
   catalog (``MUSICPODS_ICEBERG_CATALOG=glue``) and real AWS clients (``MUSICPODS_AWS_MODE=aws``);
3. upload every file the step created or changed. Bronze is immutable, so uploads never
   overwrite a different version of a bronze object.

Locally the same steps run in-process (``musicpods run``); this module is exercised by a unit
test against moto, not against a real account (none is used by this repository).
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
import tempfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

log = logging.getLogger("musicpods.glue")

SYNCED = (("bronze/", "bronze"), ("state/ops/", "ops"))


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(root: Path) -> dict[str, str]:
    """Relative path -> sha256 of every file under ``root``."""
    if not root.exists():
        return {}
    return {p.relative_to(root).as_posix(): _digest(p) for p in root.rglob("*") if p.is_file()}


def sync_down(s3: Any, bucket: str, prefix: str, dest: Path) -> int:
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            target = dest / obj["Key"][len(prefix) :]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())
            n += 1
    return n


def sync_up(s3: Any, bucket: str, prefix: str, src: Path, before: dict[str, str]) -> list[str]:
    uploaded = []
    for rel, digest in sorted(snapshot(src).items()):
        if before.get(rel) == digest:
            continue
        s3.put_object(Bucket=bucket, Key=prefix + rel, Body=(src / rel).read_bytes())
        uploaded.append(prefix + rel)
    return uploaded


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="musicpods-glue")
    p.add_argument("--step", required=True)
    p.add_argument("--lake_bucket", required=True)
    p.add_argument("--firehose_stream")
    p.add_argument("--athena_workgroup")
    p.add_argument("--run_date", help="ISO date or timestamp; defaults to today (UTC)")
    p.add_argument("--data_dir", help="scratch directory (default: a new temp dir)")
    args, _ = p.parse_known_args(argv)  # Glue adds --JOB_ID, --JOB_RUN_ID, ... of its own
    return args


def main(argv: Sequence[str] | None = None, environ: dict[str, str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    env = os.environ if environ is None else environ
    data_dir = Path(args.data_dir or tempfile.mkdtemp(prefix="musicpods-"))
    # Deployed defaults; an explicitly set variable wins (the unit test keeps moto + SQLite).
    env.setdefault("MUSICPODS_AWS_MODE", "aws")
    env.setdefault("MUSICPODS_ICEBERG_CATALOG", "glue")
    env.setdefault("MUSICPODS_LOG_FORMAT", "json")
    env["MUSICPODS_DATA_DIR"] = str(data_dir)
    env["MUSICPODS_LAKE_BUCKET"] = args.lake_bucket
    if args.firehose_stream:
        env["MUSICPODS_FIREHOSE_STREAM"] = args.firehose_stream
    if args.athena_workgroup:
        env["MUSICPODS_ATHENA_WORKGROUP"] = args.athena_workgroup

    from musicpods import logs
    from musicpods.aws import aws_clients
    from musicpods.config import Settings
    from musicpods.orchestration.steps import STEPS

    settings = Settings.from_env(env)
    logs.configure(settings.log_level, settings.log_format)
    if args.step not in STEPS:
        log.error("unknown step %r (known: %s)", args.step, ", ".join(sorted(STEPS)))
        return 2
    run_date = date.fromisoformat(args.run_date[:10]) if args.run_date else None
    ctx: dict[str, Any] = {
        "run_date": run_date.isoformat() if run_date else None,
        "run_date_obj": run_date,
        "dbt_target": "athena" if settings.iceberg_catalog == "glue" else "dev",
    }
    with aws_clients(settings) as aws:
        before: dict[str, dict[str, str]] = {}
        for prefix, local in SYNCED:
            n = sync_down(aws.s3, settings.lake_bucket, prefix, data_dir / local)
            before[local] = snapshot(data_dir / local)
            log.info("synced %d objects from s3://%s/%s", n, settings.lake_bucket, prefix)
        try:
            output = STEPS[args.step](settings, ctx)
            log.info("step %s finished: %s", args.step, output)
        finally:
            # Upload state even when the step fails: cursors and DQ results written before the
            # failure are what the next attempt (and the operator) need.
            for prefix, local in SYNCED:
                up = sync_up(aws.s3, settings.lake_bucket, prefix, data_dir / local, before[local])
                log.info(
                    "uploaded %d changed files to s3://%s/%s", len(up), args.lake_bucket, prefix
                )
    return 0


if __name__ == "__main__":  # pragma: no cover - Glue runs the script directly
    raise SystemExit(main())
