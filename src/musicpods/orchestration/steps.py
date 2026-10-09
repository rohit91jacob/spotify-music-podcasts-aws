"""The units of work behind each Step Functions Task (one Glue job each in AWS)."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from musicpods import lakehouse
from musicpods.aws import LocalStore, S3Store, aws_clients
from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.glue.jobs import run_jobs
from musicpods.ingest import run_ingest
from musicpods.streaming.replay import replay
from musicpods.streaming.sink import run_sink
from musicpods.warehouse import load_raw, run_dbt

log = logging.getLogger(__name__)

Context = dict[str, Any]
Step = Callable[[Settings, Context], dict[str, Any]]

FIXTURE_SOURCES = {
    "listenbrainz": ("listenbrainz",),
    "musicbrainz": ("musicbrainz",),
    "podcasts": ("apple", "rss"),
    "optional": ("lastfm", "podcastindex", "spotify"),
}


def _ingest(source: str) -> Step:
    def step(settings: Settings, ctx: Context) -> dict[str, Any]:
        from_dir: Path | None = ctx.get("from_dir")
        if from_dir is not None:
            n = Bronze(settings.bronze_dir).import_tree(from_dir, sources=FIXTURE_SOURCES[source])
            return {"mode": "fixtures", "files": n}
        return run_ingest(settings, sources=(source,), today=ctx.get("run_date_obj"))

    step.__name__ = f"ingest_{source}"
    return step


def replay_listens(settings: Settings, ctx: Context) -> dict[str, Any]:
    with aws_clients(settings) as aws:
        return replay(
            aws, settings, Bronze(settings.bronze_dir), full=bool(ctx.get("full_replay"))
        ).as_dict()


def listen_events_to_iceberg(settings: Settings, ctx: Context) -> dict[str, Any]:
    if settings.lake_in_s3:
        with aws_clients(settings) as aws:
            return run_sink(settings, S3Store(aws.s3, settings.lake_bucket)).as_dict()
    return run_sink(settings, LocalStore(settings.s3_mirror_dir)).as_dict()


def build_raw_tables(settings: Settings, ctx: Context) -> dict[str, Any]:
    return run_jobs(settings)


def dbt_build(settings: Settings, ctx: Context) -> dict[str, Any]:
    target = ctx.get("dbt_target", "dev")
    if target == "athena":
        # Athena reads the Iceberg tables straight from the Glue catalog; no local copy.
        run_dbt(settings, ["build", "--target", target])
        return {"target": target}
    loaded = load_raw(settings)
    run_dbt(settings, ["build", "--target", target])
    return {"target": target, "raw_tables": len(loaded)}


def maintenance(settings: Settings, ctx: Context) -> dict[str, Any]:
    return lakehouse.expire_snapshots(lakehouse.catalog(settings), settings.snapshot_retention_days)


STEPS: dict[str, Step] = {
    "ingest-listenbrainz": _ingest("listenbrainz"),
    "ingest-musicbrainz": _ingest("musicbrainz"),
    "ingest-podcasts": _ingest("podcasts"),
    "ingest-optional": _ingest("optional"),
    "replay-listens": replay_listens,
    "listen-events-to-iceberg": listen_events_to_iceberg,
    "build-raw-tables": build_raw_tables,
    "dbt-build": dbt_build,
    "maintenance": maintenance,
}
