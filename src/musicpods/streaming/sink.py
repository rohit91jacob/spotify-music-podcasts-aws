"""Firehose S3 output -> Iceberg ``raw.listen_events`` (the Glue job ``listen-events-to-iceberg``).

* Reads only objects it has not committed yet (``ops/sink_state.json``: key -> size).
* Validates every record; bad ones go to a dead-letter file with the reason
  (``ops/dlq/listen_events.jsonl``) instead of failing the batch.
* Drops duplicates inside the batch and against the table (anti-join on ``event_id``), then
  appends one Iceberg snapshot. Replaying the same events any number of times leaves the
  table unchanged: at-least-once delivery, exactly-once rows.
* The state file is written only after the Iceberg commit. A crash in between re-reads the
  same objects next time, and the anti-join makes that harmless.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from musicpods import lakehouse
from musicpods.config import Settings
from musicpods.glue.schemas import LISTEN_EVENTS
from musicpods.quality.checks import TABLE_CHECKS, run_checks
from musicpods.streaming.events import flatten, validate

log = logging.getLogger(__name__)

TABLE = "listen_events"


@dataclass
class SinkResult:
    objects: int = 0
    records: int = 0
    dead_letters: int = 0
    duplicates_in_batch: int = 0
    already_in_table: int = 0
    appended: int = 0
    table_rows: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


def _parse_object(body: bytes) -> list[Any]:
    out: list[Any] = []
    for line in body.decode("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            out.append({"_unparseable": line[:500]})
    return out


def run_sink(settings: Settings, store, cat=None) -> SinkResult:
    cat = cat or lakehouse.catalog(settings)
    state_path = settings.ops_dir / "sink_state.json"
    state: dict[str, int] = json.loads(state_path.read_text("utf-8")) if state_path.exists() else {}
    objects = [
        (k, size) for k, size in store.list(settings.firehose_prefix) if state.get(k) != size
    ]
    result = SinkResult(objects=len(objects))
    rows: dict[str, dict[str, Any]] = {}
    dlq: list[dict[str, Any]] = []
    for key, _ in objects:
        for record in _parse_object(store.get(key)):
            result.records += 1
            reason = "unparseable_json" if "_unparseable" in record else validate(record)
            if reason:
                dlq.append({"object": key, "reason": reason, "record": record})
                continue
            row = flatten(record)
            if row["event_id"] in rows:
                result.duplicates_in_batch += 1
                continue
            rows[row["event_id"]] = row
    result.dead_letters = len(dlq)
    if dlq:
        path = settings.ops_dir / "dlq" / "listen_events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            for item in dlq:
                fh.write(json.dumps(item, default=str) + "\n")

    table = lakehouse.get_or_create(cat, TABLE, LISTEN_EVENTS, partition_by=("listened_at", "day"))
    if rows:
        existing = table.scan(selected_fields=("event_id",)).to_arrow()
        seen = set(existing.column("event_id").to_pylist()) if existing.num_rows else set()
        now = datetime.now(UTC).replace(tzinfo=None)
        new = [dict(r, ingested_at=now) for eid, r in rows.items() if eid not in seen]
        result.already_in_table = len(rows) - len(new)
        if new:
            batch = pa.Table.from_pylist(new, schema=LISTEN_EVENTS)
            batch = batch.sort_by([("listened_at", "ascending"), ("event_id", "ascending")])
            run_checks(f"raw.{TABLE} (batch)", batch, TABLE_CHECKS[TABLE], settings)
            table.append(batch.cast(table.schema().as_arrow()))
            result.appended = len(new)
    for key, size in objects:
        state[key] = size
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=0, sort_keys=True), encoding="utf-8")
    table = cat.load_table(f"{lakehouse.ns(cat)}.{TABLE}")
    ids = table.scan(selected_fields=("event_id",)).to_arrow().column("event_id")
    result.table_rows = len(ids)
    if result.table_rows and pc.count_distinct(ids).as_py() != result.table_rows:
        raise RuntimeError("raw.listen_events has duplicate event_id values")
    log.info("sink %s", result.as_dict())
    return result
