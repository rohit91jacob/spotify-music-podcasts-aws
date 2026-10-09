"""Replay landed ListenBrainz listens as a live event stream into Kinesis Data Firehose.

The producer reads bronze listen pages it has not replayed yet (``ops/replay_state.json``),
orders the listens by ``listened_at`` and sends them with ``PutRecordBatch`` (at most 500
records / 4 MiB per call), retrying only the records Firehose reports as failed. With
``speedup > 0`` it sleeps between batches so the stream runs at N x real time; 0 sends as fast
as possible.

Firehose delivers the records to the lake bucket under ``firehose/listen-events/YYYY/MM/DD/HH/``
(its default time-based prefix). In moto mode those objects are mirrored to ``data/s3/``.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any

from musicpods.aws import AwsClients, ensure_bucket, mirror_prefix
from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.streaming.events import encode, to_event

log = logging.getLogger(__name__)

MAX_BATCH_RECORDS = 500
MAX_BATCH_BYTES = 4 * 1024 * 1024
MAX_RETRIES = 5


@dataclass
class ReplayResult:
    pages: int = 0
    events: int = 0
    batches: int = 0
    retried_records: int = 0
    mirrored_objects: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


def ensure_stream(aws: AwsClients, settings: Settings) -> None:
    """Emulator only: create the bucket and delivery stream (Terraform owns them in AWS)."""
    if aws.emulator == "aws":
        return
    ensure_bucket(aws.s3, settings.lake_bucket, settings.aws_region)
    names = aws.firehose.list_delivery_streams().get("DeliveryStreamNames", [])
    if settings.firehose_stream in names:
        return
    aws.firehose.create_delivery_stream(
        DeliveryStreamName=settings.firehose_stream,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "RoleARN": aws.iam_role_arn,
            "BucketARN": f"arn:aws:s3:::{settings.lake_bucket}",
            "Prefix": settings.firehose_prefix,
            "ErrorOutputPrefix": settings.firehose_error_prefix,
            "BufferingHints": {"SizeInMBs": 64, "IntervalInSeconds": 60},
            "CompressionFormat": "UNCOMPRESSED",
        },
    )
    for _ in range(30):
        desc = aws.firehose.describe_delivery_stream(DeliveryStreamName=settings.firehose_stream)
        if desc["DeliveryStreamDescription"]["DeliveryStreamStatus"] == "ACTIVE":
            return
        time.sleep(1)


def _batches(records: list[bytes]):
    batch: list[bytes] = []
    size = 0
    for rec in records:
        if batch and (len(batch) >= MAX_BATCH_RECORDS or size + len(rec) > MAX_BATCH_BYTES):
            yield batch
            batch, size = [], 0
        batch.append(rec)
        size += len(rec)
    if batch:
        yield batch


def put_batch(firehose: Any, stream: str, batch: list[bytes], sleep=time.sleep) -> int:
    """Send one batch; resend only failed records with backoff. Returns records retried."""
    pending = batch
    retried = 0
    for attempt in range(MAX_RETRIES + 1):
        resp = firehose.put_record_batch(
            DeliveryStreamName=stream, Records=[{"Data": r} for r in pending]
        )
        if resp.get("FailedPutCount", 0) == 0:
            return retried
        pending = [
            rec
            for rec, res in zip(pending, resp["RequestResponses"], strict=True)
            if res.get("ErrorCode")
        ]
        retried += len(pending)
        if attempt < MAX_RETRIES:
            sleep(min(8.0, 0.25 * 2**attempt))
    raise RuntimeError(f"{len(pending)} records still failing after {MAX_RETRIES} retries")


def replay(
    aws: AwsClients,
    settings: Settings,
    bronze: Bronze,
    full: bool = False,
    limit: int | None = None,
    duplicate_every: int = 0,
) -> ReplayResult:
    """``duplicate_every=N`` re-sends every Nth event (drill for the sink's dedup)."""
    state_path = settings.ops_dir / "replay_state.json"
    done: set[str] = set()
    if state_path.exists() and not full:
        done = set(json.loads(state_path.read_text("utf-8")))
    ensure_stream(aws, settings)
    result = ReplayResult()
    pending_pages = [
        f for f in bronze.files("listenbrainz", "listens") if f.path.as_posix() not in done
    ]
    events = []
    for f in pending_pages:
        events.extend(to_event(listen) for listen in bronze.read(f).get("listens", []))
    events.sort(key=lambda e: (e["listened_at"], e["event_id"]))
    if limit is not None:
        events = events[:limit]
    records = []
    for i, e in enumerate(events, start=1):
        records.append(encode(e))
        if duplicate_every and i % duplicate_every == 0:
            records.append(encode(e))
    result.pages = len(pending_pages)
    result.events = len(records)
    previous_ts = None
    for batch in _batches(records):
        if settings.replay_speedup > 0 and previous_ts is not None:
            first_ts = json.loads(batch[0])["listened_at"]
            time.sleep(max(0.0, (first_ts - previous_ts) / settings.replay_speedup))
        result.retried_records += put_batch(aws.firehose, settings.firehose_stream, batch)
        result.batches += 1
        previous_ts = json.loads(batch[-1])["listened_at"]
    if aws.emulator == "moto":
        result.mirrored_objects = mirror_prefix(
            aws.s3, settings.lake_bucket, settings.firehose_prefix, settings.s3_mirror_dir
        )
    if limit is None:
        done |= {f.path.as_posix() for f in pending_pages}
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(sorted(done), indent=0), encoding="utf-8")
    log.info("replayed %s", result.as_dict())
    return result
