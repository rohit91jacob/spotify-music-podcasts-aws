"""Firehose -> S3 -> Iceberg-on-S3 through a network AWS endpoint.

Runs only with ``-m integration`` and ``MUSICPODS_AWS_ENDPOINT_URL`` pointing at an emulator:
LocalStack and moto_server in CI (.github/workflows/ci.yml), or ``moto_server -p 5000`` locally.
Everything (bucket, delivery stream, Iceberg data and metadata files) lives in the emulator;
the Iceberg catalog itself is SQLite, as in the local default.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import date

import pytest

from musicpods import lakehouse
from musicpods.aws import S3Store, aws_clients
from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.streaming.replay import replay
from musicpods.streaming.sink import run_sink

# Read at import time: the autouse fixture in conftest.py clears MUSICPODS_* for each test.
ENDPOINT = os.environ.get("MUSICPODS_AWS_ENDPOINT_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not ENDPOINT, reason="needs MUSICPODS_AWS_ENDPOINT_URL (an AWS emulator)"),
]

TS = 1_791_500_000


def _listens(n: int) -> list[dict]:
    return [
        {
            "user_key": "u_integration",
            "listened_at": TS + 120 * i,
            "inserted_at": TS + 120 * i + 1,
            "recording_msid": f"msid-{i}",
            "track_metadata": {
                "artist_name": "Artist",
                "track_name": f"Track {i}",
                "additional_info": {"duration_ms": 180_000},
            },
        }
        for i in range(n)
    ]


def _wait_for_delivery(s3, bucket: str, prefix: str, timeout_s: int = 180) -> list[str]:
    """Firehose flushes after its buffering interval; moto delivers at once, LocalStack later."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        keys = [
            o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
        ]
        if keys:
            return keys
        time.sleep(3)
    raise AssertionError(f"no Firehose objects under s3://{bucket}/{prefix} after {timeout_s}s")


def test_firehose_to_iceberg_on_s3(tmp_path):
    run = uuid.uuid4().hex[:8]
    settings = Settings(
        data_dir=tmp_path / "data",
        aws_endpoint_url=ENDPOINT,
        aws_mode="endpoint",
        lake_bucket=f"musicpods-it-{run}",
        firehose_stream=f"musicpods-it-{run}",
        pseudonym_salt="it",
    )
    bronze = Bronze(settings.bronze_dir)
    bronze.write("listenbrainz", "listens", date(2026, 10, 9), "page", {"listens": _listens(50)})

    with aws_clients(settings) as aws:
        result = replay(aws, settings, bronze, duplicate_every=10)
        assert result.events == 55
        assert result.mirrored_objects == 0  # endpoint mode reads S3 directly, no local mirror
        keys = _wait_for_delivery(aws.s3, settings.lake_bucket, settings.firehose_prefix)
        assert keys

        store = S3Store(aws.s3, settings.lake_bucket)
        cat = lakehouse.catalog(settings)
        sink = run_sink(settings, store, cat)
        assert sink.appended == 50
        assert sink.duplicates_in_batch == 5
        assert sink.table_rows == 50

        # The Iceberg data and metadata files were written to the emulated bucket.
        iceberg = aws.s3.list_objects_v2(Bucket=settings.lake_bucket, Prefix="iceberg/")
        names = [o["Key"] for o in iceberg.get("Contents", [])]
        assert any(k.endswith(".parquet") for k in names)
        assert any(k.endswith(".metadata.json") for k in names)

        # Rerun: nothing new to read, table unchanged.
        again = run_sink(settings, store, cat)
        assert (again.objects, again.appended, again.table_rows) == (0, 0, 50)
