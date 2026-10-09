"""AWS SDK access against an emulator.

* Default: **moto in-process** (``mock_aws``). Nothing leaves the process; resources are
  created on the fly and objects written to the emulated bucket are mirrored to
  ``data/s3/<bucket>/`` so later steps (and later runs) can read them.
* ``MUSICPODS_AWS_ENDPOINT_URL`` set (LocalStack in CI, or ``moto_server``): real boto3 calls
  against that endpoint with dummy credentials.
* ``MUSICPODS_AWS_MODE=aws``: ambient credentials. Only the Glue entrypoint
  (``musicpods.glue.entrypoint``) selects it, inside a deployed job. Nothing in this repository's
  local runs, tests or CI uses it; the Terraform in ``infra/`` is validated, not applied.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3

from musicpods.config import Settings

DUMMY_ACCOUNT = "000000000000"


@dataclass
class AwsClients:
    s3: Any
    firehose: Any
    stepfunctions: Any
    scheduler: Any
    iam_role_arn: str
    emulator: str  # "moto" | "endpoint"


def _client(service: str, settings: Settings) -> Any:
    kwargs: dict[str, Any] = {"region_name": settings.aws_region}
    if settings.aws_mode == "endpoint":
        if not settings.aws_endpoint_url:
            raise ValueError("MUSICPODS_AWS_MODE=endpoint needs MUSICPODS_AWS_ENDPOINT_URL")
        kwargs["endpoint_url"] = settings.aws_endpoint_url
        kwargs["aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID", "test")
        kwargs["aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    return boto3.client(service, **kwargs)


def _clients(settings: Settings, role: str, emulator: str) -> AwsClients:
    return AwsClients(
        s3=_client("s3", settings),
        firehose=_client("firehose", settings),
        stepfunctions=_client("stepfunctions", settings),
        scheduler=_client("scheduler", settings),
        iam_role_arn=role,
        emulator=emulator,
    )


@contextlib.contextmanager
def aws_clients(settings: Settings) -> Iterator[AwsClients]:
    """Clients for the configured mode. ``moto`` (the default) never leaves the process."""
    if settings.aws_mode == "endpoint":
        yield _clients(
            settings, f"arn:aws:iam::{DUMMY_ACCOUNT}:role/musicpods-emulated", "endpoint"
        )
        return
    if settings.aws_mode == "aws":
        # Deployed Glue jobs only: resources are owned by Terraform, nothing is created here.
        yield _clients(settings, "", "aws")
        return
    from moto import mock_aws

    with mock_aws():
        yield _clients(settings, "arn:aws:iam::123456789012:role/musicpods-emulated", "moto")


def ensure_bucket(s3: Any, bucket: str, region: str) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket in existing:
        return
    if region == "us-east-1":
        s3.create_bucket(Bucket=bucket)
    else:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})


def mirror_prefix(s3: Any, bucket: str, prefix: str, target: Path) -> int:
    """Copy every object under ``prefix`` to ``target/<key>`` (idempotent)."""
    copied = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            dest = target / obj["Key"]
            if dest.exists() and dest.stat().st_size == obj["Size"]:
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())
            copied += 1
    return copied


# ------------------------------------------------------------------------- object stores
class LocalStore:
    """Filesystem stand-in for the lake bucket (the moto mirror)."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def list(self, prefix: str) -> list[tuple[str, int]]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(
            (p.relative_to(self.root).as_posix(), p.stat().st_size)
            for p in base.rglob("*")
            if p.is_file()
        )

    def get(self, key: str) -> bytes:
        return (self.root / key).read_bytes()

    def put(self, key: str, body: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)


class S3Store:
    def __init__(self, s3: Any, bucket: str):
        self.s3 = s3
        self.bucket = bucket

    def list(self, prefix: str) -> list[tuple[str, int]]:
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            out.extend((o["Key"], o["Size"]) for o in page.get("Contents", []))
        return sorted(out)

    def get(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def put(self, key: str, body: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
