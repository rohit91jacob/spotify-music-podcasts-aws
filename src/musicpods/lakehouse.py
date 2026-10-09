"""Iceberg catalog access (pyiceberg).

Local: a SQL catalog (SQLite) with the warehouse on the filesystem, written through fsspec
(pyarrow's local filesystem rejects Windows drive paths in file URIs).
LocalStack/CI: the same SQL catalog with the warehouse in the emulated S3 bucket.
A real deployment would use the AWS Glue Data Catalog (``type=glue``); see infra/.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform

from musicpods.config import Settings

log = logging.getLogger(__name__)

RAW = "raw"
NS_PROPERTY = "musicpods.namespace"


def catalog(settings: Settings) -> Catalog:
    if settings.iceberg_catalog == "glue":
        # Deployed jobs: tables registered in the Glue Data Catalog, data in the lake bucket.
        from pyiceberg.catalog.glue import GlueCatalog

        cat: Catalog = GlueCatalog(
            "musicpods",
            **{
                "warehouse": f"s3://{settings.lake_bucket}/iceberg",
                "glue.region": settings.aws_region,
                "s3.region": settings.aws_region,
            },
        )
        _ensure_namespace(cat, settings)
        return cat
    settings.catalog_db.parent.mkdir(parents=True, exist_ok=True)
    props: dict[str, Any] = {"uri": f"sqlite:///{settings.catalog_db.resolve().as_posix()}"}
    if settings.aws_mode == "endpoint":
        props.update(
            {
                "warehouse": f"s3://{settings.lake_bucket}/iceberg",
                "s3.endpoint": settings.aws_endpoint_url,
                "s3.region": settings.aws_region,
                "s3.access-key-id": "test",
                "s3.secret-access-key": "test",
                "s3.path-style-access": "true",
            }
        )
    else:
        settings.iceberg_dir.mkdir(parents=True, exist_ok=True)
        props.update(
            {
                "warehouse": settings.iceberg_dir.resolve().as_uri(),
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            }
        )
    cat = SqlCatalog("musicpods", **props)
    _ensure_namespace(cat, settings)
    return cat


def _ensure_namespace(cat: Catalog, settings: Settings) -> None:
    ns = namespace(settings)
    cat.properties[NS_PROPERTY] = ns  # read back by ns(); the catalog ignores unknown keys
    with contextlib.suppress(NamespaceAlreadyExistsError):
        cat.create_namespace(ns)


def ns(cat: Catalog) -> str:
    return cat.properties.get(NS_PROPERTY, RAW)


def namespace(settings: Settings | None = None) -> str:
    """``raw`` locally; the Glue database name (``musicpods_raw``) with the Glue catalog."""
    if settings is not None and settings.iceberg_catalog == "glue":
        return settings.glue_raw_database
    return RAW


def get_or_create(
    cat: Catalog, name: str, arrow_schema: pa.Schema, partition_by: tuple[str, str] | None = None
) -> Table:
    ident = f"{ns(cat)}.{name}"
    try:
        return cat.load_table(ident)
    except NoSuchTableError:
        pass
    table = cat.create_table(ident, schema=arrow_schema)
    if partition_by is not None:
        column, transform = partition_by
        with table.update_spec() as update:
            update.add_field(
                column,
                DayTransform() if transform == "day" else IdentityTransform(),
                f"{column}_{transform}",
            )
        table = cat.load_table(ident)
    return table


def overwrite(cat: Catalog, name: str, data: pa.Table, partition_by=None) -> Table:
    table = get_or_create(cat, name, data.schema, partition_by)
    table.overwrite(data.cast(table.schema().as_arrow()))
    return table


def read(cat: Catalog, name: str, columns: tuple[str, ...] | None = None) -> pa.Table | None:
    try:
        table = cat.load_table(f"{ns(cat)}.{name}")
    except NoSuchTableError:
        return None
    scan = table.scan(selected_fields=columns) if columns else table.scan()
    return scan.to_arrow()


def list_tables(cat: Catalog) -> list[str]:
    return sorted(ident[-1] for ident in cat.list_tables(ns(cat)))


def expire_snapshots(cat: Catalog, retention_days: int) -> dict[str, int]:
    """Drop snapshots older than the retention window (always keeps the current one)."""
    cutoff_ms = int((datetime.now(UTC) - timedelta(days=retention_days)).timestamp() * 1000)
    out: dict[str, int] = {}
    for name in list_tables(cat):
        table = cat.load_table(f"{ns(cat)}.{name}")
        current = table.current_snapshot()
        old = [
            s.snapshot_id
            for s in table.snapshots()
            if s.timestamp_ms < cutoff_ms
            and (current is None or s.snapshot_id != current.snapshot_id)
        ]
        if old:
            table.maintenance.expire_snapshots().by_ids(old).commit()
        out[name] = len(old)
    return out
