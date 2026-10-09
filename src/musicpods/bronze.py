"""Immutable bronze landing zone.

Layout: ``bronze/<source>/<dataset>/dt=YYYY-MM-DD/<name>.json.gz`` plus an append-only
``bronze/_manifest.jsonl`` with the sha256 of every file. Files are written atomically
(temp file + rename). Writing the same name twice with the same content is a no-op, so
re-running an ingest is safe; different content under an existing name is refused.

Committed test fixtures use the same tree with plain ``.json`` files, so the pipeline can be
pointed at ``tests/fixtures/bronze`` instead of the network (``--from-dir``).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any


class BronzeConflictError(RuntimeError):
    pass


@dataclass(frozen=True)
class BronzeFile:
    path: Path
    source: str
    dataset: str
    dt: date

    @property
    def name(self) -> str:
        return self.path.name.split(".json")[0]


def _canonical(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


class Bronze:
    def __init__(self, root: Path):
        self.root = Path(root)

    # ------------------------------------------------------------------ write
    def path_for(self, source: str, dataset: str, dt: date, name: str) -> Path:
        return self.root / source / dataset / f"dt={dt.isoformat()}" / f"{name}.json.gz"

    def exists(self, source: str, dataset: str, dt: date, name: str) -> bool:
        return self.path_for(source, dataset, dt, name).exists()

    def write(
        self,
        source: str,
        dataset: str,
        dt: date,
        name: str,
        payload: Any,
        on_conflict: str = "keep",
    ) -> Path:
        """Land ``payload``. Same content again is a no-op. Different content under an existing
        name keeps the first version (``on_conflict="keep"``: one snapshot per day wins) or
        raises (``"error"``)."""
        body = _canonical(payload)
        digest = hashlib.sha256(body).hexdigest()
        target = self.path_for(source, dataset, dt, name)
        if target.exists():
            existing = hashlib.sha256(_read_bytes(target)).hexdigest()
            if existing == digest or on_conflict == "keep":
                return target
            raise BronzeConflictError(f"{target} exists with different content")
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp")
        with gzip.GzipFile(tmp, "wb", mtime=0) as fh:
            fh.write(body)
        tmp.replace(target)
        self._manifest(target, source, dataset, dt, digest, len(body))
        return target

    def _manifest(
        self, target: Path, source: str, dataset: str, dt: date, digest: str, size: int
    ) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        entry = {
            "path": target.relative_to(self.root).as_posix(),
            "source": source,
            "dataset": dataset,
            "dt": dt.isoformat(),
            "sha256": digest,
            "bytes": size,
            "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        with (self.root / "_manifest.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")

    # ------------------------------------------------------------------ read
    def files(self, source: str, dataset: str) -> list[BronzeFile]:
        base = self.root / source / dataset
        if not base.exists():
            return []
        out = []
        for path in sorted(base.glob("dt=*/*.json*")):
            if path.suffix == ".tmp":
                continue
            dt = date.fromisoformat(path.parent.name.removeprefix("dt="))
            out.append(BronzeFile(path, source, dataset, dt))
        return out

    def read(self, f: BronzeFile | Path) -> Any:
        path = f.path if isinstance(f, BronzeFile) else f
        return json.loads(_read_bytes(path))

    def iter_payloads(self, source: str, dataset: str) -> Iterator[tuple[BronzeFile, Any]]:
        for f in self.files(source, dataset):
            yield f, self.read(f)

    def latest(self, source: str, dataset: str) -> list[BronzeFile]:
        """Files from the most recent ``dt`` partition only."""
        files = self.files(source, dataset)
        if not files:
            return []
        last = max(f.dt for f in files)
        return [f for f in files if f.dt == last]

    def import_tree(self, src: Path, sources: tuple[str, ...] | None = None) -> int:
        """Copy a fixture tree (same layout) into this bronze root; existing files are kept."""
        copied = 0
        for path in sorted(Path(src).rglob("*.json*")):
            rel = path.relative_to(src)
            if len(rel.parts) != 4 or not rel.parts[2].startswith("dt="):
                continue
            source, dataset, part, _ = rel.parts
            if sources is not None and source not in sources:
                continue
            name = path.name.split(".json")[0]
            payload = json.loads(_read_bytes(path))
            self.write(source, dataset, date.fromisoformat(part[3:]), name, payload)
            copied += 1
        return copied

    def wipe(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)


def _read_bytes(path: Path) -> bytes:
    raw = Path(path).read_bytes()
    if raw[:2] == b"\x1f\x8b":
        return gzip.decompress(raw)
    return raw
