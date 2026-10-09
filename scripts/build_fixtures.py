"""Build the committed REAL fixtures (ListenBrainz + MusicBrainz) from a live bronze tree.

    uv run python scripts/build_fixtures.py <data-dir-of-a-live-run>

* ListenBrainz data is CC0; MusicBrainz core data is CC0 and its tags (genres) are
  CC BY-NC-SA 3.0 - see DATA_LICENSE.md. Apple/RSS data is never copied (synthetic fixtures
  come from make_synthetic_podcast_fixtures.py).
* Users are already pseudonymised in bronze (salted hash, salt not committed).
* Picks USERS panel members (deterministically, by user_key) whose listen count fits the
  per-file size budget - preferring one whose client reports real playback time (``ms_played``)
  so the observed-skip path is covered - keeps all their listens (whole window, so the
  reconciliation against the published daily counts still works) and only the MusicBrainz
  entities they reference. Listens are re-scrubbed with the ingest allowlist.
* Every written file must stay under 300 KB (pre-commit check-added-large-files).
"""

from __future__ import annotations

import gzip
import json
import shutil
import sys
from collections import defaultdict
from pathlib import Path

from musicpods.bronze import Bronze
from musicpods.sources.listenbrainz import scrub_listen

USERS = 6
MAX_LISTENS = 2200
MIN_LISTENS = 150
LIMIT_BYTES = 290_000
OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "bronze"


def _dump(path: Path, payload, gz: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if gz:
        path = path.with_suffix(".json.gz")
        with gzip.GzipFile(path, "wb", mtime=0) as fh:
            fh.write(body.encode("utf-8"))
    else:
        path.write_text(body + "\n", encoding="utf-8")
    size = path.stat().st_size
    if size > LIMIT_BYTES:
        raise SystemExit(f"{path} is {size} bytes (> {LIMIT_BYTES})")


def _info(listen: dict) -> dict:
    return (listen.get("track_metadata") or {}).get("additional_info") or {}


def main(live: Path) -> None:
    src = Bronze(live / "bronze")
    by_user: dict[str, list] = defaultdict(list)
    dt_of: dict[str, str] = {}
    seen: set[tuple[str, int, str]] = set()
    for f, payload in src.iter_payloads("listenbrainz", "listens"):
        u = payload["user_key"]
        for li in payload["listens"]:
            key = (u, int(li["listened_at"]), str(li.get("recording_msid")))
            if key not in seen:  # pages overlap after a backfill
                seen.add(key)
                by_user[u].append(li)
        dt_of[u] = f.dt.isoformat()
    fits = [u for u in sorted(by_user) if MIN_LISTENS <= len(by_user[u]) <= MAX_LISTENS]
    played = [u for u in fits if any("ms_played" in _info(li) for li in by_user[u])]
    chosen = played[:1] + [u for u in fits if u not in played[:1]][: USERS - len(played[:1])]
    if not chosen:
        raise SystemExit("no user fits the fixture budget")
    for sub in ("listenbrainz", "musicbrainz"):
        shutil.rmtree(OUT / sub, ignore_errors=True)

    artists, recordings, groups = set(), set(), set()
    for u in chosen:
        listens = sorted(
            (scrub_listen(li, u) for li in by_user[u]), key=lambda x: -int(x["listened_at"])
        )
        for li in listens:
            m = (li.get("track_metadata") or {}).get("mbid_mapping") or {}
            artists.update(m.get("artist_mbids") or [])
            if m.get("recording_mbid"):
                recordings.add(m["recording_mbid"])
            if m.get("release_group_mbid"):
                groups.add(m["release_group_mbid"])
        newest = int(listens[0]["listened_at"])
        _dump(
            OUT / "listenbrainz" / "listens" / f"dt={dt_of[u]}" / f"{u}-{newest}.json",
            {"user_key": u, "listens": listens},
            gz=True,
        )
    for dataset in ("user_activity", "user_top_artists"):
        for f, payload in src.iter_payloads("listenbrainz", dataset):
            if payload.get("user_key") in chosen:
                _dump(OUT / "listenbrainz" / dataset / f"dt={f.dt}" / f"{f.name}.json", payload)
    for f, payload in src.iter_payloads("listenbrainz", "sitewide_artists"):
        _dump(OUT / "listenbrainz" / "sitewide_artists" / f"dt={f.dt}" / f"{f.name}.json", payload)
    for f, payload in src.iter_payloads("listenbrainz", "fresh_releases"):
        rel = payload["releases"]
        # Releases the fixture users played, plus the most-played fresh releases whose release
        # group MusicBrainz returned (so the release-date reconciliation has rows).
        looked_up = {
            i["id"]
            for _, items in src.iter_payloads("musicbrainz", "release_groups")
            for i in items
        }
        top = sorted(
            (r for r in rel if r.get("release_group_mbid") in looked_up),
            key=lambda r: (-(r.get("listen_count") or 0), r["release_mbid"]),
        )[:100]
        keep = [r for r in rel if r.get("release_group_mbid") in groups] + top
        seen, uniq = set(), []
        for r in keep:
            if r["release_mbid"] not in seen:
                seen.add(r["release_mbid"])
                uniq.append(r)
        groups.update(r["release_group_mbid"] for r in uniq if r.get("release_group_mbid"))
        _dump(
            OUT / "listenbrainz" / "fresh_releases" / f"dt={f.dt}" / f"{f.name}.json",
            {**payload, "releases": uniq},
        )
    wanted = {"artists": artists, "recordings": recordings, "release_groups": groups}
    for dataset, ids in wanted.items():
        items_by_dt: dict[str, list] = defaultdict(list)
        for f, items in src.iter_payloads("musicbrainz", dataset):
            items_by_dt[f.dt.isoformat()].extend(i for i in items if i["id"] in ids)
        for dt, items in items_by_dt.items():
            items.sort(key=lambda i: i["id"])
            for n in range(0, len(items), 700):
                _dump(
                    OUT / "musicbrainz" / dataset / f"dt={dt}" / f"part-{n // 700:03d}.json",
                    items[n : n + 700],
                )
    for f, payload in src.iter_payloads("musicbrainz", "genres"):
        _dump(OUT / "musicbrainz" / "genres" / f"dt={f.dt}" / f"{f.name}.json", payload)
    print(
        f"fixtures: {len(chosen)} users, {sum(len(by_user[u]) for u in chosen)} listens, "
        f"{len(artists)} artists, {len(recordings)} recordings, {len(groups)} release groups"
    )


if __name__ == "__main__":
    main(Path(sys.argv[1]).expanduser())
