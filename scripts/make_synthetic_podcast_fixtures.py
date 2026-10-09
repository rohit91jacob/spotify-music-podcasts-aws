"""Generate the SYNTHETIC podcast fixtures under tests/fixtures/bronze/{apple,rss}/.

Why synthetic: Apple's chart feed and iTunes Lookup responses are "Copyright Apple Inc. All
rights reserved" and podcast RSS feeds belong to their publishers, so real responses cannot be
committed. These files have the exact shape of the real bronze payloads but every show, name,
id and URL is invented ("SYNTHETIC ..." names, ids starting 900000, example.invalid URLs).
Output is deterministic (fixed seed); re-running the script reproduces the files byte for byte.

    uv run python scripts/make_synthetic_podcast_fixtures.py
"""

from __future__ import annotations

import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

SEED = 20261009
ROOT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "bronze"
COUNTRIES = ("us", "gb", "in")
SNAPSHOTS = [date(2026, 10, 3) + timedelta(days=i) for i in range(6)]
CHART_SIZE = 25
N_SHOWS = 40
FEEDS = 10
GENRES = ("News", "Comedy", "True Crime", "Society & Culture", "Business", "Sports", "History")
CADENCES = {"daily": 1, "twice weekly": 3.5, "weekly": 7, "fortnightly": 14, "monthly": 30}


def _write(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    rng = random.Random(SEED)
    shows = [
        {
            "id": str(900000001 + i),
            "name": f"SYNTHETIC Show {i + 1:02d}",
            "artistName": f"SYNTHETIC Publisher {i % 13 + 1:02d}",
            "genre": GENRES[i % len(GENRES)],
            "base": rng.gauss(0, 1),
            "cadence": list(CADENCES)[i % len(CADENCES)],
        }
        for i in range(N_SHOWS)
    ]
    for cc in COUNTRIES:
        bias = {s["id"]: rng.gauss(0, 0.8) for s in shows}
        drift = {s["id"]: 0.0 for s in shows}
        for snap in SNAPSHOTS:
            for s in shows:
                drift[s["id"]] += rng.gauss(0, 0.35)
            ranked = sorted(shows, key=lambda s: -(s["base"] + bias[s["id"]] + drift[s["id"]]))
            results = [
                {
                    "rank": r + 1,
                    "id": s["id"],
                    "name": s["name"],
                    "artistName": s["artistName"],
                    "genres": [s["genre"]],
                    "url": f"https://podcasts.example.invalid/{cc}/podcast/id{s['id']}",
                }
                for r, s in enumerate(ranked[:CHART_SIZE])
            ]
            _write(
                ROOT / "apple" / "charts" / f"dt={snap.isoformat()}" / f"{cc}.json",
                {
                    "_synthetic": True,
                    "country": cc,
                    "updated": f"{snap.isoformat()}T05:00:00+00:00",
                    "fetched_at": f"{snap.isoformat()}T05:10:00+00:00",
                    "results": results,
                },
            )
            if snap == SNAPSHOTS[-1]:
                ids = [r["id"] for r in results]
                lookup = []
                for s in shows:
                    if s["id"] not in ids:
                        continue
                    lookup.append(
                        {
                            "collectionId": int(s["id"]),
                            "collectionName": s["name"],
                            "artistName": s["artistName"],
                            "feedUrl": f"https://feeds.example.invalid/{s['id']}.xml",
                            "primaryGenreName": s["genre"],
                            "genres": [s["genre"], "Podcasts"],
                            "trackCount": None,  # filled from the feed below
                            "releaseDate": None,
                            "country": cc.upper(),
                            "collectionExplicitness": "notExplicit",
                            "contentAdvisoryRating": "Clean",
                        }
                    )
                _write(
                    ROOT / "apple" / "lookup" / f"dt={snap.isoformat()}" / f"{cc}.json",
                    {"_synthetic": True, "country": cc, "results": lookup},
                )

    # Episodes for the shows that top the last US snapshot.
    last = SNAPSHOTS[-1]
    us_chart = json.loads(
        (ROOT / "apple" / "charts" / f"dt={last.isoformat()}" / "us.json").read_text("utf-8")
    )["results"]
    fetched = datetime(last.year, last.month, last.day, 5, 20, tzinfo=UTC)
    feed_stats = {}
    for r in us_chart[:FEEDS]:
        show = next(s for s in shows if s["id"] == r["id"])
        gap = CADENCES[show["cadence"]]
        t = fetched - timedelta(hours=rng.uniform(2, gap * 24))
        items = []
        for n in range(rng.randint(25, 60)):
            kind = "trailer" if n == 0 and rng.random() < 0.3 else "full"
            items.append(
                {
                    "episode_key": f"{int(show['id']):x}{n:04d}".rjust(16, "0"),
                    "published_at": t.isoformat(timespec="seconds"),
                    "duration_seconds": int(rng.uniform(15, 75) * 60),
                    "episode_type": kind,
                    "has_enclosure": True,
                }
            )
            t -= timedelta(hours=max(1.0, rng.gauss(gap * 24, gap * 3)))
        _write(
            ROOT / "rss" / "episodes" / f"dt={last.isoformat()}" / f"{show['id']}.json",
            {
                "_synthetic": True,
                "podcast_id": show["id"],
                "fetched_at": fetched.isoformat(timespec="seconds"),
                "status": "ok",
                "items": items,
            },
        )
        # Apple's count includes episodes older than the feed exposes for some shows.
        extra = rng.choice([0, 0, 0, 5, 120])
        feed_stats[show["id"]] = (len(items) + extra, items[0]["published_at"])
    for cc in COUNTRIES:
        path = ROOT / "apple" / "lookup" / f"dt={last.isoformat()}" / f"{cc}.json"
        payload = json.loads(path.read_text("utf-8"))
        for r in payload["results"]:
            count, latest = feed_stats.get(str(r["collectionId"]), (rng.randint(20, 400), None))
            r["trackCount"] = count
            r["releaseDate"] = (
                latest.replace("+00:00", "Z")
                if latest
                else (fetched - timedelta(days=rng.randint(0, 9))).strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        _write(path, payload)
    print(f"wrote synthetic podcast fixtures under {ROOT}")


if __name__ == "__main__":
    main()
