"""ListenBrainz (CC0): sitewide stats, a stable panel of public users, their listens, and fresh
releases.

Privacy: ListenBrainz listens are public, but user names never leave this module. Every
payload written to bronze carries ``user_key`` (a salted SHA-256 pseudonym) instead of
``user_name``. The salt lives in the data directory (or ``MUSICPODS_PSEUDONYM_SALT``) and is
never committed.

Panel selection (deterministic, documented in the README):
1. take the sitewide top artists of the last week and keep every third one (spreads the seed
   beyond the head of the chart);
2. collect each seed artist's top listeners (at most ``PER_ARTIST`` per artist);
3. order candidates by the hash of their name and keep the first ``lb_users`` whose published
   listening activity is plausible for one human (at most ``lb_max_daily_listens`` per day and
   at least one listen in the last two weeks). Accounts that scrobble 1,000+ tracks a day are
   streaming farms, not listeners.
The panel is persisted in ``ops/lb_panel.json`` so week-over-week metrics compare the same
people; members that become inactive or implausible are replaced.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from musicpods.bronze import Bronze
from musicpods.config import Settings
from musicpods.http import HttpClient, HttpError

log = logging.getLogger(__name__)

API = "https://api.listenbrainz.org/1"
SOURCE = "listenbrainz"
PER_ARTIST = 3
PAGE_SIZE = 500


# --------------------------------------------------------------------------- pseudonyms
def load_salt(settings: Settings) -> str:
    if settings.pseudonym_salt:
        return settings.pseudonym_salt
    path = settings.ops_dir / "pseudonym_salt"
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    path.parent.mkdir(parents=True, exist_ok=True)
    salt = secrets.token_hex(16)
    path.write_text(salt, encoding="utf-8")
    return salt


def user_key(user_name: str, salt: str) -> str:
    return "u_" + hashlib.sha256(f"{salt}:{user_name}".encode()).hexdigest()[:12]


# Only the client fields the pipeline uses are kept (data minimisation: no origin URLs, player
# names or third-party ids are landed).
ADDITIONAL_INFO_KEYS = frozenset(
    {
        "duration_ms",
        "duration",
        "ms_played",
        "submission_client",
        "music_service",
        "music_service_name",
        "artist_mbids",
        "recording_mbid",
        "release_mbid",
        "release_group_mbid",
    }
)
MBID_MAPPING_KEYS = frozenset(
    {"recording_mbid", "release_mbid", "release_group_mbid", "artist_mbids", "recording_name"}
)


def scrub_listen(listen: dict[str, Any], key: str) -> dict[str, Any]:
    """Replace the user name with the pseudonym and keep only the metadata used downstream."""
    out = {k: v for k, v in listen.items() if k not in {"user_name", "user_id"}}
    out["user_key"] = key
    tm = dict(out.get("track_metadata") or {})
    info = tm.get("additional_info")
    if isinstance(info, dict):
        tm["additional_info"] = {k: v for k, v in info.items() if k in ADDITIONAL_INFO_KEYS}
    mapping = tm.get("mbid_mapping")
    if isinstance(mapping, dict):
        tm["mbid_mapping"] = {k: v for k, v in mapping.items() if k in MBID_MAPPING_KEYS}
    out["track_metadata"] = tm
    return out


# --------------------------------------------------------------------------- panel
@dataclass
class PanelMember:
    user_name: str
    user_key: str
    seed_artist_mbid: str


def _activity_is_plausible(payload: dict[str, Any] | None, max_daily: int) -> bool:
    if not payload:
        return False
    days = payload.get("listening_activity") or []
    counts = [int(d.get("listen_count") or 0) for d in days]
    return bool(counts) and sum(counts) > 0 and max(counts) <= max_daily


class ListenBrainzIngest:
    def __init__(self, settings: Settings, client: HttpClient, bronze: Bronze, today: date):
        self.s = settings
        self.http = client
        self.bronze = bronze
        self.today = today
        self.salt = load_salt(settings)
        self.panel_path = settings.ops_dir / "lb_panel.json"
        self.state_path = settings.ops_dir / "lb_listen_state.json"
        self._names: dict[str, str] = {}

    # ---------------------------------------------------------------- api helpers
    def _get(self, path: str, **params: Any) -> dict[str, Any] | None:
        try:
            body = self.http.get_json(f"{API}{path}", params=params or None, allow_404=True)
        except HttpError as exc:
            # User names are in the URL; errors end up in (public CI) logs, so redact them.
            raise HttpError(self.redact(exc.url), exc.status, self.redact(exc.detail)) from None
        return None if body is None else body.get("payload", body)

    def redact(self, text: str) -> str:
        """Replace every user name seen by this ingest with its pseudonym."""
        for name in sorted(self._names, key=len, reverse=True):
            text = text.replace(name, self._names[name])
        return text

    def _known(self, name: str) -> str:
        self._names.setdefault(name, user_key(name, self.salt))
        return name

    # ---------------------------------------------------------------- steps
    def sitewide_artists(self) -> list[dict[str, Any]]:
        payload = self._get("/stats/sitewide/artists", range="week", count=100)
        if not payload:
            raise RuntimeError("ListenBrainz returned no sitewide artist stats")
        self.bronze.write(SOURCE, "sitewide_artists", self.today, "week", payload)
        return payload["artists"]

    def _load_panel(self) -> list[PanelMember]:
        if not self.panel_path.exists():
            return []
        return [PanelMember(**m) for m in json.loads(self.panel_path.read_text("utf-8"))]

    def _save_panel(self, panel: list[PanelMember]) -> None:
        self.panel_path.parent.mkdir(parents=True, exist_ok=True)
        self.panel_path.write_text(
            json.dumps([m.__dict__ for m in panel], indent=1), encoding="utf-8"
        )

    def build_panel(self, artists: list[dict[str, Any]]) -> tuple[list[PanelMember], dict]:
        """Keep plausible members, top the panel up from the seed artists' listeners."""
        panel = self._load_panel()
        activity: dict[str, dict[str, Any] | None] = {}
        kept: list[PanelMember] = []
        for m in panel:
            self._known(m.user_name)
            activity[m.user_name] = self._get(
                f"/stats/user/{m.user_name}/listening-activity", range="week"
            )
            if _activity_is_plausible(activity[m.user_name], self.s.lb_max_daily_listens):
                kept.append(m)
            else:
                log.info("panel member %s dropped (inactive or implausible)", m.user_key)
        rejected = 0
        if len(kept) < self.s.lb_users:
            seeds = []
            seen: set[str] = set()
            for a in artists:
                mbid = a.get("artist_mbid")
                if mbid and mbid not in seen:
                    seen.add(mbid)
                    seeds.append(mbid)
            seeds = seeds[::3][: self.s.lb_seed_artists]
            candidates: dict[str, str] = {}
            for mbid in seeds:
                payload = self._get(f"/stats/artist/{mbid}/listeners", range="week")
                for listener in (payload or {}).get("listeners", [])[: PER_ARTIST * 3]:
                    candidates.setdefault(self._known(listener["user_name"]), mbid)
            members = {m.user_name for m in kept}
            order = sorted(candidates, key=lambda u: hashlib.sha256(u.encode()).hexdigest())
            per_seed: dict[str, int] = {}
            for name in order:
                if len(kept) >= self.s.lb_users:
                    break
                seed = candidates[name]
                if name in members or per_seed.get(seed, 0) >= PER_ARTIST:
                    continue
                act = self._get(f"/stats/user/{name}/listening-activity", range="week")
                if not _activity_is_plausible(act, self.s.lb_max_daily_listens):
                    rejected += 1
                    continue
                activity[name] = act
                kept.append(PanelMember(name, user_key(name, self.salt), seed))
                members.add(name)
                per_seed[seed] = per_seed.get(seed, 0) + 1
        self._save_panel(kept)
        for m in kept:
            act = activity.get(m.user_name)
            if act:
                act = {k: v for k, v in act.items() if k not in {"user_id", "user_name"}}
                act["user_key"] = m.user_key
                self.bronze.write(SOURCE, "user_activity", self.today, m.user_key, act)
        return kept, {"panel_size": len(kept), "rejected_implausible": rejected}

    def user_top_artists(self, panel: list[PanelMember]) -> None:
        for m in panel:
            self._known(m.user_name)
            payload = self._get(f"/stats/user/{m.user_name}/artists", range="week", count=10)
            if payload:
                payload = {k: v for k, v in payload.items() if k not in {"user_id", "user_name"}}
                payload["user_key"] = m.user_key
                self.bronze.write(SOURCE, "user_top_artists", self.today, m.user_key, payload)

    def listens(
        self, panel: list[PanelMember], now_ts: int | None = None, backfill: bool = False
    ) -> int:
        """Fetch listens newer than what we already hold (first time: the last N days).

        ``backfill=True`` ignores the saved cursors and re-fetches the whole window. Pages are
        content-addressed and the Firehose sink deduplicates on event_id, so this is safe."""
        state = json.loads(self.state_path.read_text("utf-8")) if self.state_path.exists() else {}
        if backfill:
            state = dict.fromkeys(state, 0)
        now_ts = now_ts or int(time.time())
        window_floor = now_ts - self.s.lb_window_days * 86400
        total = 0
        for m in panel:
            self._known(m.user_name)
            floor = max(int(state.get(m.user_key, 0)), window_floor)
            try:
                collected = self._user_listens(m.user_name, floor, now_ts)
            except HttpError as exc:
                # One slow or failing user must not sink the run; the next run resumes from
                # the saved state for this user.
                log.warning("listens for %s failed, skipped this run: %s", m.user_key, exc)
                continue
            if collected:
                scrubbed = [scrub_listen(p, m.user_key) for p in collected]
                newest = max(int(p["listened_at"]) for p in collected)
                payload = {"user_key": m.user_key, "listens": scrubbed}
                digest = hashlib.sha256(
                    json.dumps(payload, sort_keys=True).encode("utf-8")
                ).hexdigest()[:8]
                self.bronze.write(
                    SOURCE, "listens", self.today, f"{m.user_key}-{newest}-{digest}", payload
                )
                state[m.user_key] = max(newest, int(state.get(m.user_key, 0)))
                total += len(collected)
                self.state_path.parent.mkdir(parents=True, exist_ok=True)
                self.state_path.write_text(
                    json.dumps(state, indent=1, sort_keys=True), encoding="utf-8"
                )
        return total

    def _user_listens(self, user_name: str, floor: int, now_ts: int) -> list[dict[str, Any]]:
        """Page backwards from now until ``floor`` (exclusive) or the per-user cap.

        ``max_ts`` is exclusive, and many listens share a timestamp (imports, some scrobblers),
        so each next page starts one second *above* the oldest listen seen and the overlap is
        dropped by (listened_at, recording_msid). Continuing from the oldest timestamp itself
        would silently skip the other listens of that second.
        """
        collected: list[dict[str, Any]] = []
        seen: set[tuple[int, str]] = set()
        max_ts = now_ts + 1
        while len(collected) < self.s.lb_max_listens_per_user:
            payload = self._get(f"/user/{user_name}/listens", max_ts=max_ts, count=PAGE_SIZE)
            page = (payload or {}).get("listens") or []
            for p in page:
                key = (int(p["listened_at"]), str(p.get("recording_msid")))
                if key[0] > floor and key not in seen:
                    seen.add(key)
                    collected.append(p)
            if not page or len(page) < PAGE_SIZE // 2:
                break
            oldest = min(int(p["listened_at"]) for p in page)
            if oldest <= floor:
                break
            # A whole page inside one second cannot move the window; step past it.
            max_ts = oldest + 1 if oldest + 1 < max_ts else oldest
        return collected[: self.s.lb_max_listens_per_user]

    def fresh_releases(self) -> int:
        payload = self._get(
            "/explore/fresh-releases/",
            days=self.s.lb_fresh_release_days,
            past="true",
            future="false",
        )
        releases = (payload or {}).get("releases") or []
        slim = [
            {
                k: r.get(k)
                for k in (
                    "artist_credit_name",
                    "artist_mbids",
                    "release_date",
                    "release_group_mbid",
                    "release_group_primary_type",
                    "release_mbid",
                    "release_name",
                    "listen_count",
                )
            }
            for r in releases
        ]
        self.bronze.write(
            SOURCE,
            "fresh_releases",
            self.today,
            f"past-{self.s.lb_fresh_release_days}d",
            {"days": self.s.lb_fresh_release_days, "releases": slim},
        )
        return len(slim)

    def run(self, backfill: bool = False) -> dict[str, Any]:
        artists = self.sitewide_artists()
        panel, stats = self.build_panel(artists)
        self.user_top_artists(panel)
        stats["listens_fetched"] = self.listens(panel, backfill=backfill)
        stats["fresh_releases"] = self.fresh_releases()
        return stats


def panel_size(ops_dir: Path) -> int:
    path = ops_dir / "lb_panel.json"
    return len(json.loads(path.read_text("utf-8"))) if path.exists() else 0
