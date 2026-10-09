"""Static results site (``musicpods report --out site``).

One HTML page plus a stylesheet and ``summary.json``. No JavaScript and no external assets, so
it can be served from GitHub Pages or opened from disk. Every number comes from the DuckDB
marts of the last pipeline run. Tables are the primary view; single-series inline bars (one
hue, value printed beside them, native hover title) add a visual cue only.
"""

from __future__ import annotations

import html
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from musicpods.config import Settings
from musicpods.warehouse import connect

REPO_URL = "https://github.com/rohit91jacob/spotify-music-podcasts-aws"
Row = dict[str, Any]

CSS = """
:root { color-scheme: light dark; }
body { margin: 0; font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  --surface-1: #fcfcfb; --surface-2: #f1f0ec; --text-primary: #0b0b0b; --text-secondary: #52514e;
  --border: #dcdad3; --series-1: #2a78d6; --link: #1f5fae;
  background: var(--surface-1); color: var(--text-primary); }
@media (prefers-color-scheme: dark) {
  body { --surface-1: #1a1a19; --surface-2: #252523; --text-primary: #ffffff;
    --text-secondary: #c3c2b7; --border: #3a3936; --series-1: #3987e5; --link: #8ab8f0; }
}
main { max-width: 1100px; margin: 0 auto; padding: 1.5rem 1rem 4rem; }
h1 { font-size: 1.7rem; margin: 0 0 .25rem; } h2 { margin-top: 2.5rem; font-size: 1.3rem; }
h3 { font-size: 1.05rem; margin: 1.5rem 0 .5rem; }
a { color: var(--link); } .muted, caption, .note { color: var(--text-secondary); }
.note { font-size: .9rem; } .banner { background: var(--surface-2); border: 1px solid var(--border);
  border-radius: 6px; padding: .75rem 1rem; font-size: .92rem; }
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .75rem;
  margin: 1.25rem 0; }
.kpi { border: 1px solid var(--border); border-radius: 6px; padding: .6rem .8rem; }
.kpi b { display: block; font-size: 1.35rem; font-variant-numeric: tabular-nums; }
.kpi span { color: var(--text-secondary); font-size: .85rem; }
.grid2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 1.5rem; }
.table-wrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: .9rem; }
caption { text-align: left; padding: .25rem 0; font-size: .85rem; }
th, td { padding: .3rem .5rem; border-bottom: 1px solid var(--border); text-align: left;
  vertical-align: top; }
th { font-weight: 600; color: var(--text-secondary); }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
.bar { display: inline-block; height: .7rem; background: var(--series-1);
  border-radius: 0 4px 4px 0; vertical-align: middle; margin-right: .4rem; min-width: 2px; }
.empty { color: var(--text-secondary); font-style: italic; }
footer { margin-top: 3rem; font-size: .85rem; color: var(--text-secondary); }
"""


# --------------------------------------------------------------------------- formatting
def _t(v: object) -> str:
    return "&ndash;" if v is None else html.escape(str(v))


def _int(v: object) -> str:
    return "&ndash;" if v is None else f"{int(v):,}"


def _pct(v: object, places: int = 1) -> str:
    return "&ndash;" if v is None else f"{100 * float(v):.{places}f}%"


def _num(v: object, places: int = 1) -> str:
    return "&ndash;" if v is None else f"{float(v):,.{places}f}"


def _signed(v: object) -> str:
    return "&ndash;" if v is None else f"{int(v):+,}"


@dataclass(frozen=True)
class Col:
    header: str
    render: Callable[[Row], str]
    numeric: bool = False


def table(caption: str, cols: Sequence[Col], rows: list[Row], empty: str = "No rows yet.") -> str:
    if not rows:
        return f'<p class="empty">{html.escape(empty)}</p>'
    head = "".join(
        f'<th scope="col"{" class=num" if c.numeric else ""}>{c.header}</th>' for c in cols
    )
    body = "".join(
        "<tr>"
        + "".join(f"<td{' class=num' if c.numeric else ''}>{c.render(r)}</td>" for c in cols)
        + "</tr>"
        for r in rows
    )
    return (
        f'<div class="table-wrap"><table><caption>{html.escape(caption)}</caption>'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def bar(value: float | None, maximum: float, label: str) -> str:
    if value is None or not maximum:
        return _t(None)
    width = max(2.0, 140.0 * float(value) / float(maximum))
    title = html.escape(label, quote=True)
    return f'<span class="bar" style="width:{width:.0f}px" title="{title}"></span>'


# --------------------------------------------------------------------------- data access
class Data:
    def __init__(self, con):
        self.con = con

    def rows(self, sql: str) -> list[Row]:
        try:
            cur = self.con.execute(sql)
        except Exception:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    def one(self, sql: str) -> Row:
        r = self.rows(sql)
        return r[0] if r else {}


def collect(d: Data) -> dict[str, Any]:
    out: dict[str, Any] = {}
    out["overview"] = d.one(
        """
        SELECT count(*) AS listens, count(DISTINCT user_key) AS users,
               count(DISTINCT artist_key) AS artists, count(DISTINCT session_id) AS sessions,
               min(listen_date) AS first_day, max(listen_date) AS last_day,
               avg(CASE WHEN is_mbid_mapped THEN 1.0 ELSE 0.0 END) AS mbid_mapped_share,
               sum(CASE WHEN is_short_play THEN 1 ELSE 0 END) AS short_plays,
               sum(CASE WHEN is_short_play_derivable THEN 1 ELSE 0 END) AS derivable,
               sum(CASE WHEN short_play_basis = 'played_ms' THEN 1 ELSE 0 END) AS derivable_played,
               sum(CASE WHEN short_play_basis = 'next_listen' THEN 1 ELSE 0 END) AS derivable_next
        FROM marts.fct_listens
        """
    )
    out["duplicates"] = d.one(
        "SELECT sum(CASE WHEN is_duplicate_scrobble THEN 1 ELSE 0 END) AS n, count(*) AS total "
        "FROM intermediate.int_listens__enriched"
    )
    week = d.one(
        "SELECT max(week_start) AS w FROM marts.mart_artist_momentum WHERE is_comparable_week"
    ).get("w")
    out["momentum_week"] = week
    if week:
        base = (
            "SELECT artist_name, primary_genre, listens, prev_week_listens, wow_change, wow_pct, "
            f"listeners FROM marts.mart_artist_momentum WHERE week_start = DATE '{week}' "
        )
        out["risers"] = d.rows(
            base + "AND wow_change > 0 ORDER BY wow_change DESC, listens DESC LIMIT 15"
        )
        out["fallers"] = d.rows(
            base + "AND wow_change < 0 ORDER BY wow_change, listens DESC LIMIT 10"
        )
    gweek = d.one(
        "SELECT max(week_start) AS w FROM marts.mart_artist_momentum WHERE is_complete_week"
    ).get("w")
    out["genre_week"] = gweek
    if gweek:
        out["genres"] = d.rows(
            "SELECT genre, listens, listen_share, listeners, artists FROM marts.mart_genre_share "
            f"WHERE week_start = DATE '{gweek}' ORDER BY genre_rank LIMIT 15"
        )
        out["release_age"] = d.rows(
            "SELECT release_age_bucket, listens, listen_share FROM marts.mart_new_release_share "
            f"WHERE week_start = DATE '{gweek}' ORDER BY listens DESC"
        )
        out["short_play_artists"] = d.rows(
            "SELECT artist_name, derivable_listens, short_plays, short_play_rate "
            f"FROM marts.mart_short_plays_weekly WHERE week_start = DATE '{gweek}' "
            "ORDER BY short_play_rate DESC, derivable_listens DESC LIMIT 10"
        )
    out["sessions_daily"] = d.rows(
        "SELECT * FROM marts.mart_listening_sessions_daily ORDER BY session_date DESC LIMIT 14"
    )
    out["session_overall"] = d.one(
        "SELECT count(*) AS sessions, median(duration_minutes) AS median_minutes, "
        "avg(listens) AS avg_listens, quantile_cont(duration_minutes, 0.9) AS p90_minutes "
        "FROM marts.fct_listening_sessions"
    )
    out["discovery"] = d.rows(
        "SELECT release_name, artist_credit_name, primary_type, release_date, discovery_lag_days, "
        "panel_listeners, panel_listens, listens_first_7_days FROM marts.mart_release_discovery_lag "
        "ORDER BY panel_listeners DESC, panel_listens DESC LIMIT 15"
    )
    out["discovery_summary"] = d.one(
        "SELECT count(*) AS releases, median(discovery_lag_days) AS median_lag, "
        "avg(CASE WHEN discovery_lag_days <= 7 THEN 1.0 ELSE 0.0 END) AS within_week "
        "FROM marts.mart_release_discovery_lag WHERE discovery_lag_days IS NOT NULL"
    )
    # podcasts
    out["chart_meta"] = d.one(
        "SELECT count(DISTINCT country) AS countries, count(DISTINCT snapshot_date) AS snapshots, "
        "min(snapshot_date) AS first_snapshot, max(snapshot_date) AS last_snapshot, "
        "count(DISTINCT podcast_id) AS shows FROM marts.mart_podcast_chart_movement"
    )
    last = out["chart_meta"].get("last_snapshot")
    if last:
        out["us_chart"] = d.rows(
            "SELECT m.chart_rank, m.podcast_name, m.chart_genre, m.previous_rank, m.rank_change, "
            "m.movement, l.current_streak, l.snapshots_on_chart "
            "FROM marts.mart_podcast_chart_movement m JOIN marts.mart_podcast_chart_longevity l "
            "ON m.country = l.country AND m.podcast_id = l.podcast_id "
            f"WHERE m.country = 'us' AND m.snapshot_date = DATE '{last}' ORDER BY m.chart_rank LIMIT 20"
        )
        out["movers"] = d.rows(
            "SELECT country, podcast_name, previous_rank, chart_rank, rank_change, movement "
            f"FROM marts.mart_podcast_chart_movement WHERE snapshot_date = DATE '{last}' "
            "AND movement IN ('up','down') ORDER BY abs(rank_change) DESC, country LIMIT 12"
        )
    out["longevity"] = d.rows(
        "SELECT country, podcast_name, current_streak, snapshots_on_chart, snapshots_in_country, "
        "best_rank, latest_rank FROM marts.mart_podcast_chart_longevity "
        "WHERE on_latest_chart ORDER BY current_streak DESC, best_rank, country LIMIT 15"
    )
    out["cross_country"] = d.rows(
        "SELECT podcast_name, countries, best_rank, avg_rank, country_list "
        "FROM marts.mart_podcast_cross_country ORDER BY countries DESC, avg_rank LIMIT 15"
    )
    out["cadence"] = d.rows(
        "SELECT podcast_name, primary_genre, cadence, median_gap_days, episodes_last_30_days, "
        "days_since_latest_episode, avg_duration_minutes FROM marts.mart_episode_cadence "
        "ORDER BY median_gap_days NULLS LAST, podcast_name LIMIT 30"
    )
    out["cadence_mix"] = d.rows(
        "SELECT cadence, count(*) AS shows FROM marts.mart_episode_cadence GROUP BY cadence "
        "ORDER BY shows DESC"
    )
    out["synthetic_podcasts"] = bool(
        d.one(
            "SELECT count(*) AS n FROM marts.dim_podcast WHERE podcast_name LIKE 'SYNTHETIC%'"
        ).get("n")
    )
    return out


# --------------------------------------------------------------------------- page
def _kpi(value: str, label: str) -> str:
    return f'<div class="kpi"><b>{value}</b><span>{html.escape(label)}</span></div>'


def render(data: dict[str, Any], verification: dict[str, Any] | None, generated: str) -> str:
    ov = data.get("overview", {})
    so = data.get("session_overall", {})
    cm = data.get("chart_meta", {})
    dup = data.get("duplicates", {})
    parts: list[str] = []
    synthetic = data.get("synthetic_podcasts")
    parts.append(
        f"""<h1>Music &amp; podcast analytics</h1>
<p class="muted">Spotify-style listening and podcast-chart metrics from real public data, built by
an emulated AWS lakehouse (Firehose &rarr; S3 &rarr; Iceberg &rarr; dbt). Generated {html.escape(generated)}.
Listening window {_t(ov.get("first_day"))} to {_t(ov.get("last_day"))} (UTC).</p>
<div class="banner"><b>What this is.</b> Listening data: real public listens of a fixed panel of
{_int(ov.get("users"))} ListenBrainz users (pseudonymised), selected as described in the
<a href="{REPO_URL}#data-sources">README</a>; it is not a sample of Spotify or of all ListenBrainz
users. Podcast charts: Apple Podcasts Top Shows per country storefront, one snapshot per day.
{"<b>The podcast section of this build uses SIMULATED fixture data (synthetic shows from a seeded generator), not Apple&#39;s charts.</b> " if synthetic else ""}
Cloud services are emulated (moto / LocalStack); nothing runs in a real AWS account.</div>"""
    )
    parts.append(
        '<div class="kpis">'
        + _kpi(_int(ov.get("listens")), "listens (deduplicated)")
        + _kpi(_int(ov.get("users")), "panel users")
        + _kpi(_int(ov.get("artists")), "artists heard")
        + _kpi(_int(so.get("sessions")), "listening sessions")
        + _kpi(_num(so.get("median_minutes")), "median session, minutes")
        + _kpi(_pct(ov.get("mbid_mapped_share")), "listens mapped to MusicBrainz")
        + _kpi(_int(cm.get("countries")), "chart countries")
        + _kpi(_int(cm.get("snapshots")), "daily chart snapshots")
        + "</div>"
    )

    # ---- momentum
    parts.append("<h2>Artist momentum (week over week)</h2>")
    wk = data.get("momentum_week")
    if wk:
        risers = data.get("risers", [])
        mx = max([r["wow_change"] for r in risers] or [0])
        parts.append(
            f'<p class="note">Week starting {_t(wk)} versus the week before. Only weeks fully inside '
            "the data window are compared.</p><div class=grid2>"
            + table(
                "Biggest gains in listens",
                [
                    Col("Artist", lambda r: _t(r["artist_name"])),
                    Col("Genre", lambda r: _t(r["primary_genre"])),
                    Col(
                        "Change",
                        lambda r: (
                            bar(r["wow_change"], mx, f"{r['artist_name']}: {r['wow_change']:+}")
                            + _signed(r["wow_change"])
                        ),
                        True,
                    ),
                    Col("Listens", lambda r: _int(r["listens"]), True),
                    Col("Prev.", lambda r: _int(r["prev_week_listens"]), True),
                    Col("Listeners", lambda r: _int(r["listeners"]), True),
                ],
                risers,
            )
            + table(
                "Biggest drops",
                [
                    Col("Artist", lambda r: _t(r["artist_name"])),
                    Col("Change", lambda r: _signed(r["wow_change"]), True),
                    Col("Listens", lambda r: _int(r["listens"]), True),
                    Col("Prev.", lambda r: _int(r["prev_week_listens"]), True),
                ],
                data.get("fallers", []),
            )
            + "</div>"
        )
    else:
        parts.append('<p class="empty">Needs two complete weeks of listens.</p>')

    # ---- genres + release age
    gw = data.get("genre_week")
    parts.append("<h2>Genre share and catalogue age</h2>")
    if gw:
        genres = data.get("genres", [])
        mx = max([g["listen_share"] or 0 for g in genres] or [0])
        ages = data.get("release_age", [])
        amx = max([a["listen_share"] or 0 for a in ages] or [0])
        parts.append(
            f'<p class="note">Week starting {_t(gw)}. Genre = the artist\'s most-voted MusicBrainz '
            "genre tag; <i>unknown</i> = mapped artist without genre tags; <i>unmapped</i> = listen not "
            "matched to a MusicBrainz artist.</p><div class=grid2>"
            + table(
                "Share of panel listens by primary genre",
                [
                    Col("Genre", lambda r: _t(r["genre"])),
                    Col(
                        "Share",
                        lambda r: (
                            bar(
                                r["listen_share"],
                                mx,
                                f"{r['genre']}: {100 * r['listen_share']:.1f}%",
                            )
                            + _pct(r["listen_share"])
                        ),
                        True,
                    ),
                    Col("Listens", lambda r: _int(r["listens"]), True),
                    Col("Listeners", lambda r: _int(r["listeners"]), True),
                ],
                genres,
            )
            + table(
                "Listens by age of the release at listen time",
                [
                    Col("Release age", lambda r: _t(r["release_age_bucket"])),
                    Col(
                        "Share",
                        lambda r: (
                            bar(
                                r["listen_share"],
                                amx,
                                f"{r['release_age_bucket']}: {100 * r['listen_share']:.1f}%",
                            )
                            + _pct(r["listen_share"])
                        ),
                        True,
                    ),
                    Col("Listens", lambda r: _int(r["listens"]), True),
                ],
                ages,
            )
            + "</div>"
        )

    # ---- sessions & short plays
    parts.append("<h2>Listening sessions and skip-like short plays</h2>")
    parts.append(
        f'<p class="note">A session ends after 30 minutes without a new listen. Across all sessions: '
        f"median {_num(so.get('median_minutes'))} min, 90th percentile {_num(so.get('p90_minutes'))} min, "
        f"{_num(so.get('avg_listens'))} listens on average. A <b>skip-like short play</b> is a listen "
        "that played for less than half the track: measured directly where the client reports playback "
        f"time ({_int(ov.get('derivable_played'))} listens, Spotify history imports), otherwise inferred "
        "when the next listen in the same session started before half the track had elapsed "
        f"({_int(ov.get('derivable_next'))} listens; gaps under 30 s are timestamp collisions from batch "
        "imports and are not counted). "
        f"{_int(ov.get('short_plays'))} of {_int(ov.get('derivable'))} derivable listens were short plays "
        f"({_pct((ov.get('short_plays') or 0) / ov['derivable'] if ov.get('derivable') else None)}). "
        "Clients only submit a listen after half the track or 4 minutes by default, so most real skips "
        "never reach ListenBrainz and the inferred ones come mainly from clients with lower thresholds. "
        f"{_int(dup.get('n'))} duplicate scrobbles (same track again within 10 s) were excluded.</p>"
    )
    sd = data.get("sessions_daily", [])
    smx = max([r["sessions"] for r in sd] or [0])
    parts.append(
        "<div class=grid2>"
        + table(
            "Sessions per day (latest 14 days)",
            [
                Col("Day", lambda r: _t(r["session_date"])),
                Col(
                    "Sessions",
                    lambda r: (
                        bar(r["sessions"], smx, f"{r['session_date']}: {r['sessions']}")
                        + _int(r["sessions"])
                    ),
                    True,
                ),
                Col("Users", lambda r: _int(r["active_users"]), True),
                Col("Median min", lambda r: _num(r["median_session_minutes"]), True),
                Col("Listens/session", lambda r: _num(r["avg_listens_per_session"]), True),
            ],
            sd,
        )
        + table(
            "Highest short-play rates (artists with 20+ derivable listens)",
            [
                Col("Artist", lambda r: _t(r["artist_name"])),
                Col("Rate", lambda r: _pct(r["short_play_rate"]), True),
                Col("Short plays", lambda r: _int(r["short_plays"]), True),
                Col("Derivable", lambda r: _int(r["derivable_listens"]), True),
            ],
            data.get("short_play_artists", []),
        )
        + "</div>"
    )

    # ---- discovery
    ds = data.get("discovery_summary", {})
    parts.append("<h2>New-release discovery lag</h2>")
    parts.append(
        '<p class="note">Releases ListenBrainz lists as released in the last 30 days that the panel '
        f"played: {_int(ds.get('releases'))}. Median days from release date to the panel's first play: "
        f"{_num(ds.get('median_lag'))}; first played within a week: {_pct(ds.get('within_week'))}.</p>"
    )
    parts.append(
        table(
            "Fresh releases with the most panel listeners",
            [
                Col("Release", lambda r: _t(r["release_name"])),
                Col("Artist", lambda r: _t(r["artist_credit_name"])),
                Col("Type", lambda r: _t(r["primary_type"])),
                Col("Released", lambda r: _t(r["release_date"])),
                Col("Lag (days)", lambda r: _int(r["discovery_lag_days"]), True),
                Col("Listeners", lambda r: _int(r["panel_listeners"]), True),
                Col("Listens", lambda r: _int(r["panel_listens"]), True),
                Col("First 7 days", lambda r: _int(r["listens_first_7_days"]), True),
            ],
            data.get("discovery", []),
        )
    )

    # ---- podcasts
    parts.append("<h2>Podcast charts</h2>")
    parts.append(
        f'<p class="note">{_int(cm.get("snapshots"))} daily snapshot(s) from {_t(cm.get("first_snapshot"))} '
        f"to {_t(cm.get('last_snapshot'))} across {_int(cm.get('countries'))} storefronts. Movement compares "
        "each snapshot with the previous one for the same country; the first snapshot is the baseline. "
        "Longevity grows by one data point per daily refresh.</p>"
    )
    parts.append(
        table(
            f"Apple Podcasts Top Shows, United States, {cm.get('last_snapshot')}",
            [
                Col("#", lambda r: _int(r["chart_rank"]), True),
                Col("Show", lambda r: _t(r["podcast_name"])),
                Col("Genre", lambda r: _t(r["chart_genre"])),
                Col("Prev.", lambda r: _int(r["previous_rank"]), True),
                Col("Move", lambda r: _t(r["movement"])),
                Col("Streak (days)", lambda r: _int(r["current_streak"]), True),
            ],
            data.get("us_chart", []),
        )
    )
    parts.append(
        "<div class=grid2>"
        + table(
            "Biggest movers in the latest snapshot",
            [
                Col("Country", lambda r: _t(r["country"]).upper()),
                Col("Show", lambda r: _t(r["podcast_name"])),
                Col("From", lambda r: _int(r["previous_rank"]), True),
                Col("To", lambda r: _int(r["chart_rank"]), True),
                Col("Change", lambda r: _signed(r["rank_change"]), True),
            ],
            data.get("movers", []),
            "No movement yet: needs at least two daily snapshots.",
        )
        + table(
            "On the most country charts",
            [
                Col("Show", lambda r: _t(r["podcast_name"])),
                Col("Countries", lambda r: _int(r["countries"]), True),
                Col("Best", lambda r: _int(r["best_rank"]), True),
                Col("Charts", lambda r: _t(r["country_list"]).upper()),
            ],
            data.get("cross_country", []),
        )
        + "</div>"
    )
    parts.append(
        table(
            "Longest current chart streaks",
            [
                Col("Country", lambda r: _t(r["country"]).upper()),
                Col("Show", lambda r: _t(r["podcast_name"])),
                Col("Streak", lambda r: _int(r["current_streak"]), True),
                Col(
                    "On chart / snapshots",
                    lambda r: (
                        f"{_int(r['snapshots_on_chart'])} / {_int(r['snapshots_in_country'])}"
                    ),
                    True,
                ),
                Col("Best", lambda r: _int(r["best_rank"]), True),
                Col("Latest", lambda r: _int(r["latest_rank"]), True),
            ],
            data.get("longevity", []),
        )
    )
    parts.append("<h3>Episode release cadence (from each show's RSS feed)</h3>")
    mix = ", ".join(f"{_t(m['cadence'])}: {_int(m['shows'])}" for m in data.get("cadence_mix", []))
    parts.append(
        f'<p class="note">Median gap between the last 20 full episodes. Mix: {mix or "&ndash;"}.</p>'
    )
    parts.append(
        table(
            "Shows by release cadence",
            [
                Col("Show", lambda r: _t(r["podcast_name"])),
                Col("Genre", lambda r: _t(r["primary_genre"])),
                Col("Cadence", lambda r: _t(r["cadence"])),
                Col("Median gap (days)", lambda r: _num(r["median_gap_days"]), True),
                Col("Last 30 days", lambda r: _int(r["episodes_last_30_days"]), True),
                Col("Days since latest", lambda r: _int(r["days_since_latest_episode"]), True),
                Col("Avg min", lambda r: _num(r["avg_duration_minutes"], 0), True),
            ],
            data.get("cadence", []),
        )
    )

    # ---- verification
    if verification:
        v = verification.get("summary", {})
        ud = v.get("user_daily_listens", {})
        ta = v.get("user_top_artists", {})
        ep = v.get("podcast_episode_counts", {})
        rd = v.get("release_dates", {})
        parts.append("<h2>Verification against published figures</h2>")
        parts.append(
            table(
                "Outputs reconciled with what the sources publish (details in docs/verification.md)",
                [Col("Check", lambda r: r["check"]), Col("Result", lambda r: r["result"])],
                [
                    {
                        "check": "Listens per user per UTC day vs ListenBrainz listening-activity stats (settled days)",
                        "result": f"{_int(ud.get('exact_matches'))} of {_int(ud.get('user_days'))} user-days exact, "
                        f"{_int(ud.get('within_1pct'))} within 1%; {_int(ud.get('our_listens'))} vs "
                        f"{_int(ud.get('published_listens'))} listens. Last 3 days of the published "
                        f"range (after the stats snapshot): {_int(ud.get('tail_exact_matches'))} of "
                        f"{_int(ud.get('tail_user_days'))} exact",
                    },
                    {
                        "check": "Listens of each user's top-10 artists in the stats week vs ListenBrainz top-artist stats",
                        "result": f"{_int(ta.get('exact_matches'))} of {_int(ta.get('user_artist_pairs'))} user-artist pairs exact, "
                        f"{_int(ta.get('within_5pct'))} within 5%, {_int(ta.get('within_20pct'))} within 20%",
                    },
                    {
                        "check": "Episodes in RSS feed vs Apple's published episode count",
                        "result": f"{_int(ep.get('exact_matches'))} of {_int(ep.get('shows'))} shows equal; "
                        f"{_int(ep.get('feed_has_fewer'))} feeds list fewer; latest episode within 1 h for "
                        f"{_int(ep.get('latest_episode_within_1h'))}",
                    },
                    {
                        "check": "ListenBrainz fresh-release dates vs MusicBrainz release-group dates",
                        "result": f"{_int(rd.get('exact_matches'))} of {_int(rd.get('releases'))} equal; "
                        f"{_int(rd.get('group_released_earlier'))} groups released earlier",
                    },
                ],
            )
        )

    parts.append(
        f"""<footer><p>Listening data: <a href="https://listenbrainz.org/">ListenBrainz</a> (CC0).
Artist, recording and release metadata: <a href="https://musicbrainz.org/">MusicBrainz</a> (core data CC0);
genre tags are MusicBrainz supplementary data under
<a href="https://creativecommons.org/licenses/by-nc-sa/3.0/">CC BY-NC-SA 3.0</a>, so the genre tables
on this page are shared under the same licence. Podcast charts and metadata: Apple Podcasts via
Apple's RSS generator and the iTunes Search API (&copy; Apple; shown for reference with links back,
not redistributed in the repository). Episode dates: each show's public RSS feed.
Not affiliated with Spotify, Apple, ListenBrainz or MusicBrainz.
Source code (MIT): <a href="{REPO_URL}">{REPO_URL}</a>.</p></footer>"""
    )
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        '<meta name=viewport content="width=device-width,initial-scale=1">'
        "<title>Music &amp; podcast analytics</title><link rel=stylesheet href=style.css></head>"
        "<body><main>" + "\n".join(parts) + "</main></body></html>\n"
    )


def build_site(settings: Settings, out: Path) -> dict[str, Any]:
    con = connect(settings, read_only=True)
    try:
        data = collect(Data(con))
    finally:
        con.close()
    vpath = settings.ops_dir / "verification.json"
    verification = json.loads(vpath.read_text("utf-8")) if vpath.exists() else None
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    out.mkdir(parents=True, exist_ok=True)
    (out / "index.html").write_text(render(data, verification, generated), encoding="utf-8")
    (out / "style.css").write_text(CSS.lstrip(), encoding="utf-8")
    (out / ".nojekyll").write_text("", encoding="utf-8")
    ov = data.get("overview", {})
    summary = {
        "generated_at": generated,
        "listening_window": [ov.get("first_day"), ov.get("last_day")],
        "listens": ov.get("listens"),
        "panel_users": ov.get("users"),
        "artists": ov.get("artists"),
        "sessions": data.get("session_overall", {}).get("sessions"),
        "momentum_week": data.get("momentum_week"),
        "chart_countries": data.get("chart_meta", {}).get("countries"),
        "chart_snapshots": data.get("chart_meta", {}).get("snapshots"),
        "last_chart_snapshot": data.get("chart_meta", {}).get("last_snapshot"),
        "podcasts_synthetic": data.get("synthetic_podcasts"),
        "verification": (verification or {}).get("summary"),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    return summary
