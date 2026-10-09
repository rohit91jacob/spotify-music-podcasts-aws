# Verification against published figures

The pipeline's outputs are reconciled with figures each source publishes independently of the
data we pull. `uv run musicpods verify` computes everything below from the verification marts
(`dbt/models/marts/verification/`) and writes `verification.json`; the results site shows the
summary. Mismatches are reported, not hidden, with the causes we could establish.

**Recorded run:** live download on 2026-10-09 (UTC), 40-user ListenBrainz panel, listening window
2026-09-11 to 2026-10-09, 10 Apple storefronts, 60 RSS feeds. All numbers below come from that run.

## 1. Listens per user per day vs ListenBrainz listening-activity statistics

**What is compared.** For every panel user ListenBrainz publishes daily listen counts
([`/1/stats/user/<user>/listening-activity`](https://listenbrainz.readthedocs.io/en/latest/users/api/statistics.html),
range `week`: the current and previous Monday-Sunday weeks). We count the same user's listens per
UTC day in `raw.listen_events`, which is the end of the streaming path: ListenBrainz API -> bronze ->
replay -> Kinesis Data Firehose (moto) -> S3 -> sink -> Iceberg. Duplicate scrobbles are kept here,
because ListenBrainz counts them too. A day is *comparable* when both sides cover it completely:
after the first day we hold listens for, and before the day the statistics were last computed.

**Settled days.** ListenBrainz computes statistics from a snapshot of its listens. New listens
reach the statistics daily, and older listens or imports only on the 1st and 15th of the month
([data update intervals](https://listenbrainz.readthedocs.io/en/latest/general/data-update-intervals.html)).
In practice the last days of each published range were computed before many of their listens
were inserted. `is_settled` excludes the last `stats_settle_days` (3) days of the range.

| | User-days | Exact | Within 1% | Our listens | Published listens |
| --- | --- | --- | --- | --- | --- |
| Settled days | 426 | **425** | 425 | 35,674 | 35,667 |
| Last 3 days of the range (not settled) | 120 | 31 | | 10,112 | 3,642 |

* The one settled mismatch (+7, one user, 2026-09-30) is on the published side. Asking the
  ListenBrainz listens API directly for that user and day returns 104 listens, the same as we
  hold, while the statistics say 97.
* The tail is expected to disagree. For one fixture user, the published count for 2026-10-03 is 0
  while we hold 94 listens. Every one of them has an `inserted_at` after 2026-10-03 14:00 UTC,
  whereas the listens of the earlier, matching days had been inserted by then.
* **A bug this check found.** The first run had 414 of 426 settled days exact, and 11 of the 12
  misses were exactly one listen short. The listens endpoint pages backwards with an exclusive
  `max_ts`, and the client continued from the oldest timestamp of each page, so another listen
  with that exact timestamp was skipped. These panels have many: 82 duplicate timestamps on one
  affected day, from importers. Re-querying the API for two affected days returned the
  published counts (164 and 38) where we held 163 and 37; the third day re-queried is the +7
  case above. The fix (overlap one second, dedupe on
  `(listened_at, recording_msid)`, regression test
  `test_pagination_keeps_listens_sharing_the_boundary_second`) was followed by a
  `--lb-backfill` re-fetch. The sink received 111,950 events again, dropped 23,165 duplicates
  within the batch and 88,610 already in the table, and appended the **175** missing listens. That
  took the check from 414 to 425 exact.

## 2. Each user's top-10 artists vs ListenBrainz top-artist statistics

**What is compared.** The published per-user top artists for the statistics week
([`/1/stats/user/<user>/artists`](https://listenbrainz.readthedocs.io/en/latest/users/api/statistics.html),
`range=week`, with its `from_ts`/`to_ts`) against our count of that user's listens crediting the
artist (any artist in the credit, as ListenBrainz attributes) in the same window.

| User-artist pairs | Users | Exact | Within 5% | Within 20% | Our listens | Published |
| --- | --- | --- | --- | --- | --- | --- |
| 324 | 39 | 93 | 128 | 177 | 13,425 | 16,466 |

This check agrees much less, and we only partly explain it:

* The statistics week ended 2026-10-04, so it includes the unsettled days of check 1. Listens
  inserted after the snapshot make our side higher for some pairs.
* For other pairs the published side is far higher, and we hold almost none of those listens.
  Examples: an artist published with 274 listens where we hold 2; another with 136 where we hold
  0. The statistics attribute
  listens with ListenBrainz's own MBID mapping at computation time, which can differ from the
  `mbid_mapping` the listens endpoint returns today. Some entries look like they come from a
  different window (one published top artist has 128 listens for the stats week; we hold none in that
  week but 192 in the week of 2026-09-14).
* We did not find a definition that reproduces these statistics exactly, so this is reported as
  a weak check. Check 1 is the authoritative reconciliation of the listen stream.

## 3. Episodes per show: RSS feed vs Apple's published count

**What is compared.** For the 60 shows whose feeds we fetched (top 10 per storefront,
deduplicated), the items with a publish date in the show's own RSS feed vs Apple's
[iTunes Lookup](https://performance-partners.apple.com/search-api) `trackCount`, and the newest
item vs `releaseDate`.

| Shows | Equal counts | Feed lists fewer | Feed lists more | Latest episode within 1 h | Latest differs by more than 24 h |
| --- | --- | --- | --- | --- | --- |
| 60 | 29 | 28 | 3 | 37 | 14 |

* "Feed lists fewer" is mostly by design of the publishers. Large shows publish only recent items
  in their feed: *The Daily* lists 64 episodes against Apple's 2,744, and *Global News Podcast*
  lists 284 against 2,984. Feed-based cadence is computed from the most recent 20 full episodes,
  so it is unaffected. Feed-based "episodes in feed" is not a catalogue size.
* "Feed lists more": *The Joe Rogan Experience* (2,762 vs 2,000), *Front Burner* (2,163 vs 2,000)
  and *The News Agents* (1,239 vs 1,238). Two of the three report exactly 2,000 in iTunes. We have
  not confirmed whether Apple caps `trackCount`.
* Latest-episode differences of exactly 168 h (four weekly shows, three German and one US) mean that Apple's
  `releaseDate` is one episode behind the feed: the newest item is not in Apple's index yet.
  Positive differences (Apple newer than the feed we fetched, for example +265 h for *The Rest Is
  History*) suggest the feed URL served us a stale or region-specific copy. Not investigated
  further.

## 4. Fresh-release dates: ListenBrainz vs MusicBrainz

**What is compared.** The release date ListenBrainz lists for each fresh release
([`/1/explore/fresh-releases/`](https://listenbrainz.readthedocs.io/en/latest/users/api/misc.html))
against the first-release-date MusicBrainz publishes for the release group
([`/ws/2/release-group`](https://musicbrainz.org/doc/MusicBrainz_API)), for the 232 release
groups we looked up.

| Releases | Exact | Group released earlier | Partial or missing group date |
| --- | --- | --- | --- |
| 232 | **232** | 0 | 0 |

ListenBrainz builds fresh releases from MusicBrainz data, so this is not independent evidence
about the true release date. It verifies the pipeline instead: date parsing (`YYYY`, `YYYY-MM`,
`YYYY-MM-DD`), the MusicBrainz batched lookups (exact-id filtering of search results), and the
joins between the two sources. All of these feed the new-release discovery-lag mart.

## Fixture run (CI)

CI runs the same checks on the committed fixtures (6 real panel users; podcast data simulated).
The 2026-10-09 run: settled user-days 66 of 66 exact (2,716 = 2,716 listens), top artists 20 of 50
pairs exact, release dates 100 of 100. The simulated podcast fixtures agree with themselves by
construction, so check 3 is not meaningful on fixtures.

## Reproduce

```bash
uv run musicpods run                       # live download + pipeline
uv run musicpods verify --out site/verification.json
uv run musicpods sql "SELECT * FROM marts.ver_user_daily_listens WHERE is_settled AND difference <> 0"
```
