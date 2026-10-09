# Music and podcast analytics on an emulated AWS lakehouse

[![CI](https://github.com/rohit91jacob/spotify-music-podcasts-aws/actions/workflows/ci.yml/badge.svg)](https://github.com/rohit91jacob/spotify-music-podcasts-aws/actions/workflows/ci.yml)
[![refresh](https://github.com/rohit91jacob/spotify-music-podcasts-aws/actions/workflows/refresh.yml/badge.svg)](https://github.com/rohit91jacob/spotify-music-podcasts-aws/actions/workflows/refresh.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](pyproject.toml)
[![Data: CC0 + CC BY-NC-SA](https://img.shields.io/badge/data-see%20DATA__LICENSE-lightgrey.svg)](DATA_LICENSE.md)

Spotify-style listening and podcast metrics (artist momentum, genre share, listening sessions,
skip-like short plays, new-release discovery lag, podcast chart movement and longevity by
country, episode release cadence) built from **real public data**:

* **ListenBrainz** (CC0): the actual listens of a fixed, pseudonymised panel of 40 public users,
  replayed as a live event stream through **Kinesis Data Firehose -> S3 -> Apache Iceberg**;
* **MusicBrainz** (CC0 core data): artist, recording and release metadata and genres;
* **Apple Podcasts** top charts for 10 storefronts, the **iTunes Lookup API**, and each charting
  show's own **RSS feed**.

Batch "Glue" jobs turn the landed JSON into typed Iceberg tables behind data-quality gates, dbt
builds tested, contract-enforced marts (DuckDB locally, compiled for Athena in CI), and the whole
flow is a **Step Functions** state machine on an **EventBridge Scheduler** schedule. A results
site is rebuilt daily on GitHub Pages.

**What is real and what is emulated.** The data is real and downloaded at run time. AWS is
**emulated**: moto in-process by default, plus LocalStack and moto_server in CI. The state
machine is interpreted locally from the same ASL file Terraform deploys. Everything under
`infra/` is Terraform that is formatted, validated, linted and scanned, but **not applied**:
no AWS account was used to build or run this repository.

## Contents

1. [Architecture](#architecture)
2. [Results from a recorded run](#results-from-a-recorded-run)
3. [Tech stack](#tech-stack)
4. [Data sources](#data-sources)
5. [Data model](#data-model)
6. [Quickstart](#quickstart)
7. [Local emulator to AWS service](#local-emulator-to-aws-service)
8. [Configuration and CLI](#configuration-and-cli)
9. [Data quality, idempotency and verification](#data-quality-idempotency-and-verification)
10. [Testing and CI](#testing-and-ci)
11. [Infrastructure and cost](#infrastructure-and-cost)
12. [Project structure](#project-structure)
13. [Design decisions](#design-decisions)
14. [Limitations](#limitations)
15. [License](#license)

## Architecture

```mermaid
flowchart LR
    subgraph Sources["Public sources (credential-free)"]
        LB[ListenBrainz API<br/>listens + statistics]
        MB[MusicBrainz API<br/>1 req/s]
        AP[Apple Podcasts<br/>top charts RSS generator]
        IT[iTunes Lookup API]
        RSS[Podcast RSS feeds]
        OPT[Last.fm / Podcast Index / Spotify<br/>optional, key-gated]
    end

    subgraph Orchestration
        EBS([EventBridge Scheduler<br/>daily 06:15 UTC]) --> SFN{{Step Functions<br/>pipeline.asl.json}}
    end

    subgraph Lake["S3 lake bucket"]
        BR[(bronze/<br/>immutable JSON, sha256 manifest)]
        FH[(firehose/listen-events/<br/>YYYY/MM/DD/HH JSON Lines)]
        ICE[(iceberg/<br/>raw.* tables)]
    end

    LB & MB & AP & IT & RSS & OPT -->|ingest jobs| BR
    BR -->|replay-listens<br/>PutRecordBatch| KF[Kinesis Data Firehose]
    KF --> FH
    FH -->|listen-events-to-iceberg<br/>validate, dedupe on event_id, DLQ| ICE
    BR -->|build-raw-tables<br/>typed + DQ-gated| ICE
    ICE -->|Glue Data Catalog| DBT[dbt build<br/>staging -> intermediate -> marts<br/>Athena / DuckDB]
    DBT --> MARTS[(marts.*<br/>contracts + tests)]
    MARTS --> SITE[Static results site<br/>GitHub Pages]
    SFN -.runs.-> BR & KF & ICE & DBT
    SFN -.on failure.-> SNS[SNS alert]
```

| Component | Responsibility | Where |
| --- | --- | --- |
| Ingest | Polite HTTP clients (User-Agent, per-host pacing, retries, rate-limit headers); land each response once per UTC day in an immutable, content-addressed bronze zone; pseudonymise users | `src/musicpods/sources/`, `bronze.py`, `http.py` |
| Stream replay | Turn real listens into schema-versioned events with deterministic ids; `PutRecordBatch` to Firehose (500 records / 4 MiB), resending only failed records | `src/musicpods/streaming/replay.py`, `events.py` |
| Firehose sink | Firehose S3 output -> Iceberg `raw.listen_events`: validate, dead-letter bad records, dedupe in batch and against the table, DQ gate, one snapshot per batch | `src/musicpods/streaming/sink.py` |
| Glue-style jobs | Bronze -> 14 typed Iceberg raw tables, every one DQ-gated before the commit; all-or-nothing | `src/musicpods/glue/jobs.py`, `quality/checks.py` |
| dbt | 36 models (staging views, intermediate, contract-enforced marts) and 103 tests; same SQL on DuckDB and Athena | `dbt/` |
| Orchestration | ASL state machine (Parallel ingest, Retry, Catch -> SNS -> Fail) deployed by Terraform; interpreted locally by a small runner; registered with Step Functions + Scheduler in the emulator | `src/musicpods/orchestration/` |
| Glue entrypoint | What a deployed Glue job runs: sync state from S3, run one step against real AWS and the Glue catalog, sync back | `src/musicpods/glue/entrypoint.py` |
| Verification | Reconcile outputs with what the sources publish | `src/musicpods/verify.py`, `dbt/models/marts/verification/` |
| Results site | One static HTML page + `summary.json`, no JavaScript | `src/musicpods/report.py` |
| Infrastructure | KMS, S3, Firehose, Glue jobs and catalog, Athena workgroup, Step Functions, Scheduler, SNS, budget, alarms, GitHub OIDC role | `infra/` |

## Results from a recorded run

From one live run on **2026-10-09** (download, then the whole pipeline on moto), plus a backfill
re-fetch the same evening (see [verification](#data-quality-idempotency-and-verification)). The
daily refresh workflow publishes the current numbers to GitHub Pages.

| Metric | Value |
| --- | --- |
| Listens landed (raw.listen_events, after dedup on event_id) | 89,812 from 40 panel users, 2026-09-11 to 2026-10-09 |
| Listens in the marts (751 duplicate scrobbles excluded) | 89,061 |
| Listens mapped to a MusicBrainz recording | 83.0% |
| Distinct artists heard / with a MusicBrainz genre | 6,686 / 467 (MusicBrainz lookups are capped at the 600 most-played artists) |
| Listening sessions (30-minute gap) | 3,842; median 35.7 min, 90th percentile 175.3 min, 23.2 listens per session |
| Skip-like short plays | 1,503 of 51,264 derivable listens (2.9%) |
| Fresh releases (last 30 days) the panel played | 70; median discovery lag 2.5 days, 71.4% first played within a week |
| Apple Podcasts chart entries | 1,000 (10 storefronts x top 100), 796 distinct shows, 1 daily snapshot so far |
| Shows with feed-based cadence | 60 (21 daily, 18 several per week, 19 weekly, 1 irregular, 1 insufficient history) |
| Episodes parsed from RSS | 38,062 |
| dbt build | 139 nodes (36 models, 103 tests) passed |
| Settled user-days matching ListenBrainz's published daily counts | **425 of 426** exactly (35,674 vs 35,667 listens) |

Artist momentum for the week of 2026-09-28: the largest gains were Magdalena Bay (+498 listens),
Taylor Swift (+416) and Playboi Carti (+405). With 40 users these are panel movements, not
market trends.

## Tech stack

| Tool | Version | Role |
| --- | --- | --- |
| Python | 3.12 (3.13 on Glue 6.0) | runtime |
| uv | 0.10 | dependencies (`uv.lock`) |
| boto3 / moto | 1.43.110 / 5.2.3 | AWS SDK / in-process AWS emulator (S3, Firehose, Step Functions, Scheduler) |
| LocalStack | 4.4.0 (CI only) | second emulator for the endpoint path |
| PyIceberg (+ pyiceberg-core) | 0.12.0 (0.10.1) | Iceberg tables: SQLite catalog locally, Glue catalog when deployed |
| PyArrow | 26.0.0 | typed batches |
| DuckDB | 1.5.6 | local warehouse (Athena stand-in), DQ check engine |
| dbt-core / dbt-duckdb / dbt-athena-community | 1.12.5 / 1.11.0 / 1.11.1 | transformations, contracts, tests |
| requests, defusedxml | 2.34.2, 0.7.1 | HTTP; safe RSS parsing (no entity expansion) |
| Terraform / AWS provider | 1.16.5 / ~> 6.68 | infrastructure as code |
| tflint (+ aws ruleset 0.49.0), checkov | 0.64.0, 3.3.26 | IaC linting and security scanning |
| ruff, pytest, pre-commit, gitleaks | 0.16.10, 9.1.1, 4.6.2, 8.30.1 | quality gates |

## Data sources

| Source | Data used | Licence | Refresh cadence (source / ours) | Known gaps |
| --- | --- | --- | --- | --- |
| [ListenBrainz API](https://listenbrainz.readthedocs.io/en/latest/users/api/index.html) | listens of the panel; per-user daily activity and top artists; sitewide top artists; fresh releases | CC0 1.0 | listens immediately; statistics daily for new listens, 1st and 15th of the month for older listens ([intervals](https://listenbrainz.readthedocs.io/en/latest/general/data-update-intervals.html)) / daily, incremental by per-user cursor | a panel of 40 public users is not representative of anyone; 17% of listens are not mapped to MusicBrainz; the listens endpoint is often slow |
| [MusicBrainz API](https://musicbrainz.org/doc/MusicBrainz_API) | artists (type, country, tags), recordings (length), release groups (type, first-release date), genre list | core CC0; tags (genre associations) CC BY-NC-SA 3.0 | live / ids looked up once and refreshed after 30 days, at 1 request per second | only the most-played 600 artists, 3,000 recordings and 2,000 release groups (+200 fresh-release groups) are looked up; partial dates (`2019`) are kept raw and excluded from age buckets |
| [Apple Podcasts top charts](https://rss.marketingtools.apple.com/) | Top Shows (100) for us, gb, ca, au, in, ie, de, fr, br, jp | Copyright Apple; not redistributed | Apple publishes no cadence (the feed's `updated` field is the response time) / one snapshot per storefront per UTC day | movement and longevity need several days of snapshots; the recorded run has one |
| [iTunes Lookup API](https://performance-partners.apple.com/search-api) | feed URL, genre, episode count, latest release date | Apple API terms (no artwork or previews used) | live / daily per storefront, paced below ~20 calls/min | `trackCount` and `releaseDate` lag or disagree with feeds for some shows (see verification) |
| Podcast RSS feeds | publish dates, durations, episode type (guid hashed; titles not stored) | each publisher | live / daily for the top 10 shows per storefront (60 feeds) | many large shows publish only recent episodes in their feed |
| Last.fm, Podcast Index, Spotify Web API (optional) | global top artists; independent episode counts; catalogue metadata only | each API's terms | daily when the secret is set | not used in the recorded run (no keys). Spotify: since November 2024 new apps cannot call audio-features or recommendations, so nothing depends on them |

The committed fixtures are 6 real ListenBrainz users with their MusicBrainz entities (CC0, plus
CC BY-NC-SA tags), and **simulated** podcast data. Apple's data cannot be redistributed, so the
podcast fixtures are generated with a fixed seed and every show is named `SYNTHETIC Show NN`.
Licences, attribution and privacy measures: [DATA_LICENSE.md](DATA_LICENSE.md).

**How the listener panel is chosen** (deterministic, `src/musicpods/sources/listenbrainz.py`):

1. Take ListenBrainz's sitewide top artists of the week and keep every third (to reach beyond the
   head of the chart).
2. Collect each seed artist's top listeners.
3. Order candidates by a hash of their name and keep the first 40 whose published activity is
   plausible for one person (no day above 480 listens, some activity this week).

The panel is persisted so week-over-week metrics compare the same people. User names never
leave the ingest step: records carry a salted SHA-256 `user_key`, and logged errors are redacted.

## Data model

Full column-level documentation: [docs/data_dictionary.md](docs/data_dictionary.md) (generated
from the dbt contracts). Layers: `raw` (Iceberg) -> `staging` (views) -> `intermediate` -> `marts`.

| Model | Grain | Answers |
| --- | --- | --- |
| `fct_listens` | one non-duplicate listen | session id, short-play basis and flag, artist and genre |
| `fct_listening_sessions`, `mart_listening_sessions_daily` | session / UTC day | session length, listens, short plays |
| `dim_artist` | artist (MBID or credited name) | MusicBrainz metadata, primary genre |
| `mart_artist_momentum` | ISO week x artist | listens vs the previous week, momentum rank, complete/comparable-week flags |
| `mart_genre_share` | week x primary genre | share of listens |
| `mart_short_plays_weekly` | week x artist | skip-like short-play rate where derivable |
| `mart_new_release_share` | week x release-age bucket | how much listening goes to new releases |
| `mart_release_discovery_lag` | fresh release group | days from release to first panel play, first-week listens |
| `dim_podcast` | show | Apple metadata, first/last charted |
| `mart_podcast_chart_movement` | snapshot x storefront x rank | rank change and movement class |
| `mart_podcast_chart_longevity` | storefront x show | snapshots on chart, current streak, best rank |
| `mart_podcast_cross_country` | show | storefronts charting it now |
| `mart_episode_cadence` | show | median gap between recent episodes, cadence class |
| `ver_*` | see [verification](docs/verification.md) | reconciliation with published figures |

**Sessionisation.** A new session starts after more than `session_gap_minutes` (30) without a new
listen.

**Short plays.** A listen counts as a short play when it played for less than half the track:

* measured directly where the client reports playback time (`ms_played` from Spotify history
  imports);
* otherwise inferred when the next listen in the same session started before half the track had
  elapsed, with gaps under 30 s treated as timestamp collisions from batch imports, not plays.

Clients only submit a listen after half the track or 4 minutes by default, so most real skips
never reach ListenBrainz.

## Quickstart

Prerequisites: [uv](https://docs.astral.sh/uv/) (installs Python 3.12), git. `make` is optional;
every target is a plain `uv run` command (see `make help` / the `Makefile`).

```bash
git clone https://github.com/rohit91jacob/spotify-music-podcasts-aws.git
cd spotify-music-podcasts-aws
uv sync --frozen

# 1. Whole pipeline on the committed fixtures: no network, AWS emulated in-process (~1 min)
uv run musicpods --data-dir data/fixtures run --from-dir tests/fixtures/bronze --run-date 2026-10-09 --dbt-target ci
uv run musicpods --data-dir data/fixtures verify
uv run musicpods --data-dir data/fixtures report --out site     # open site/index.html

# 2. The same on live data (MusicBrainz at 1 req/s: about 15-30 minutes the first time)
uv run musicpods run
uv run musicpods verify --out site/verification.json && uv run musicpods report --out site

# 3. Explore
uv run musicpods sql --list
uv run musicpods sql "SELECT week_start, artist_name, wow_change FROM marts.mart_artist_momentum WHERE is_comparable_week ORDER BY week_start DESC, momentum_rank LIMIT 10"
```

Against LocalStack or moto_server instead of in-process moto:

```bash
uv run moto_server -p 5000 &          # or: docker run -p 4566:4566 localstack/localstack:4.4.0
export MUSICPODS_AWS_ENDPOINT_URL=http://127.0.0.1:5000 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
uv run pytest -m integration tests/integration
```

## Local emulator to AWS service

| In this repo (local / CI) | In AWS (Terraform in `infra/`) |
| --- | --- |
| moto `firehose` in-process; LocalStack / moto_server in CI | Kinesis Data Firehose (Direct PUT -> S3, GZIP, SSE-KMS) |
| moto `s3`, mirrored to `data/s3/` | S3 lake bucket (versioned, SSE-KMS, TLS-only, access-logged, lifecycle) |
| PyIceberg `SqlCatalog` (SQLite) + local warehouse | PyIceberg `GlueCatalog`: AWS Glue Data Catalog + Iceberg on S3 |
| Functions in `musicpods.glue.jobs` / `orchestration.steps`, run in-process | One AWS Glue 6.0 job per step running `glue/entrypoint.py` |
| DuckDB (`dbt-duckdb`) | Amazon Athena workgroup (`dbt-athena`; compiled in CI) |
| `musicpods.orchestration.runner` interpreting `pipeline.asl.json`; moto Step Functions + Scheduler for registration | AWS Step Functions (same ASL file via `templatefile`) + EventBridge Scheduler |
| `ops/alerts.jsonl` | SNS topic (KMS-encrypted) + CloudWatch alarms + AWS Budgets |
| `.env` / repository secrets | AWS Secrets Manager (`musicpods/lastfm`, ...) read by the ingest job |
| GitHub Pages results site | (same) |

## Configuration and CLI

All settings are `MUSICPODS_*` environment variables with safe defaults; see
[.env.example](.env.example). Invalid values fail fast with exit code 2.

```
musicpods ingest   [--source S] [--from-dir DIR] [--lb-backfill]   download (or import fixtures) into bronze
musicpods replay   [--full] [--limit N] [--duplicate-every N]      listens -> Kinesis Data Firehose
musicpods sink                                                       Firehose S3 output -> Iceberg raw.listen_events
musicpods glue     [--job NAME]                                      bronze -> DQ gate -> Iceberg raw tables
musicpods load | dbt <args>                                          raw -> DuckDB; dbt (loads raw first)
musicpods run      [--from-dir DIR] [--run-date D] [--full-replay]   the whole state machine
musicpods asl      validate | render | register                      check / print / register in the emulator
musicpods verify | report --out site | sql "<query>" | maintain       reconcile; site; ad-hoc SQL; expire snapshots
```

Exit codes: 0 ok, 1 unexpected error, 2 configuration, 3 data-quality gate, 4 dbt error.

## Data quality, idempotency and verification

* **Gates between layers.** Every raw table is checked before its Iceberg commit (not null,
  unique keys, ranges, UUID formats, ISO country codes, timestamps not in the future, minimum
  rows). An `error` failure stops the step with exit code 3 and leaves the previous snapshot in
  place; `warn` checks are logged to `ops/dq_results.jsonl`. dbt gates the marts with 103
  tests, including singular tests: every listen belongs to exactly one session, genre shares sum
  to 1, each chart snapshot is a gapless ranking, and the session gap is respected.
* **Exactly-once rows over at-least-once delivery.** `event_id` is a hash of
  (user, timestamp, message id). The sink drops duplicates within a batch and against the table,
  and moves its cursor only after the commit. On the live data, a full re-fetch re-sent 111,950
  events; the sink appended only the 175 that were actually missing.
* **Reruns.** A second `musicpods run` on the same inputs replays 0 events, appends 0 rows and
  leaves every mart byte-for-byte identical. This is asserted in `tests/e2e`, in CI, and after a
  deliberate DQ failure.
* **Verification** ([docs/verification.md](docs/verification.md)) against figures the sources
  publish:

  | Check | Result (live run) |
  | --- | --- |
  | Listens per user and UTC day vs ListenBrainz's listening-activity statistics (settled days) | 425 of 426 user-days exact |
  | Each user's top-10 artists vs ListenBrainz's top-artist statistics | 93 of 324 pairs exact, 177 within 20% (a weak check; causes discussed) |
  | Episodes in each RSS feed vs Apple's `trackCount` | 29 of 60 equal; 28 feeds list fewer (feeds publish recent episodes only) |
  | ListenBrainz fresh-release dates vs MusicBrainz release-group dates | 232 of 232 equal (both derive from MusicBrainz; checks parsing and joins) |

  The daily check found a real pagination bug (listens sharing a timestamp at a page boundary
  were skipped). It was fixed, regression-tested and backfilled, which took exact days from 414
  to 425.

## Testing and CI

```bash
uv run ruff check . && uv run ruff format --check .
uv run pytest tests/unit                 # 65 tests, moto in-process, HTTP mocked
uv run pytest -m e2e tests/e2e           # whole pipeline on fixtures, reruns, full replay, DQ failure
uv run pytest -m integration tests/integration   # needs MUSICPODS_AWS_ENDPOINT_URL
```

Locally, 73 unit and e2e tests (65 + 8) pass with 86% line coverage, and the integration test passes
against moto_server.

[`ci.yml`](.github/workflows/ci.yml) (actions pinned by commit SHA):

| Job | What it does |
| --- | --- |
| lint | ruff, pre-commit (whitespace, YAML/TOML/JSON, files under 300 KB, private keys, gitleaks) |
| unit | ASL validation + unit tests with coverage |
| pipeline | `musicpods run` on fixtures twice (rerun is a no-op), standalone `dbt build`, verify, report, e2e tests, dbt docs |
| dbt-athena | `dbt compile --target athena` with dbt-athena, no AWS connection |
| terraform | `fmt -check`, `init -backend=false`, `validate`, tflint, checkov |
| emulator-moto-server, emulator-localstack | Firehose -> S3 -> Iceberg-on-S3 round trip against a network AWS endpoint |

Other workflows: [`refresh.yml`](.github/workflows/refresh.yml) runs daily at 06:15 UTC on live
data and publishes the site to GitHub Pages, opening an issue when a scheduled run fails.
[`keepalive.yml`](.github/workflows/keepalive.yml) keeps schedules enabled.
[`deploy.yml`](.github/workflows/deploy.yml) is manual, runs only when the `AWS_DEPLOY_ROLE_ARN`
repository variable is set, and authenticates with OIDC (no static keys). It has never been run.

## Infrastructure and cost

`infra/` holds a root module and eight child modules: `kms`, `lake`, `catalog`, `streaming`,
`glue_jobs`, `orchestration`, `monitoring` and `github_oidc`.

* **Encryption.** One customer-managed KMS key (rotation on) for S3, Firehose, Glue (logs,
  bookmarks, S3), the catalog, Step Functions, SNS and CloudWatch Logs.
* **Least-privilege IAM.** Each role is scoped to its prefixes and resources: Firehose writes
  only `firehose/`, Glue reads `firehose/` and writes `bronze/`, `state/` and `iceberg/`. Service-role
  trust policies are restricted to this account; the GitHub OIDC role is restricted to one
  repository and environment.
* **Buckets.** Public access blocks, TLS-only bucket policies, versioning, access logs, and
  lifecycle rules (Firehose raw data moves to IA after 30 days; Athena results expire after 7).
* **Guardrails.** An Athena per-query scan limit of 1 GiB, Glue job timeouts with
  `max_concurrent_runs=1`, a monthly AWS Budget on the `project` tag (default 25 USD; alerts at
  80% actual and 100% forecast), and alarms for a failed execution, a missing daily run and
  Firehose data freshness.
* **Checks.** `terraform validate`, tflint and checkov all pass (252 checks). The 13 inline checkov
  skip annotations (17 skipped results, as some cover `for_each` instances) each give a
  reason: for example, key policies cannot name their own ARN, and access-log
  buckets cannot use SSE-KMS.

**Cost notes.** These are estimates, not measured: nothing was deployed.

* Data is tiny: about 23 MB of bronze per first run and 14 MB of Iceberg.
* The dominant cost would be Glue. Each step is a 2-DPU Glue 6.0 Spark job, billed with a
  1-minute minimum, and the daily run has 9 steps. At about 3 minutes each, that is
  9 x 2 DPU x 0.05 h x 0.44 USD = roughly 0.40 USD a day.
* Athena scans are kilobytes to megabytes; Firehose ingests a few thousand small records per day after the first run.
* Moving the light steps to Lambda (or one container task) would cut cost by an order of
  magnitude. A Spark job type is used only because Glue's Python shell jobs stop at Python 3.9.

## Project structure

```
src/musicpods/
  sources/        listenbrainz, musicbrainz, podcasts (charts, lookup, RSS), optional (key-gated)
  streaming/      events (schema v1), replay (-> Firehose), sink (-> Iceberg)
  glue/           jobs (bronze -> raw), schemas (Arrow), entrypoint (deployed Glue jobs)
  orchestration/  pipeline.asl.json, runner (local ASL interpreter), steps
  quality/        DQ checks and gates
  bronze.py http.py lakehouse.py warehouse.py verify.py report.py cli.py config.py aws.py
dbt/              staging/ intermediate/ marts/{music,podcasts,verification}/ tests/ macros/
infra/            main.tf + modules/{kms,lake,catalog,streaming,glue_jobs,orchestration,monitoring,github_oidc}
tests/            unit/ e2e/ integration/ fixtures/bronze/ (real LB + MB, simulated podcasts)
scripts/          build_fixtures.py, make_synthetic_podcast_fixtures.py, gen_data_dictionary.py
docs/             data_dictionary.md, runbook.md, verification.md
```

## Design decisions

* **Replay real listens instead of generating events.** The stream is real user behaviour with
  real timestamps, duplicates and client quirks. `--duplicate-every` exists only as a drill for
  the dedup path; it is off by default and labelled as such.
* **Firehose -> S3 -> commit job, not Firehose's native Iceberg destination.** That keeps
  validation, the dead-letter path and exactly-once dedup in code that the emulators and tests
  can run. The native destination would be a reasonable choice in production.
* **One ASL file for both worlds.** Terraform renders it for Step Functions, and the local runner
  interprets it (Parallel, Retry, Catch, Fail). Local runs and CI therefore exercise the
  deployed control flow, including the alert path.
* **Bronze is the history.** Raw tables are rebuilt from bronze in one snapshot per run, which
  makes reruns trivial and rule changes retroactive. That stays cheap at this scale (tens of MB).
* **Settled-day reconciliation.** The published statistics lag; comparing the lagging tail would
  report noise as error. The tail is still reported, separately.

## Limitations

* **Emulated cloud only.** Nothing ran on AWS. The Glue entrypoint, the Glue catalog path and
  dbt-athena are exercised by unit tests and compilation, not against real services. The
  LocalStack job is written for CI and has not been run locally (no Docker here). The same
  endpoint test was run locally against moto_server.
* **Sample, not population.** 40 public ListenBrainz users, chosen by a documented rule, skew
  towards heavy, technical listeners. Rankings are panel facts, not market facts.
* **Podcast history starts at the first run.** Apple's feed has no history, so movement and
  longevity accumulate one snapshot per day. The recorded run has one, so every entry is a
  baseline.
* **Metadata coverage is capped.** Genres come from at most 600 MusicBrainz artists (467 tagged),
  so 33% of the week's listens fall in `unknown` or `unmapped`.
* **Short plays are a lower bound.** Most skips never become listens. The `next_listen` inference
  depends on client behaviour.
* **Top-artist reconciliation is weak** (93 of 324 exact), for reasons explained but not fully
  resolved in [docs/verification.md](docs/verification.md).
* **Local Windows runs** print harmless faulthandler dumps from `platform._wmi_query` during
  imports. They do not affect results.

## License

Code: MIT ([LICENSE](LICENSE)). Data: see [DATA_LICENSE.md](DATA_LICENSE.md). ListenBrainz and
MusicBrainz data by the MetaBrainz Foundation and contributors; podcast charts and metadata by
Apple. Not affiliated with Spotify, Apple, ListenBrainz or MusicBrainz.
