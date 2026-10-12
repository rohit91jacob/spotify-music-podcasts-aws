# Runbook

How to operate the pipeline locally, in CI and (if ever deployed) in AWS. Commands assume the
repository root and `uv sync --frozen`. `make <target>` wraps most of them (`make help`).

## Exit codes

| Code | Meaning | Typical cause | Action |
| --- | --- | --- | --- |
| 0 | success | | |
| 1 | unexpected error | network outage, emulator not reachable, a bug | read the traceback in the log; rerun (every step is idempotent) |
| 2 | invalid configuration | a `MUSICPODS_*` variable with a bad value | the message names the variable; fix `.env` or the workflow env |
| 3 | data-quality gate failed | an error-level raw check, or a dbt data test | see [Data-quality failure](#data-quality-failure) |
| 4 | dbt failed for another reason | SQL compile/runtime error | run `uv run musicpods dbt build` and read the failing node |

## Daily run

```bash
uv run musicpods run                    # whole state machine on live data
uv run musicpods verify --out site/verification.json
uv run musicpods report --out site
```

`musicpods run` interprets `src/musicpods/orchestration/pipeline.asl.json`, the same definition
Terraform deploys to Step Functions. Every execution writes its state history to
`$MUSICPODS_DATA_DIR/ops/executions/<id>.json`; failures also append to `ops/alerts.jsonl` (the
local stand-in for the SNS topic).

Expected duration on a laptop: 15-30 minutes on the first run (MusicBrainz is paced at one
request per second; ListenBrainz's listens endpoint is often slow), a few minutes afterwards.

## Rerunning and backfilling

Every step is safe to rerun:

| Step | Why a rerun is safe |
| --- | --- |
| ingest | Bronze files are content-addressed and immutable; a chart/lookup/feed already landed for the UTC day is not fetched again; ListenBrainz cursors (`ops/lb_listen_state.json`) only move forward after a page is landed |
| replay | Pages already replayed are listed in `ops/replay_state.json`; `--full` re-sends everything |
| sink | Records are deduplicated within the batch and anti-joined on `event_id` against the table, so redelivery appends nothing; the object cursor is written only after the Iceberg commit |
| glue | Each table is rebuilt from the complete bronze history and replaced in one Iceberg snapshot |
| dbt | Tables are rebuilt from raw |

Backfill or repair ListenBrainz listens (for example after a client fix) by ignoring the cursors:

```bash
uv run musicpods ingest --source listenbrainz --lb-backfill
uv run musicpods replay && uv run musicpods sink && uv run musicpods glue && uv run musicpods dbt build
```

Only listens that were missing are appended (`appended` in the sink output); the rest are
reported as `already_in_table`.

## Data-quality failure

1. Find the failing check: the log line `DQ <table> <check>: N violating of M rows`, or
   `ops/dq_results.jsonl` (one JSON object per check and run). For dbt tests, the failing
   test name is in the dbt output.
2. Nothing was committed for the failing table: raw tables keep their previous Iceberg snapshot,
   the sink does not advance its cursor, and the marts keep the last good build.
3. Decide whether the data or the rule is wrong. Upstream quirks the rules already allow for
   (so a new failure usually means something else changed): MusicBrainz partial dates (`2019`,
   `2019-05`), clients sending `duration` in seconds instead of `duration_ms`, feeds repeating an
   item, and listens that ListenBrainz could not map to MusicBrainz.
4. Fix and rerun `uv run musicpods run`. To publish anyway while investigating, set
   `MUSICPODS_DQ_FAIL_ON_ERROR=false` for one run (results are still recorded).
5. If the bad payload itself is in bronze, a same-day rerun will not replace it: bronze keeps
   the first version of each file per day and logs `already landed with different content` for
   a new one. Move the offending `bronze/<source>/<dataset>/dt=<day>/<name>.json.gz` aside
   (and its line in `bronze/_manifest.jsonl`) before rerunning, so the source is fetched again.

Bad Firehose records never fail a run: they go to `ops/dlq/listen_events.jsonl` with a reason
(`unparseable_json`, `missing:<field>`, `event_id_mismatch`, ...). Inspect, fix the producer, and
replay with `--full`; the sink drops whatever is already in the table.

## Source problems

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `ListenBrainz returned no sitewide artist stats` | statistics are being recomputed and no earlier snapshot is cached (a run with one retries 3 times, then seeds the panel from the last snapshot and logs a warning) | rerun later |
| `listens for u_... failed, skipped this run` | slow or failing listens endpoint for one user | nothing; the next run resumes from that user's cursor |
| `chart xx unavailable` / `iTunes lookup for xx unavailable` | Apple endpoint timeout or DNS failure | nothing; that storefront is retried by the next run, other storefronts continue |
| `feed for podcast N failed` | publisher feed down, too large (> `MUSICPODS_RSS_MAX_BYTES`) or malformed | recorded with `status=error:<type>` in bronze; excluded from cadence |
| many HTTP 503 from MusicBrainz | rate limit | keep `MUSICPODS_MB_MIN_INTERVAL_SECONDS` >= 1 |
| optional source skipped | its secret is not set | expected; see `.env.example` |

## Scheduled refresh failed

`refresh.yml` runs daily at 06:15 UTC on GitHub Actions and opens (or comments on) an issue
titled "Scheduled refresh is failing" when a scheduled run fails.

1. Open the run linked in the issue and find the failing step.
2. Exit code 3: follow [Data-quality failure](#data-quality-failure). Exit 1 with HTTP errors:
   usually a source outage; rerun the workflow (`workflow_dispatch`) later.
3. The data directory is restored from the Actions cache. If the cache was evicted (7 days
   without use) the next run starts from scratch: the ListenBrainz panel is rebuilt and the chart
   history restarts. To keep pseudonyms stable across cache loss, set the
   `MUSICPODS_PSEUDONYM_SALT` repository secret.
4. Close the issue after a green run.

`keepalive.yml` re-enables the scheduled workflows twice a month, because GitHub disables
schedules in repositories without activity for 60 days.

## Emulators

* Default: moto in-process; nothing to start.
* LocalStack or `moto_server`: set `MUSICPODS_AWS_ENDPOINT_URL` (for example
  `http://localhost:4566`). The Firehose output, Iceberg data and metadata then live in the
  emulated bucket and the sink reads S3 directly. `uv run pytest -m integration` exercises this
  path; CI runs it against both emulators.

## Deploying to AWS (not done in this repository)

1. `terraform -chdir=infra init -backend-config=environments/dev.s3.tfbackend` and
   `terraform -chdir=infra apply -var-file=environments/dev.tfvars` from an operator machine.
2. Activate the `project` tag as a cost allocation tag (the budget filters on it) and confirm
   the SNS email subscriptions.
3. Set the repository variables `AWS_DEPLOY_ROLE_ARN` (output `deploy_role_arn`),
   `MUSICPODS_ARTIFACTS_BUCKET` and `MUSICPODS_STATE_MACHINE_ARN`; create the `aws-dev`
   environment with required reviewers.
4. Run the `deploy` workflow: it builds the wheel and uploads it with the Glue entrypoint over
   OIDC, then optionally starts one execution.
5. Copy `ops/` state if migrating from local runs: `s3://<lake>/state/ops/` and bronze to
   `s3://<lake>/bronze/`.

Alarms: `pipeline-failed` (any failed execution), `pipeline-not-started` (no execution in 26 h),
`firehose-s3-freshness` (records waiting more than 15 minutes), and the monthly budget
(80% actual, 100% forecast). All notify the alerts SNS topic.

## Unexplained local crash (Windows)

During review, one of five local runs on Windows (a sampled live run) stopped inside the
`listen-events-to-iceberg` step with exit code 127 and no Python traceback. Running the same
command again completed normally, and two further runs from the same bronze did not reproduce
it. The cause is not known; it has not been seen in a test or fixture run. Because every step is
rerun-safe, the remedy is to run `uv run musicpods run` again.
