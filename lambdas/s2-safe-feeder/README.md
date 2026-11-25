# s2-safe-feeder

Reads one part of the S3 Inventory of the Sentinel-2 SAFE bucket and queues a
Cirrus workflow payload with `safe_href` for each `.SAFE` it lists, so the
`sentinel-2-l2a-to-stac` task builds the STAC item and COGs from the raw ESA
archive. Only L2A SAFEs are queued: any other product would get its L2A scene's
item ID, and once the task rejects it as INVALID, Cirrus skips that ID for good.
It is a manually triggered backfill: there is no schedule and no S3 event.

## How it works

1. `scripts/submit_safe_payloads.py` (run by hand) reads one inventory
   `manifest.json` and sends one feeder payload per data part listed in its
   `files[]` to the queue that triggers this lambda.
2. Each invocation reads its part and keeps one row per SAFE: its
   `DATASTRIP/DS_*/MTD_DS.xml` key. So a SAFE split across two parts is queued
   by exactly one of them, and a SAFE without that key is never queued. It
   derives `safe_href` (`s3://<bucket>/<path>/<name>.SAFE`), the processing
   baseline and the Earth Search item ID (the task's rule), drops non-L2A SAFEs
   and SAFEs it can't parse, applies the baseline filters and `limit`, and sends one Cirrus
   payload per remaining SAFE to `SQS_QUEUE_URL` (the Cirrus process queue).

The Cirrus payload ID is built from the item ID. Cirrus starts a payload only
if that ID has never run, or last FAILED, was ABORTED, or is stuck CLAIMED. An
ID that succeeded, is still PROCESSING, or was rejected as INVALID is skipped.
So scenes an earlier backfill finished under the same ID are not reprocessed,
and resubmitting a manifest is safe.

Every manifest is a full snapshot of the bucket, not a delta. Pin a specific
`manifest.json` for a backfill so that its reruns see the same parts.

The feeder never reads the SAFE bucket itself; only the task does.

## Payload

| Field | Required | Default | Notes |
|---|---|---|---|
| `inventory_uri` | yes | | One inventory data part: `s3://<inventory-bucket>/<source-bucket>/<config-id>/data/<uuid>.parquet`. Payload only; there is no env fallback. |
| `upload_bucket` | yes | | Where the workflow writes the item and **every COG**. Use the workflow's `sentinel_data_bucket` from earthsearch-infra; the task role can't write elsewhere. |
| `min_processing_baseline` | no | `05.00` | String comparison on `"NN.NN"`. |
| `max_processing_baseline` | no | `05.99` | |
| `exclude_processing_baseline` | no | `05.09` | |
| `limit` | no | none | Max SAFEs queued from this part, applied after the filters, in key order. Falls back to the `LIMIT` env var. |
| `batch_size` | no | `10` | Cirrus payloads per `SendMessageBatch` call. Not validated; SQS rejects more than 10. |

`lambda_handler` accepts the payload directly or wrapped in an SQS event source
mapping envelope.

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `SQS_QUEUE_URL` | unset | Cirrus process queue. When unset, payloads are only formatted (and logged at DEBUG), not sent. |
| `WORKFLOW_NAME` | `sentinel-2-c1-l2a` | Cirrus workflow; also the prefix of the payload `id`. |
| `LIMIT` | unset | Fallback for the payload's `limit`. |
| `LOG_LEVEL` | `INFO` | `DEBUG` logs every payload. |
| `CHUNK_SIZE` | `5000` | Rows per streamed polars chunk. |
| `SQS_CONCURRENCY` | `64` | In-flight `SendMessageBatch` calls. |
| `LOG_EVERY_N_BATCHES` | `1000` | Batches between progress log lines; `0` disables them. |

## Submitting a manifest

```
scripts/submit_safe_payloads.py \
    --manifest <manifest.json path or s3:// URI> \
    --upload-bucket <upload-bucket> \
    --queue-url <feeder trigger queue URL>
```

The script prints the manifest's `creationTimestamp` and `sourceBucket` to
stderr, and exits before queuing anything unless `fileFormat` is `Parquet`.
`--queue-url` may also come from `FEEDER_QUEUE_URL`. The baseline options,
`--limit` and `--batch-size` are copied into every payload when given and
omitted otherwise, so the lambda's defaults apply.

`--dry-run` prints the payloads to stdout as JSON lines instead of sending
them. With a local `--manifest`, a dry run makes no AWS calls.

To queue only some parts (e.g. a pilot), pass a local copy of the manifest with
`files[]` cut down.

## Checking a run

Each invocation logs `Scanning inventory part: <uri>` and, on success, ends with
`Queued <N> total Cirrus payloads.` Long parts also log
`Queued <b> batches (<n> payloads)` every `LOG_EVERY_N_BATCHES` batches.

- `SQS rejected <n> of <m> messages` (logged, not raised): those payloads weren't
  queued. Resubmit that part (a manifest cut down to it); Cirrus skips what
  already ran.
- An invocation that raises is retried through SQS redelivery of its trigger
  message, up to the trigger queue's `maxReceiveCount`, then goes to its
  dead-letter queue if one is configured.

## Tests

From the repo root:

```zsh
uv run --package s2-safe-feeder pytest lambdas/s2-safe-feeder
```

If importing stac-task fails on your AWS config (e.g. `botocore[crt]` under SSO),
run with dummy credentials and no profile:

```zsh
env -u AWS_PROFILE AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null \
  AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy AWS_DEFAULT_REGION=us-west-2 \
  uv run --package s2-safe-feeder pytest lambdas/s2-safe-feeder
```

Fixtures: a synthetic inventory part (`tests/fixtures/inventory/make_part.py`
regenerates it) and real Earth Search STAC GeoParquet rows for the ID-rule
check.

Not covered by the automated tests:

- the submit script (check it with `--dry-run`)
- anything needing AWS: real S3 reads and IAM, the Docker image, the task itself
- the contract between the env vars this code reads and the ones the infra
  feeder definition sets; check it by hand when either side changes

See [DEVELOPMENT.md](../../DEVELOPMENT.md) at the repo root for more information
on this monorepo.
