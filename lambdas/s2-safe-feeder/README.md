# s2-safe-feeder

A manually triggered backfill: there is no schedule and no S3 event. Each
invocation reads one part of the S3 Inventory of the Sentinel-2 SAFE bucket and
queues a Cirrus workflow payload with `safe_href` for each L2A `.SAFE` it lists,
so the `sentinel-2-l2a-to-stac` task builds the STAC item and COGs from the raw
ESA archive.

Resubmitting a manifest is safe. The Cirrus payload ID is built from the Earth
Search item ID, and Cirrus skips any ID that already succeeded, is still
PROCESSING, or was rejected as INVALID. It reruns only IDs that never ran, last
FAILED, were ABORTED, or are stuck CLAIMED.

## Run a backfill

1. **Pin one `manifest.json`.** Every manifest is a full snapshot of the
   bucket, not a delta, so reruns of a backfill must use the same manifest to
   see the same parts.
2. **Dry-run it.** `--dry-run` prints the payloads to stdout as JSON lines
   instead of sending them. With a local `--manifest`, it makes no AWS calls.
3. **Submit it:**

   ```
   scripts/submit_safe_payloads.py \
       --manifest <manifest.json path or s3:// URI> \
       --upload-bucket <upload-bucket> \
       --queue-url <feeder trigger queue URL>
   ```

The script sends one feeder payload per data part in the manifest's `files[]`
to the queue that triggers this lambda. `--upload-bucket` is where the workflow
writes the item and **every COG**: use the workflow's `sentinel_data_bucket`
from earthsearch-infra, because the task role can't write elsewhere.

The script prints the manifest's `creationTimestamp` and `sourceBucket` to
stderr, and exits before queuing anything unless `fileFormat` is `Parquet`.
`--queue-url` may also come from `FEEDER_QUEUE_URL`. The baseline options,
`--limit` and `--batch-size` are copied into every payload when given and
omitted otherwise, so the lambda's defaults apply.

To queue only some parts (e.g. a pilot), pass a local copy of the manifest with
`files[]` cut down.

## Check a run

Each invocation logs `Scanning inventory part: <uri>` and, on success, ends with
`Queued <N> total Cirrus payloads.` Long parts also log
`Queued <b> batches (<n> payloads)` every `LOG_EVERY_N_BATCHES` batches.

- `SQS rejected <n> of <m> messages` (logged, not raised): those payloads weren't
  queued. Resubmit that part (a manifest cut down to it); Cirrus skips what
  already ran.
- An invocation that raises is retried through SQS redelivery of its trigger
  message, up to the trigger queue's `maxReceiveCount`, then goes to its
  dead-letter queue if one is configured.

## What one invocation queues

Each invocation handles exactly one inventory part:

1. Keep one row per SAFE: its `DATASTRIP/DS_*/MTD_DS.xml` key. A SAFE split
   across two parts is therefore queued by exactly one of them, and a SAFE
   without that key is never queued.
2. Derive `safe_href` (`s3://<bucket>/<path>/<name>.SAFE`), the processing
   baseline, and the Earth Search item ID (the task's rule).
3. Drop non-L2A SAFEs and SAFEs it can't parse.
4. Apply the baseline filters, then `limit`.
5. Send one Cirrus payload per remaining SAFE to `SQS_QUEUE_URL` (the Cirrus
   process queue).

Only L2A SAFEs are queued because any other product would get its L2A scene's
item ID. Once the task rejects it as INVALID, Cirrus skips that ID for good, and
the real L2A scene would never be processed.

The feeder never reads the SAFE bucket itself; only the task does.

## Payload

`lambda_handler` accepts the payload directly or wrapped in an SQS event source
mapping envelope.

| Field | Required | Default | Notes |
|---|---|---|---|
| `inventory_uri` | yes | | One inventory data part: `s3://<inventory-bucket>/<source-bucket>/<config-id>/data/<uuid>.parquet`. Payload only; there is no env fallback. |
| `upload_bucket` | yes | | Where the workflow writes the item and **every COG**. Use the workflow's `sentinel_data_bucket` from earthsearch-infra; the task role can't write elsewhere. |
| `min_processing_baseline` | no | `05.00` | String comparison on `"NN.NN"`. |
| `max_processing_baseline` | no | `05.99` | |
| `exclude_processing_baseline` | no | `05.09` | |
| `limit` | no | none | Max SAFEs queued from this part, applied after the filters, in key order. Falls back to the `LIMIT` env var. |
| `batch_size` | no | `10` | Cirrus payloads per `SendMessageBatch` call. Not validated; SQS rejects more than 10. |

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
