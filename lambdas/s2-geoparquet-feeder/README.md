# s2-geoparquet-feeder

Scans a single Sentinel-2 STAC geoparquet file in S3, filters items by
processing baseline, and queues Cirrus workflow payloads to SQS so each item's
granule metadata can be converted into a STAC item.

One invocation handles one geoparquet part file, so the caller is responsible
for listing the part files under a prefix and invoking the lambda once per
file. This keeps each invocation's runtime bounded and lets the files be
processed in parallel. It also avoids a multi-file `scan_parquet`, which
decodes a row group of the large nested `assets` column from every part file
concurrently (~20x slower when a limit means most files emit no rows).

Payload:

```json
{
  "upload_bucket": "my-upload-bucket",
  "geoparquet_uri": "s3://my-bucket/sentinel-2-c1-l2a/part-00000.parquet",
  "min_processing_baseline": "05.00",
  "max_processing_baseline": "05.99",
  "exclude_processing_baseline": "05.09",
  "limit": 1000,
  "batch_size": 10
}
```

Only `upload_bucket` and `geoparquet_uri` are required; `geoparquet_uri` and
`limit` may also come from the `GEOPARQUET_URI` and `LIMIT` environment
variables. `lambda_handler` accepts the payload directly or wrapped in an SQS
event source mapping envelope.

## Submitting a whole prefix

`scripts/submit_geoparquet_payloads.py` lists the part files under a prefix,
builds one payload each, and sends them to the queue that triggers the lambda:

```
scripts/submit_geoparquet_payloads.py \
    --prefix s3://my-bucket/sentinel-2-c1-l2a/ \
    --upload-bucket my-upload-bucket \
    --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/s2-geoparquet-feeder
```

Pass `--dry-run` to print the payloads instead of sending them. The baseline,
`--limit`, and `--batch-size` options are forwarded into every payload when
given, and omitted otherwise so the task's defaults apply.

See [DEVELOPMENT.md](../../DEVELOPMENT.md) at the repo root for more information
on this monorepo.
