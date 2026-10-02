# s2-geoparquet-feeder

Scans Sentinel-2 STAC geoparquet files in S3, filters items by processing
baseline, and queues Cirrus workflow payloads to SQS so each item's granule
metadata can be converted into a STAC item. See
[DEVELOPMENT.md](../../DEVELOPMENT.md) at the repo root for more information
on this monorepo.
