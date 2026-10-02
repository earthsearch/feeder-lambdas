import logging
import os
import uuid
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from itertools import batched
from typing import Any
from urllib.parse import urlparse

import boto3
import orjson as json
import polars as pl
from botocore.config import Config
from stactask import Task

logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Rows buffered per streamed polars chunk.
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))
SQS_BATCH_SIZE = 10
# In-flight SendMessageBatch calls. Sending serially is bound by round-trip
# latency (~13ms), not by SQS throughput.
SQS_CONCURRENCY = int(os.getenv("SQS_CONCURRENCY", "32"))
WORKFLOW_NAME = os.getenv("WORKFLOW_NAME", "sentinel-2-c1-l2a")

queue_url = os.getenv("SQS_QUEUE_URL")
# A client rather than a resource: boto3 resources are not thread safe.
sqs = (
    None
    if not queue_url
    else boto3.client(
        "sqs",
        config=Config(
            max_pool_connections=SQS_CONCURRENCY,
            retries={"max_attempts": 10, "mode": "standard"},
        ),
    )
)


def get_parquet_uris_from_prefix(prefix_uri: str) -> list[str]:
    parsed = urlparse(prefix_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    logging.info(f"Listing geoparquet files under s3://{bucket}/{prefix}...")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    return [
        f"s3://{bucket}/{obj['Key']}"
        for page in pages
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]


def build_records_frame(
    geoparquet_uri: str,
    min_processing_baseline: str | None,
    max_processing_baseline: str | None,
    exclude_processing_baseline: str | None,
    limit: int | None,
) -> pl.LazyFrame:
    """Scans STAC geoparquet, resolving each granule_metadata asset to an S3 URI."""
    df = pl.scan_parquet(geoparquet_uri)

    if min_processing_baseline:
        df = df.filter(pl.col("s2:processing_baseline") >= min_processing_baseline)
    if max_processing_baseline:
        df = df.filter(pl.col("s2:processing_baseline") <= max_processing_baseline)
    if exclude_processing_baseline:
        df = df.filter(pl.col("s2:processing_baseline") != exclude_processing_baseline)

    df = df.with_columns(
        pl.col("assets")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "granule_metadata")
            .struct.field("value")
            .struct.field("href")
        )
        .list.first()
        .alias("metadata_href")
    )
    df = df.with_columns(
        pl.col("metadata_href")
        .str.replace(r"https://([^.]+)\.s3[^/]*\.amazonaws\.com/", "s3://${1}/")
        .alias("metadata_s3_uri")
    )

    records_df = df.select(["id", "metadata_s3_uri"])
    if limit is not None:
        records_df = records_df.head(limit)

    return records_df


def get_records_from_geoparquet(
    geoparquet_uris: list[str],
    min_processing_baseline: str | None,
    max_processing_baseline: str | None,
    exclude_processing_baseline: str | None,
    limit: int | None,
) -> Iterator[pl.DataFrame]:
    """Yields record chunks, scanning one part file at a time.

    A single multi-file `scan_parquet` decodes a row group of the large nested
    `assets` column from every part file concurrently, so the pushed-down slice
    still pays the decode cost of files it never emits rows from (~20x slower
    for a 50k limit over 60 part files).
    """
    remaining = limit
    for uri in geoparquet_uris:
        if remaining is not None and remaining <= 0:
            return

        records_df = build_records_frame(
            uri,
            min_processing_baseline,
            max_processing_baseline,
            exclude_processing_baseline,
            remaining,
        )
        for frame in records_df.collect_batches(
            engine="streaming", chunk_size=CHUNK_SIZE, lazy=True
        ):
            yield frame
            if remaining is not None:
                remaining -= frame.height


def iter_sqs_entries(
    frames: Iterator[pl.DataFrame],
    upload_bucket: str,
    batch_size: int,
) -> Iterator[list[dict[str, str]]]:
    """Yields SendMessageBatch entries one streamed chunk at a time."""
    for frame in frames:
        for group in batched(frame.iter_rows(named=True), batch_size):
            entries = []
            for record in group:
                # orjson.dumps returns bytes; SQS MessageBody requires str
                body = json.dumps(format_cirrus_payload(record, upload_bucket)).decode(
                    "utf-8"
                )
                logging.debug(f"Submitting payload: {body}")
                entries.append({"Id": str(uuid.uuid4()), "MessageBody": body})
            yield entries


def send_entries(entries: list[dict[str, str]]) -> None:
    if not sqs:
        return
    failed = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries).get("Failed")
    if failed:
        logging.error(
            f"SQS rejected {len(failed)} of {len(entries)} messages: {failed}"
        )


def queue_payloads(
    frames: Iterator[pl.DataFrame],
    upload_bucket: str,
    batch_size: int = SQS_BATCH_SIZE,
) -> None:
    """Sends batches concurrently, keeping at most SQS_CONCURRENCY in flight."""
    total = 0
    b = 0
    pending: set[Future[None]] = set()

    with ThreadPoolExecutor(max_workers=SQS_CONCURRENCY) as pool:
        for entries in iter_sqs_entries(frames, upload_bucket, batch_size):
            b += 1
            total += len(entries)

            if len(pending) >= SQS_CONCURRENCY:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()

            pending.add(pool.submit(send_entries, entries))
            if (b % 100) == 0:
                logging.info(f"Queued {b} batches")

        for future in pending:
            future.result()

    logging.info(f"Queued {total} total Cirrus payloads.")


def format_cirrus_payload(record: dict[str, Any], upload_bucket: str) -> dict[str, Any]:
    """Wraps a STAC geoparquet row (id + metadata_s3_uri) into a Cirrus payload."""
    return {
        "id": f"{WORKFLOW_NAME}/workflow-{WORKFLOW_NAME}/{record['id']}",
        "metadata_href": record["metadata_s3_uri"],
        "process": [
            {
                "workflow": WORKFLOW_NAME,
                "upload_options": {
                    "path_template": f"s3://{upload_bucket}/${{collection}}/${{mgrs:utm_zone}}/${{mgrs:latitude_band}}/${{mgrs:grid_square}}/${{year}}/${{month}}/${{id}}",
                    "collections": {"sentinel-2-l2a": "$[?(@.id =~ '.*')]"},
                },
                "tasks": {"sentinel-2-l2a-to-stac": {}},
            }
        ],
    }


class S2GeoparquetFeeder(Task):
    name = "s2-geoparquet-feeder"
    description = (
        "Scans Sentinel-2 STAC geoparquet in S3, filters by processing "
        "baseline, and queues Cirrus workflow payloads to SQS."
    )

    def validate(self) -> bool:
        return True

    def process(self, **kwargs: Any) -> list[dict[str, Any]]:
        upload_bucket = self.payload["upload_bucket"]
        limit = self.payload.get("limit", os.getenv("LIMIT"))
        if limit is not None:
            limit = int(limit)

        geoparquet_prefix = self.payload.get(
            "geoparquet_prefix", os.getenv("GEOPARQUET_PREFIX")
        )
        if not geoparquet_prefix:
            raise ValueError("geoparquet_prefix is required")

        min_processing_baseline = self.payload.get("min_processing_baseline", "05.00")
        max_processing_baseline = self.payload.get("max_processing_baseline", "05.99")
        exclude_processing_baseline = self.payload.get(
            "exclude_processing_baseline", "05.09"
        )

        self.logger.info(f"Scanning geoparquet under: {geoparquet_prefix}")
        geoparquet_uris = get_parquet_uris_from_prefix(geoparquet_prefix)
        frames = get_records_from_geoparquet(
            geoparquet_uris,
            min_processing_baseline,
            max_processing_baseline,
            exclude_processing_baseline,
            limit,
        )

        batch_size = self.payload.get("batch_size", SQS_BATCH_SIZE)
        queue_payloads(frames, upload_bucket, batch_size=batch_size)

        return []


def lambda_handler(event: dict[str, Any], context: Any = None) -> Any:
    return S2GeoparquetFeeder.handler(payload=event)


if __name__ == "__main__":
    S2GeoparquetFeeder.cli()
