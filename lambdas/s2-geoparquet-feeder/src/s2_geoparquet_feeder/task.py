import asyncio
import logging
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from itertools import batched
from typing import Any

import orjson as json
import polars as pl
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from stactask import Task

logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Rows buffered per streamed polars chunk.
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))
SQS_BATCH_SIZE = 10
# In-flight SendMessageBatch calls. Past ~64 the send loop is bound by botocore's
# per-request CPU (signing and serialization), not by round-trip latency.
SQS_CONCURRENCY = int(os.getenv("SQS_CONCURRENCY", "64"))
# Batches between progress log lines; 0 disables them.
LOG_EVERY_N_BATCHES = int(os.getenv("LOG_EVERY_N_BATCHES", "1000"))
WORKFLOW_NAME = os.getenv("WORKFLOW_NAME", "sentinel-2-c1-l2a")

queue_url = os.getenv("SQS_QUEUE_URL")


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
    geoparquet_uri: str,
    min_processing_baseline: str | None,
    max_processing_baseline: str | None,
    exclude_processing_baseline: str | None,
    limit: int | None,
) -> Iterator[pl.DataFrame]:
    """Yields record chunks streamed from a single geoparquet part file."""
    records_df = build_records_frame(
        geoparquet_uri,
        min_processing_baseline,
        max_processing_baseline,
        exclude_processing_baseline,
        limit,
    )
    return iter(records_df.collect_batches(engine="streaming", chunk_size=CHUNK_SIZE))


def frame_to_entry_batches(
    frame: pl.DataFrame, upload_bucket: str, batch_size: int
) -> list[list[dict[str, str]]]:
    """Encodes one streamed chunk into SendMessageBatch entries."""
    groups = []
    for group in batched(frame.iter_rows(named=True), batch_size):
        entries = []
        for record in group:
            # orjson.dumps returns bytes; SQS MessageBody requires str
            body = json.dumps(format_cirrus_payload(record, upload_bucket)).decode(
                "utf-8"
            )
            # %s defers formatting; an f-string here costs ~15% of the encode phase.
            logging.debug("Submitting payload: %s", body)
            entries.append({"Id": str(uuid.uuid4()), "MessageBody": body})
        groups.append(entries)
    return groups


async def iter_entry_batches(
    frames: Iterator[pl.DataFrame],
    upload_bucket: str,
    batch_size: int,
) -> AsyncIterator[list[dict[str, str]]]:
    """Yields entry batches, decoding and encoding each chunk off the event loop."""

    def next_chunk() -> list[list[dict[str, str]]] | None:
        frame = next(frames, None)
        if frame is None:
            return None
        return frame_to_entry_batches(frame, upload_bucket, batch_size)

    while (groups := await asyncio.to_thread(next_chunk)) is not None:
        for entries in groups:
            yield entries


@asynccontextmanager
async def sqs_client() -> AsyncIterator[Any]:
    """Yields None when SQS_QUEUE_URL is unset, so local runs only format payloads."""
    if not queue_url:
        yield None
        return

    async with get_session().create_client(
        "sqs",
        config=AioConfig(
            max_pool_connections=SQS_CONCURRENCY,
            retries={"max_attempts": 10, "mode": "standard"},
        ),
    ) as client:
        yield client


async def send_entries(client: Any, entries: list[dict[str, str]]) -> None:
    if client is None:
        return
    response = await client.send_message_batch(QueueUrl=queue_url, Entries=entries)
    failed = response.get("Failed")
    if failed:
        logging.error(
            f"SQS rejected {len(failed)} of {len(entries)} messages: {failed}"
        )


async def send_all(
    frames: Iterator[pl.DataFrame], upload_bucket: str, batch_size: int
) -> None:
    total = 0
    b = 0
    pending: set[asyncio.Task[None]] = set()

    async with sqs_client() as client:
        async for entries in iter_entry_batches(frames, upload_bucket, batch_size):
            b += 1
            total += len(entries)

            if len(pending) >= SQS_CONCURRENCY:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    task.result()

            pending.add(asyncio.create_task(send_entries(client, entries)))
            if LOG_EVERY_N_BATCHES and (b % LOG_EVERY_N_BATCHES) == 0:
                logging.info(f"Queued {b} batches ({total} payloads)")

        if pending:
            await asyncio.gather(*pending)

    logging.info(f"Queued {total} total Cirrus payloads.")


def queue_payloads(
    frames: Iterator[pl.DataFrame],
    upload_bucket: str,
    batch_size: int = SQS_BATCH_SIZE,
) -> None:
    """Sends batches concurrently, keeping at most SQS_CONCURRENCY in flight."""
    asyncio.run(send_all(frames, upload_bucket, batch_size))


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
        "Scans a single Sentinel-2 STAC geoparquet file in S3, filters by "
        "processing baseline, and queues Cirrus workflow payloads to SQS."
    )

    def validate(self) -> bool:
        return True

    def process(self, **kwargs: Any) -> list[dict[str, Any]]:
        upload_bucket = self.payload["upload_bucket"]
        limit = self.payload.get("limit", os.getenv("LIMIT"))
        if limit is not None:
            limit = int(limit)

        geoparquet_uri = self.payload.get("geoparquet_uri", os.getenv("GEOPARQUET_URI"))
        if not geoparquet_uri:
            raise ValueError("geoparquet_uri is required")

        min_processing_baseline = self.payload.get("min_processing_baseline", "05.00")
        max_processing_baseline = self.payload.get("max_processing_baseline", "05.99")
        exclude_processing_baseline = self.payload.get(
            "exclude_processing_baseline", "05.09"
        )

        self.logger.info(f"Scanning geoparquet: {geoparquet_uri}")
        frames = get_records_from_geoparquet(
            geoparquet_uri,
            min_processing_baseline,
            max_processing_baseline,
            exclude_processing_baseline,
            limit,
        )

        batch_size = self.payload.get("batch_size", SQS_BATCH_SIZE)
        queue_payloads(frames, upload_bucket, batch_size=batch_size)

        return []


def lambda_handler(event: dict[str, Any], context: Any = None) -> Any:
    """Accepts a payload directly or wrapped in an SQS event source mapping envelope."""
    if "Records" in event:
        return [
            S2GeoparquetFeeder.handler(payload=json.loads(record["body"]))
            for record in event["Records"]
        ]
    return S2GeoparquetFeeder.handler(payload=event)


if __name__ == "__main__":
    S2GeoparquetFeeder.cli()
