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

# Every SAFE has exactly one DATASTRIP/DS_*/MTD_DS.xml, so matching only that key
# yields one row per SAFE, even when the SAFE's other objects spill into another
# inventory part.
SAFE_DATASTRIP_KEY = (
    r"^(?P<safe_key>.+\.SAFE)/DATASTRIP/(?P<datastrip>DS_[^/]+)/MTD_DS\.xml$"
)


def scan_inventory(inventory_uri: str) -> pl.LazyFrame:
    """Reduces one S3 Inventory data part to one row per SAFE.

    Columns: bucket, safe_key (object key through `<name>.SAFE`) and datastrip (the
    DATASTRIP folder name).
    """
    return (
        pl.scan_parquet(inventory_uri)
        .select("bucket", "key")
        # Cheap literal check first, so the regex below runs on ~1% of the rows.
        .filter(pl.col("key").str.ends_with("/MTD_DS.xml"))
        .select(
            "bucket",
            pl.col("key").str.extract_groups(SAFE_DATASTRIP_KEY).alias("match"),
        )
        .unnest("match")
        .drop_nulls("safe_key")
    )


def processing_baseline() -> pl.Expr:
    """`_N0510_` in safe_name -> "05.10"; null when the name has no such field."""
    digits = pl.col("safe_name").str.extract(r"_N(\d{4})_", 1)
    return pl.concat_str(
        [digits.str.slice(0, 2), digits.str.slice(2, 2)], separator="."
    )


def item_id() -> pl.Expr:
    """Earth Search item ID from safe_name and datastrip, as the task builds it.

    The time is the datastrip's sensing start, not the SAFE name's, so the SAFE name
    alone isn't enough. datastrip may be the DATASTRIP folder name or the full
    `s2:datastrip_id`; both carry the time as `_S<YYYYMMDDTHHMMSS>`. The level is
    always L2A: the DATASTRIP folder name doesn't carry it, and build_records_frame
    keeps only L2A SAFEs. Null when any part is missing.
    """
    safe_name = pl.col("safe_name")
    return pl.concat_str(
        [
            safe_name.str.slice(0, 3),
            safe_name.str.extract(r"_(T\d{2}[A-Z]{3})_", 1),
            pl.col("datastrip").str.extract(r"_S(\d{8}T\d{6})", 1),
            pl.lit("L2A"),
        ],
        separator="_",
    )


def with_safe_fields(safes: pl.LazyFrame) -> pl.LazyFrame:
    """Adds safe_href, safe_name, processing_baseline and id to scan_inventory rows."""
    return safes.with_columns(
        pl.concat_str([pl.lit("s3://"), "bucket", pl.lit("/"), "safe_key"]).alias(
            "safe_href"
        ),
        pl.col("safe_key").str.extract(r"([^/]+)$", 1).alias("safe_name"),
    ).with_columns(
        processing_baseline().alias("processing_baseline"),
        item_id().alias("id"),
    )


def build_records_frame(
    inventory_uri: str,
    min_processing_baseline: str | None,
    max_processing_baseline: str | None,
    exclude_processing_baseline: str | None,
    limit: int | None,
) -> pl.LazyFrame:
    """Plans the id + safe_href records to queue from one inventory part."""
    df = with_safe_fields(scan_inventory(inventory_uri))

    # An unparseable SAFE name or DATASTRIP folder leaves a null baseline or id. Nothing
    # later removes those rows: the baseline filters are each optional, and none applies
    # to id.
    df = df.drop_nulls(["processing_baseline", "id"])

    # Only L2A products. Any other product (e.g. MSIL1C) gets the same item ID as its
    # L2A scene; the task rejects it as invalid, and Cirrus then skips that payload ID
    # on every later submission, so the real L2A SAFE would never be processed.
    df = df.filter(pl.col("safe_name").str.contains("_MSIL2A_", literal=True))

    if min_processing_baseline:
        df = df.filter(pl.col("processing_baseline") >= min_processing_baseline)
    if max_processing_baseline:
        df = df.filter(pl.col("processing_baseline") <= max_processing_baseline)
    if exclude_processing_baseline:
        df = df.filter(pl.col("processing_baseline") != exclude_processing_baseline)

    records_df = df.select(["id", "safe_href"])
    if limit is not None:
        records_df = records_df.head(limit)

    return records_df


def get_records_from_inventory(
    inventory_uri: str,
    min_processing_baseline: str | None,
    max_processing_baseline: str | None,
    exclude_processing_baseline: str | None,
    limit: int | None,
) -> Iterator[pl.DataFrame]:
    """Yields record chunks streamed from a single inventory part."""
    records_df = build_records_frame(
        inventory_uri,
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
    """Wraps one SAFE record (id + safe_href) into a Cirrus payload."""
    return {
        # Same id format as s2-geoparquet-feeder, so Cirrus skips a scene either feeder
        # already completed.
        "id": f"{WORKFLOW_NAME}/workflow-{WORKFLOW_NAME}/{record['id']}",
        "safe_href": record["safe_href"],
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


class S2SafeFeeder(Task):
    name = "s2-safe-feeder"
    description = (
        "Reads one S3 Inventory part of the Sentinel-2 SAFE bucket, keeps one row "
        "per SAFE, filters by processing baseline, and queues Cirrus workflow "
        "payloads with safe_href to SQS."
    )

    def validate(self) -> bool:
        return True

    def process(self, **kwargs: Any) -> list[dict[str, Any]]:
        upload_bucket = self.payload.get("upload_bucket")
        if not upload_bucket:
            raise ValueError("upload_bucket is required")
        inventory_uri = self.payload.get("inventory_uri")
        if not inventory_uri:
            raise ValueError("inventory_uri is required")

        limit = self.payload.get("limit", os.getenv("LIMIT"))
        if limit is not None:
            limit = int(limit)

        min_processing_baseline = self.payload.get("min_processing_baseline", "05.00")
        max_processing_baseline = self.payload.get("max_processing_baseline", "05.99")
        exclude_processing_baseline = self.payload.get(
            "exclude_processing_baseline", "05.09"
        )

        self.logger.info(f"Scanning inventory part: {inventory_uri}")
        frames = get_records_from_inventory(
            inventory_uri,
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
            S2SafeFeeder.handler(payload=json.loads(record["body"]))
            for record in event["Records"]
        ]
    return S2SafeFeeder.handler(payload=event)


if __name__ == "__main__":
    S2SafeFeeder.cli()
