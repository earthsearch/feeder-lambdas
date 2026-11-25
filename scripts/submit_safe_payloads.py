#!/usr/bin/env -S uv run --script
"""Queue one s2-safe-feeder payload per data part listed in an S3 Inventory manifest.

The feeder lambda handles exactly one inventory part per invocation, so this reads the
manifest and sends one SQS message per `files[]` entry to the queue that triggers it.
Each manifest is a full snapshot of the SAFE bucket; to queue a subset (e.g. a pilot),
pass a copy of the manifest with `files[]` cut down. S3 Inventory writes each manifest
to `s3://<inventory-bucket>/<source-bucket>/<config-id>/<date>/manifest.json`.

Usage:
  scripts/submit_safe_payloads.py \
      --manifest <manifest.json path or s3:// URI> \
      --upload-bucket my-upload-bucket \
      --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/s2-safe-feeder
"""

# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.40.0"]
# ///

import argparse
import json
import os
import sys
import uuid
from datetime import UTC, datetime
from itertools import batched
from typing import Any
from urllib.parse import urlparse

import boto3

SQS_BATCH_SIZE = 10

# Payload keys forwarded to the lambda only when given on the command line, so the
# lambda's own defaults apply otherwise.
OPTIONAL_KEYS = (
    "min_processing_baseline",
    "max_processing_baseline",
    "exclude_processing_baseline",
    "limit",
    "batch_size",
)


def load_manifest(manifest: str) -> dict[str, Any]:
    """Reads an S3 Inventory manifest.json from an s3:// URI or a local path."""
    parsed = urlparse(manifest)
    if parsed.scheme == "s3":
        body = boto3.client("s3").get_object(
            Bucket=parsed.netloc, Key=parsed.path.lstrip("/")
        )["Body"]
        result: dict[str, Any] = json.loads(body.read())
        return result
    with open(manifest) as f:
        result = json.load(f)
        return result


def inventory_uris(manifest: dict[str, Any]) -> list[str]:
    """s3:// URI of every data part, in manifest order.

    `files[].key` is relative to the destination bucket, which the manifest gives as
    an ARN.
    """
    bucket = manifest["destinationBucket"].removeprefix("arn:aws:s3:::")
    return [f"s3://{bucket}/{entry['key']}" for entry in manifest["files"]]


def build_payload(inventory_uri: str, args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "upload_bucket": args.upload_bucket,
        "inventory_uri": inventory_uri,
    }
    for key in OPTIONAL_KEYS:
        value = getattr(args, key)
        if value is not None:
            payload[key] = value
    return payload


def send_payloads(queue_url: str, payloads: list[dict[str, Any]]) -> None:
    sqs = boto3.client("sqs")
    for group in batched(payloads, SQS_BATCH_SIZE):
        entries = [
            {"Id": str(uuid.uuid4()), "MessageBody": json.dumps(payload)}
            for payload in group
        ]
        failed = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries).get(
            "Failed"
        )
        if failed:
            sys.exit(f"SQS rejected {len(failed)} of {len(entries)} messages: {failed}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help="S3 Inventory manifest.json of the SAFE bucket (s3:// URI or local path)",
    )
    parser.add_argument(
        "--upload-bucket",
        required=True,
        help="Bucket the Cirrus workflow uploads STAC assets to",
    )
    parser.add_argument(
        "--queue-url",
        default=os.getenv("FEEDER_QUEUE_URL"),
        help="SQS queue that triggers the feeder lambda (env: FEEDER_QUEUE_URL)",
    )
    parser.add_argument("--min-processing-baseline")
    parser.add_argument("--max-processing-baseline")
    parser.add_argument("--exclude-processing-baseline")
    parser.add_argument(
        "--limit", type=int, help="Max SAFEs each invocation queues from its part"
    )
    parser.add_argument(
        "--batch-size", type=int, help="SQS batch size used by the task (1-10)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print the payloads instead of sending"
    )

    args = parser.parse_args()
    if not args.queue_url and not args.dry_run:
        parser.error("--queue-url is required (or set FEEDER_QUEUE_URL)")
    # The feeder doesn't validate these. A batch size SQS rejects would fail every
    # invocation until each trigger message reaches the dead-letter queue.
    if args.batch_size is not None and not 1 <= args.batch_size <= SQS_BATCH_SIZE:
        parser.error(f"--batch-size must be between 1 and {SQS_BATCH_SIZE}")
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be 0 or more")
    return args


def main() -> None:
    args = parse_args()

    manifest = load_manifest(args.manifest)
    # The feeder only reads parquet; fail once here rather than in every invocation.
    if manifest.get("fileFormat") != "Parquet":
        sys.exit(f"Expected a Parquet inventory, got {manifest.get('fileFormat')!r}")

    # Printed on every run so the operator can confirm which snapshot is being queued.
    # stderr keeps dry-run stdout to payload lines only.
    created_ms = manifest["creationTimestamp"]
    created = datetime.fromtimestamp(int(created_ms) / 1000, UTC).isoformat()
    print(f"Manifest creationTimestamp: {created_ms} ({created})", file=sys.stderr)
    print(f"Manifest sourceBucket: {manifest['sourceBucket']}", file=sys.stderr)

    uris = inventory_uris(manifest)
    if not uris:
        sys.exit(f"No data files listed in {args.manifest}")
    payloads = [build_payload(uri, args) for uri in uris]

    if args.dry_run:
        for payload in payloads:
            print(json.dumps(payload))
        print(f"Dry run: {len(payloads)} payloads not sent.", file=sys.stderr)
        return

    send_payloads(args.queue_url, payloads)
    print(f"Queued {len(payloads)} payloads to {args.queue_url}")


if __name__ == "__main__":
    main()
