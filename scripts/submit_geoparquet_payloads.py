#!/usr/bin/env -S uv run --script
"""Queue one s2-geoparquet-feeder payload per geoparquet part file under an S3 prefix.

The feeder lambda handles exactly one part file per invocation, so this lists the
prefix and sends one SQS message per `.parquet` object to the queue that triggers it.

Usage:
  scripts/submit_geoparquet_payloads.py \
      --prefix s3://my-bucket/sentinel-2-c1-l2a/ \
      --upload-bucket my-upload-bucket \
      --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/s2-geoparquet-feeder
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


def list_parquet_uris(prefix_uri: str) -> list[str]:
    parsed = urlparse(prefix_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        sys.exit(f"--prefix must be an s3:// URI, got {prefix_uri!r}")

    s3 = boto3.client("s3")
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=parsed.netloc, Prefix=parsed.path.lstrip("/")
    )
    return [
        f"s3://{parsed.netloc}/{obj['Key']}"
        for page in pages
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]


def build_payload(geoparquet_uri: str, args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "upload_bucket": args.upload_bucket,
        "geoparquet_uri": geoparquet_uri,
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
        "--prefix", required=True, help="s3:// prefix holding the geoparquet part files"
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
        "--limit", type=int, help="Max items each invocation queues from its part file"
    )
    parser.add_argument(
        "--batch-size", type=int, help="SQS batch size used by the task"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print the payloads instead of sending"
    )

    args = parser.parse_args()
    if not args.queue_url and not args.dry_run:
        parser.error("--queue-url is required (or set FEEDER_QUEUE_URL)")
    return args


def main() -> None:
    args = parse_args()

    uris = list_parquet_uris(args.prefix)
    if not uris:
        sys.exit(f"No .parquet objects found under {args.prefix}")
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
