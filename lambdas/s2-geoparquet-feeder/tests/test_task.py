import json
import os
import threading
from pathlib import Path
from typing import Any

import boto3
import polars as pl
import pytest
from moto import mock_aws
from s2_geoparquet_feeder import task as task_module
from s2_geoparquet_feeder.task import (
    S2GeoparquetFeeder,
    format_cirrus_payload,
    get_parquet_uris_from_prefix,
    get_records_from_geoparquet,
    queue_payloads,
)

# 10 rows sampled from a real sentinel-2-c1-l2a STAC geoparquet part file, with the
# first row's baseline lowered to 04.00 so every baseline filter branch is exercised.
GEOPARQUET_PATH = str(
    Path(__file__).parent / "fixtures" / "geoparquet" / "items.parquet"
)

# The rows of that fixture surviving the default filters (>=05.00, <=05.99, !=05.09).
EXPECTED_RECORDS = [
    {
        "id": "S2B_T56JRT_20191222T235240_L2A",
        "metadata_s3_uri": "s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/56/J/RT/2019/12/S2B_T56JRT_20191222T235240_L2A/metadata.xml",
    },
    {
        "id": "S2B_T37WFP_20200724T090556_L2A",
        "metadata_s3_uri": "s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/37/W/FP/2020/7/S2B_T37WFP_20200724T090556_L2A/metadata.xml",
    },
    {
        "id": "S2B_T11UNQ_20240119T185717_L2A",
        "metadata_s3_uri": "s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/11/U/NQ/2024/1/S2B_T11UNQ_20240119T185717_L2A/metadata.xml",
    },
    {
        "id": "S2A_T34KEB_20240921T084011_L2A",
        "metadata_s3_uri": "s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/34/K/EB/2024/9/S2A_T34KEB_20240921T084011_L2A/metadata.xml",
    },
    {
        "id": "S2B_T36NZN_20260311T075830_L2A",
        "metadata_s3_uri": "s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/36/N/ZN/2026/3/S2B_T36NZN_20260311T075830_L2A/metadata.xml",
    },
    {
        "id": "S2C_T18QYG_20260924T152653_L2A",
        "metadata_s3_uri": "s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/18/Q/YG/2026/9/S2C_T18QYG_20260924T152653_L2A/metadata.xml",
    },
]


@pytest.fixture(autouse=True)
def _clear_task_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("GEOPARQUET_PREFIX", "LIMIT"):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keeps moto from falling back to real credentials in the environment."""
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
    ):
        monkeypatch.setenv(key, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    monkeypatch.delenv("AWS_PROFILE", raising=False)


class FakeQueue:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.batches: list[list[dict[str, str]]] = []

    def send_message_batch(
        self, QueueUrl: str, Entries: list[dict[str, str]]
    ) -> dict[str, Any]:
        with self._lock:
            self.batches.append(Entries)
        return {}

    @property
    def bodies(self) -> list[dict[str, Any]]:
        return [json.loads(e["MessageBody"]) for batch in self.batches for e in batch]


@pytest.fixture
def fake_queue(monkeypatch: pytest.MonkeyPatch) -> FakeQueue:
    queue = FakeQueue()
    monkeypatch.setattr(task_module, "sqs", queue)
    return queue


def test_get_parquet_uris_from_prefix(aws_credentials: None) -> None:
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="e84-bucket",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        for key in ("items/_SUCCESS", "items/a.parquet", "items/nested/b.parquet"):
            s3.put_object(Bucket="e84-bucket", Key=key, Body=b"")

        assert get_parquet_uris_from_prefix("s3://e84-bucket/items/") == [
            "s3://e84-bucket/items/a.parquet",
            "s3://e84-bucket/items/nested/b.parquet",
        ]


def test_get_records_from_geoparquet_filters_and_resolves_hrefs() -> None:
    frames = get_records_from_geoparquet(
        [GEOPARQUET_PATH], "05.00", "05.99", "05.09", None
    )

    assert pl.concat(list(frames)).to_dicts() == EXPECTED_RECORDS


def test_get_records_from_geoparquet_respects_limit() -> None:
    frames = get_records_from_geoparquet([GEOPARQUET_PATH], None, None, None, 2)

    assert sum(frame.height for frame in frames) == 2


def test_format_cirrus_payload() -> None:
    workflow = task_module.WORKFLOW_NAME
    record = EXPECTED_RECORDS[0]
    payload = format_cirrus_payload(record, "upload-bucket")

    assert payload["id"] == f"{workflow}/workflow-{workflow}/{record['id']}"
    assert payload["metadata_href"] == record["metadata_s3_uri"]
    process = payload["process"][0]
    assert process["workflow"] == workflow
    assert process["upload_options"]["path_template"].startswith("s3://upload-bucket/")
    assert process["tasks"] == {"sentinel-2-l2a-to-stac": {}}


def test_queue_payloads_batches_messages(fake_queue: FakeQueue) -> None:
    frames = iter([pl.DataFrame(EXPECTED_RECORDS)])

    queue_payloads(frames, "upload-bucket", batch_size=4)

    # Batches are sent concurrently, so only their contents are deterministic.
    assert sorted(len(batch) for batch in fake_queue.batches) == [2, 4]
    assert sorted(body["metadata_href"] for body in fake_queue.bodies) == sorted(
        record["metadata_s3_uri"] for record in EXPECTED_RECORDS
    )


def get_test_cases() -> list[tuple[Path, Path, bool]]:
    test_cases = []
    fixtures_dir = Path(__file__).parent / "fixtures" / "payloads"

    for dirpath, _dirnames, filenames in os.walk(fixtures_dir):
        if "in.json" in filenames:
            input_file = Path(dirpath) / "in.json"
            output_file = Path(dirpath) / "out.json"
            exception_file = Path(dirpath) / "exception.txt"

            if exception_file.exists():
                test_cases.append((input_file, exception_file, True))
            elif output_file.exists():
                test_cases.append((input_file, output_file, False))

    return test_cases


@pytest.mark.parametrize(
    "input_file, expected_output_file, expect_exception", get_test_cases()
)
def test_task(
    input_file: Path,
    expected_output_file: Path,
    expect_exception: bool,
    fake_queue: FakeQueue,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        task_module, "get_parquet_uris_from_prefix", lambda _prefix: [GEOPARQUET_PATH]
    )
    input_payload = json.loads(input_file.read_text())

    if expect_exception:
        expected_exception = expected_output_file.read_text().strip()
        with pytest.raises(Exception, match=expected_exception):
            S2GeoparquetFeeder.handler(payload=input_payload, upload=False)
    else:
        expected_output = json.loads(expected_output_file.read_text())
        actual_output = S2GeoparquetFeeder.handler(payload=input_payload, upload=False)
        assert actual_output == expected_output
        assert [body["metadata_href"] for body in fake_queue.bodies] == [
            record["metadata_s3_uri"] for record in EXPECTED_RECORDS
        ]
