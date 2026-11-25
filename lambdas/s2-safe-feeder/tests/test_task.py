import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from s2_safe_feeder import task as task_module
from s2_safe_feeder.task import (
    S2SafeFeeder,
    format_cirrus_payload,
    get_records_from_inventory,
    item_id,
    lambda_handler,
    queue_payloads,
    scan_inventory,
    with_safe_fields,
)

# Synthetic S3 Inventory data part (real schema, one row per object, sorted by key)
# holding ten SAFEs: normal ones, filtered and excluded baselines, an unparseable SAFE
# name, a DATASTRIP folder without a sensing time, an L1C SAFE sharing an L2A SAFE's
# item ID, and one SAFE whose DATASTRIP key is in another part. Plus one object outside
# any SAFE, and a decoy MTD_DS.xml nested below a DATASTRIP folder. Regenerate it with
# fixtures/inventory/make_part.py.
INVENTORY_PATH = str(Path(__file__).parent / "fixtures" / "inventory" / "part.parquet")
SAFE_BUCKET = "test-safe-bucket"

# 10 rows of real sentinel-2-c1-l2a STAC geoparquet, copied from s2-geoparquet-feeder.
# Its ids are real Earth Search ids, so they check item_id against ids built from
# real s2:product_uri and s2:datastrip_id values.
GEOPARQUET_PATH = str(
    Path(__file__).parent / "fixtures" / "geoparquet" / "items.parquet"
)

# Every SAFE in the fixture whose DATASTRIP/DS_*/MTD_DS.xml key is in this part:
# (safe_key, datastrip folder, processing baseline, item id).
EXPECTED_SAFES = [
    (
        "safe/S2A_MSIL2A_20220101T002021_N0510_R059_T57UXA_20240420T155212.SAFE",
        "DS_S2RP_20240420T155212_S20220101T002019",
        "05.10",
        "S2A_T57UXA_20220101T002019_L2A",
    ),
    (
        "safe/S2A_MSIL2A_20230615T103031_N0509_R108_T31UFU_20230615T160000.SAFE",
        "DS_2APS_20230615T160000_S20230615T103027",
        "05.09",
        "S2A_T31UFU_20230615T103027_L2A",
    ),
    (
        "safe/S2B_MSIL2A_20190110T104359_N0400_R008_T31TCJ_20190110T120000.SAFE",
        "DS_S2RP_20190110T120000_S20190110T104355",
        "04.00",
        "S2B_T31TCJ_20190110T104355_L2A",
    ),
    (
        "safe/S2B_MSIL2A_20230305T140709_N0510_R110_T19GEQ_20240819T200028.SAFE",
        "DS_S2RP_20240819T200028_S20230305T142205",
        "05.10",
        "S2B_T19GEQ_20230305T142205_L2A",
    ),
    # No _Nxxyy_ field in the name.
    (
        "safe/S2B_MSIL2A_20230305T140709_R110_T19GER_20240819T200028.SAFE",
        "DS_S2RP_20240819T200028_S20230305T142205",
        None,
        "S2B_T19GER_20230305T142205_L2A",
    ),
    # No _S<time> in the DATASTRIP folder.
    (
        "safe/S2B_MSIL2A_20230307T140709_N0510_R110_T19GEQ_20240821T000000.SAFE",
        "DS_S2RP_20240821T000000",
        "05.10",
        None,
    ),
    (
        "safe/S2B_MSIL2A_20240101T101010_N9999_R022_T19GEQ_20240101T120000.SAFE",
        "DS_S2RP_20240101T120000_S20240101T101005",
        "99.99",
        "S2B_T19GEQ_20240101T101005_L2A",
    ),
    # L1C twin of the next SAFE: same item ID.
    (
        "safe/S2C_MSIL1C_20250601T101031_N0511_R022_T32TQM_20250601T130000.SAFE",
        "DS_2CPS_20250601T130000_S20250601T101025",
        "05.11",
        "S2C_T32TQM_20250601T101025_L2A",
    ),
    (
        "safe/S2C_MSIL2A_20250601T101031_N0511_R022_T32TQM_20250601T150000.SAFE",
        "DS_2CPS_20250601T150000_S20250601T101025",
        "05.11",
        "S2C_T32TQM_20250601T101025_L2A",
    ),
]


@pytest.fixture(autouse=True)
def _clear_task_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LIMIT", raising=False)


class FakeQueue:
    def __init__(self) -> None:
        self.batches: list[list[dict[str, str]]] = []

    async def send_message_batch(
        self, QueueUrl: str, Entries: list[dict[str, str]]
    ) -> dict[str, Any]:
        self.batches.append(Entries)
        return {}

    @property
    def bodies(self) -> list[dict[str, Any]]:
        return [json.loads(e["MessageBody"]) for batch in self.batches for e in batch]


@pytest.fixture
def fake_queue(monkeypatch: pytest.MonkeyPatch) -> FakeQueue:
    queue = FakeQueue()

    @asynccontextmanager
    async def fake_client() -> AsyncIterator[FakeQueue]:
        yield queue

    monkeypatch.setattr(task_module, "sqs_client", fake_client)
    return queue


def test_scan_inventory_keeps_one_row_per_safe() -> None:
    rows = scan_inventory(INVENTORY_PATH).collect().to_dicts()

    assert rows == [
        {"bucket": SAFE_BUCKET, "safe_key": safe_key, "datastrip": datastrip}
        for safe_key, datastrip, _, _ in EXPECTED_SAFES
    ]


def test_with_safe_fields_derives_href_baseline_and_id() -> None:
    rows = (
        with_safe_fields(scan_inventory(INVENTORY_PATH))
        .select("safe_href", "safe_name", "processing_baseline", "id")
        .collect()
        .to_dicts()
    )

    assert rows == [
        {
            "safe_href": f"s3://{SAFE_BUCKET}/{safe_key}",
            "safe_name": safe_key.removeprefix("safe/"),
            "processing_baseline": baseline,
            "id": expected_id,
        }
        for safe_key, _, baseline, expected_id in EXPECTED_SAFES
    ]


def test_item_id_matches_task_ids_from_geoparquet() -> None:
    frame = (
        pl.scan_parquet(GEOPARQUET_PATH)
        .select(
            "id",
            pl.col("s2:product_uri").alias("safe_name"),
            pl.col("s2:datastrip_id").alias("datastrip"),
        )
        .with_columns(item_id().alias("derived_id"))
        .collect()
    )

    assert frame.height == 10
    assert frame["derived_id"].to_list() == frame["id"].to_list()


def expected_record(index: int) -> dict[str, Any]:
    safe_key, _, _, expected_id = EXPECTED_SAFES[index]
    return {"id": expected_id, "safe_href": f"s3://{SAFE_BUCKET}/{safe_key}"}


# EXPECTED_SAFES surviving the default filters (>=05.00, <=05.99, !=05.09) and the
# L2A-only check.
EXPECTED_RECORDS = [expected_record(i) for i in (0, 3, 8)]


def test_get_records_from_inventory_filters_baselines() -> None:
    frames = get_records_from_inventory(INVENTORY_PATH, "05.00", "05.99", "05.09", None)

    assert pl.concat(list(frames)).to_dicts() == EXPECTED_RECORDS


def test_get_records_from_inventory_drops_unparseable_and_non_l2a() -> None:
    frames = get_records_from_inventory(INVENTORY_PATH, None, None, None, None)

    # Everything except the null-baseline (4), null-id (5) and L1C (7) SAFEs, even
    # with every baseline filter off.
    assert pl.concat(list(frames)).to_dicts() == [
        expected_record(i) for i in (0, 1, 2, 3, 6, 8)
    ]


def test_get_records_from_inventory_limits_after_filters() -> None:
    frames = get_records_from_inventory(INVENTORY_PATH, "05.00", "05.99", "05.09", 2)

    assert pl.concat(list(frames)).to_dicts() == EXPECTED_RECORDS[:2]


def test_format_cirrus_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    # Read from the environment at import time, so pin it to the default here.
    monkeypatch.setattr(task_module, "WORKFLOW_NAME", "sentinel-2-c1-l2a")
    record = EXPECTED_RECORDS[1]

    payload = format_cirrus_payload(record, "upload-bucket")

    assert payload == {
        "id": (
            "sentinel-2-c1-l2a/workflow-sentinel-2-c1-l2a/"
            "S2B_T19GEQ_20230305T142205_L2A"
        ),
        "safe_href": "s3://test-safe-bucket/safe/S2B_MSIL2A_20230305T140709_N0510_R110_T19GEQ_20240819T200028.SAFE",
        "process": [
            {
                "workflow": "sentinel-2-c1-l2a",
                "upload_options": {
                    "path_template": "s3://upload-bucket/${collection}/${mgrs:utm_zone}/${mgrs:latitude_band}/${mgrs:grid_square}/${year}/${month}/${id}",
                    "collections": {"sentinel-2-l2a": "$[?(@.id =~ '.*')]"},
                },
                "tasks": {"sentinel-2-l2a-to-stac": {}},
            }
        ],
    }
    # Contracts with the task and Cirrus, named so a drift fails with a clear message.
    assert "metadata_href" not in payload  # mutually exclusive with safe_href
    assert "replace" not in payload  # completed scenes must be skipped
    assert payload["process"][0]["workflow"] == "sentinel-2-c1-l2a"
    assert list(payload["process"][0]["tasks"]) == ["sentinel-2-l2a-to-stac"]


def test_queue_payloads_batches_messages(fake_queue: FakeQueue) -> None:
    frames = iter([pl.DataFrame(EXPECTED_RECORDS)])

    queue_payloads(frames, "upload-bucket", batch_size=2)

    # Batches are sent concurrently, so only their contents are deterministic.
    assert sorted(len(batch) for batch in fake_queue.batches) == [1, 2]
    assert sorted(body["safe_href"] for body in fake_queue.bodies) == sorted(
        record["safe_href"] for record in EXPECTED_RECORDS
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


def use_local_inventory(payload: dict[str, Any]) -> dict[str, Any]:
    """Repoints a fixture payload's s3:// URI at the local inventory fixture."""
    if "inventory_uri" in payload:
        payload["inventory_uri"] = INVENTORY_PATH
    return payload


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
    # Read from the environment at import time, so pin it to the default here.
    monkeypatch.setattr(task_module, "WORKFLOW_NAME", "sentinel-2-c1-l2a")
    input_payload = use_local_inventory(json.loads(input_file.read_text()))

    if expect_exception:
        expected_exception = expected_output_file.read_text().strip()
        with pytest.raises(Exception, match=expected_exception):
            S2SafeFeeder.handler(payload=input_payload, upload=False)
    else:
        expected_output = use_local_inventory(
            json.loads(expected_output_file.read_text())
        )
        actual_output = S2SafeFeeder.handler(payload=input_payload, upload=False)
        assert actual_output == expected_output
        # The exact Cirrus payloads sent to SQS. The fixture's records fit in one
        # batch, so their order is deterministic.
        queued_file = input_file.with_name("queued.json")
        assert fake_queue.bodies == json.loads(queued_file.read_text())


@pytest.mark.parametrize(
    "payload, message",
    [
        ({"inventory_uri": INVENTORY_PATH}, "upload_bucket is required"),
        ({"upload_bucket": "upload-bucket"}, "inventory_uri is required"),
    ],
)
def test_missing_required_field_raises_value_error(
    payload: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        S2SafeFeeder.handler(payload=payload, upload=False)


def test_payload_baselines_override_defaults(fake_queue: FakeQueue) -> None:
    payload = {
        "upload_bucket": "upload-bucket",
        "inventory_uri": INVENTORY_PATH,
        "exclude_processing_baseline": "05.10",
    }

    S2SafeFeeder.handler(payload=payload, upload=False)

    # The default exclusion (05.09) no longer applies, and both 05.10 SAFEs are gone.
    assert [body["safe_href"] for body in fake_queue.bodies] == [
        expected_record(i)["safe_href"] for i in (1, 8)
    ]


def test_limit_from_payload(fake_queue: FakeQueue) -> None:
    payload = {"upload_bucket": "upload-bucket", "inventory_uri": INVENTORY_PATH}

    S2SafeFeeder.handler(payload={**payload, "limit": 1}, upload=False)

    assert [body["safe_href"] for body in fake_queue.bodies] == [
        EXPECTED_RECORDS[0]["safe_href"]
    ]


def test_limit_from_env(fake_queue: FakeQueue, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LIMIT", "2")
    payload = {"upload_bucket": "upload-bucket", "inventory_uri": INVENTORY_PATH}

    S2SafeFeeder.handler(payload=payload, upload=False)

    assert [body["safe_href"] for body in fake_queue.bodies] == [
        record["safe_href"] for record in EXPECTED_RECORDS[:2]
    ]


def test_lambda_handler_unwraps_sqs_records(fake_queue: FakeQueue) -> None:
    payload = {"upload_bucket": "upload-bucket", "inventory_uri": INVENTORY_PATH}
    event = {"Records": [{"body": json.dumps(payload)}, {"body": json.dumps(payload)}]}

    results = lambda_handler(event)

    assert results == [{**payload, "features": []}] * 2
    assert len(fake_queue.bodies) == 2 * len(EXPECTED_RECORDS)
