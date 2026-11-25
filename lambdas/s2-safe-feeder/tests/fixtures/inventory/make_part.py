"""Regenerates part.parquet, the synthetic S3 Inventory data part used by the tests.

The schema and key layout follow real inventory parts: bucket, key, size and
last_modified_date (datetime ms UTC), one row per object, sorted by key.

Run from the repo root:
  uv run --package s2-safe-feeder python \
      lambdas/s2-safe-feeder/tests/fixtures/inventory/make_part.py
"""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

OUTPUT = Path(__file__).with_name("part.parquet")
BUCKET = "test-safe-bucket"
MODIFIED = datetime(2026, 4, 16, 12, 0, 0, tzinfo=UTC)

# (SAFE name, DATASTRIP folder, or None when this part lacks the SAFE's DATASTRIP key)
SAFES: list[tuple[str, str | None]] = [
    # Real SAFE / DATASTRIP pair.
    (
        "S2A_MSIL2A_20220101T002021_N0510_R059_T57UXA_20240420T155212",
        "DS_S2RP_20240420T155212_S20220101T002019",
    ),
    # Excluded baseline 05.09.
    (
        "S2A_MSIL2A_20230615T103031_N0509_R108_T31UFU_20230615T160000",
        "DS_2APS_20230615T160000_S20230615T103027",
    ),
    # Below the minimum baseline.
    (
        "S2B_MSIL2A_20190110T104359_N0400_R008_T31TCJ_20190110T120000",
        "DS_S2RP_20190110T120000_S20190110T104355",
    ),
    # Real SAFE / DATASTRIP pair; the sensing times differ by minutes.
    (
        "S2B_MSIL2A_20230305T140709_N0510_R110_T19GEQ_20240819T200028",
        "DS_S2RP_20240819T200028_S20230305T142205",
    ),
    # Unparseable name: no _Nxxyy_ field, so the baseline is null.
    (
        "S2B_MSIL2A_20230305T140709_R110_T19GER_20240819T200028",
        "DS_S2RP_20240819T200028_S20230305T142205",
    ),
    # Split across parts: this part has no DATASTRIP key for it.
    ("S2B_MSIL2A_20230306T140709_N0510_R110_T19GEQ_20240820T000000", None),
    # DATASTRIP folder without an _S<time> part, so the item ID is null.
    (
        "S2B_MSIL2A_20230307T140709_N0510_R110_T19GEQ_20240821T000000",
        "DS_S2RP_20240821T000000",
    ),
    # Above the maximum baseline.
    (
        "S2B_MSIL2A_20240101T101010_N9999_R022_T19GEQ_20240101T120000",
        "DS_S2RP_20240101T120000_S20240101T101005",
    ),
    # L1C twin of the S2C SAFE below: same sensor, tile and datastrip sensing start,
    # so the same item ID. The feeder must drop it.
    (
        "S2C_MSIL1C_20250601T101031_N0511_R022_T32TQM_20250601T130000",
        "DS_2CPS_20250601T130000_S20250601T101025",
    ),
    # S2C, with a nominal-production DATASTRIP site code (2CPS) rather than a
    # reprocessing one (S2RP).
    (
        "S2C_MSIL2A_20250601T101031_N0511_R022_T32TQM_20250601T150000",
        "DS_2CPS_20250601T150000_S20250601T101025",
    ),
]


def safe_keys(name: str, datastrip: str | None) -> list[str]:
    root = f"safe/{name}.SAFE"
    product = name.split("_")[1]  # MSIL2A or MSIL1C
    level = product.removeprefix("MSI")
    tile = name.split("_")[-2]
    sensing = name[11:26]
    keys = [
        f"{root}/GRANULE/{level}_{tile}_A000000_{sensing}/IMG_DATA/R10m/"
        f"{tile}_{sensing}_B02_10m.jp2",
        f"{root}/MTD_{product}.xml",
        f"{root}/manifest.safe",
    ]
    if datastrip is not None:
        keys += [
            f"{root}/DATASTRIP/{datastrip}/MTD_DS.xml",
            f"{root}/DATASTRIP/{datastrip}/QI_DATA/FORMAT_CORRECTNESS.xml",
        ]
    return keys


def main() -> None:
    keys = [key for name, datastrip in SAFES for key in safe_keys(name, datastrip)]
    first_name, first_datastrip = SAFES[0]
    # An object outside any SAFE folder.
    keys.append(f"metadata/{first_name}.json")
    # Ends in /MTD_DS.xml but isn't directly under DATASTRIP/DS_*/, so it passes the
    # feeder's suffix prefilter and must be rejected by the regex.
    keys.append(
        f"safe/{first_name}.SAFE/DATASTRIP/{first_datastrip}/QI_DATA/MTD_DS.xml"
    )
    keys.sort()

    pl.DataFrame(
        {
            "bucket": [BUCKET] * len(keys),
            "key": keys,
            "size": [1000 + i for i in range(len(keys))],
            "last_modified_date": [MODIFIED] * len(keys),
        },
        schema={
            "bucket": pl.String,
            "key": pl.String,
            "size": pl.Int64,
            "last_modified_date": pl.Datetime("ms", "UTC"),
        },
    ).write_parquet(OUTPUT)
    print(f"Wrote {len(keys)} rows to {OUTPUT}")


if __name__ == "__main__":
    main()
