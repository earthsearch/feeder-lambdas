import json
import os
from pathlib import Path

import pytest
from s2_safe_feeder.task import S2SafeFeeder


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
    input_file: Path, expected_output_file: Path, expect_exception: bool
) -> None:
    input_payload = json.loads(input_file.read_text())

    if expect_exception:
        expected_exception = expected_output_file.read_text().strip()
        with pytest.raises(Exception, match=expected_exception):
            S2SafeFeeder.handler(payload=input_payload, upload=False)
    else:
        expected_output = json.loads(expected_output_file.read_text())
        actual_output = S2SafeFeeder.handler(payload=input_payload, upload=False)
        assert actual_output == expected_output
