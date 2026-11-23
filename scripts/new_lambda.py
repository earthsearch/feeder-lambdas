#!/usr/bin/env python3
"""Scaffold a new Cirrus feeder lambda from the `template/` folder.

Usage: scripts/new_lambda.py <lambda-name>

<lambda-name> must be kebab-case (lowercase letters, digits, and hyphens), e.g.
`sentinel-fetcher`. Creates `lambdas/<lambda-name>/` with all template placeholders
(`lambda-template`, `lambda_template`, `LambdaTemplate`) replaced.
"""

import keyword
import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_DIR = REPO_ROOT / "template"
LAMBDAS_DIR = REPO_ROOT / "lambdas"

NAME_RE = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$")


def to_pascal_case(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("-"))


def replace_in_file(path: Path, replacements: list[tuple[str, str]]) -> None:
    text = path.read_text()
    for old, new in replacements:
        text = text.replace(old, new)
    path.write_text(text)


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("Usage: scripts/new_lambda.py <lambda-name>")

    name = sys.argv[1]
    if not NAME_RE.match(name):
        sys.exit(
            f"Invalid lambda name {name!r}: use lowercase letters, digits, and "
            "hyphens, e.g. 'sentinel-fetcher'."
        )

    if keyword.iskeyword(name.replace("-", "_")):
        sys.exit(f"Invalid lambda name {name!r}: it is a Python keyword.")

    dest = LAMBDAS_DIR / name
    if dest.exists():
        sys.exit(f"'{dest.relative_to(REPO_ROOT)}' already exists.")

    underscore_name = name.replace("-", "_")
    pascal_name = to_pascal_case(name)

    shutil.copytree(
        TEMPLATE_DIR, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
    )

    replacements = [
        ("lambdas/_template", f"lambdas/{name}"),
        ("lambda_template", underscore_name),
        ("lambda-template", name),
        ("LambdaTemplate", pascal_name),
        (
            "Template for a new Cirrus feeder Lambda - copy this folder to start a "
            "new lambda",
            "TODO: describe this lambda",
        ),
        (
            "Template for a new Cirrus feeder Lambda. See "
            "[DEVELOPMENT.md](../DEVELOPMENT.md) at the\n"
            "repo root for instructions on using this template to scaffold a new "
            "lambda.",
            "TODO: describe this lambda. See "
            "[DEVELOPMENT.md](../../DEVELOPMENT.md) at the\n"
            "repo root for more information on this monorepo.",
        ),
    ]
    for path in dest.rglob("*"):
        if path.is_file():
            replace_in_file(path, replacements)

    (dest / "src" / "lambda_template").rename(dest / "src" / underscore_name)

    print(f"Created lambdas/{name}/")
    print("Next steps:")
    print(f"  1. Update dependencies in lambdas/{name}/pyproject.toml")
    print(
        "  2. Implement the lambda's logic in "
        f"lambdas/{name}/src/{underscore_name}/task.py"
    )
    print(f"  3. Add test payload fixtures under lambdas/{name}/tests/fixtures")
    print(f"  4. Update the COPY/CMD lines in lambdas/{name}/Dockerfile if needed")
    print(
        "  5. Run `uv sync --all-packages` from the repo root to add the new "
        "member to uv.lock"
    )


if __name__ == "__main__":
    main()
