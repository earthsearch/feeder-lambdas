# Feeder Lambdas

*This repository is a monorepo of Cirrus feeder Lambda functions. See the [DEVELOPMENT.md](DEVELOPMENT.md) file for
instructions on adding a new lambda using the `template/` scaffold and `scripts/new_lambda.py`.*

## Usage

Each lambda lives under `lambdas/<name>/` and is built/deployed independently. To use a lambda in
a Cirrus workflow, reference its Docker image location in the task configuration file in the
Cirrus deployment repository. See each lambda's own CHANGELOG for version information.

```
Docker URL
```

Configuration parameters are available to a lambda through the Cirrus process payload as `['tasks']['<taskname>']`.

## Development

Lambdas can be run locally with the built-in CLI from within the lambda's own directory, e.g.:

```
$ cd lambdas/<name>
$ uv run <name>

usage: task.py run [-h] [--logging LOGGING] [--output OUTPUT] [--workdir WORKDIR] [--save-workdir] [--skip-upload] [--skip-validation] [--upload] [--no-upload] [--validate]
                   [--no-validate] [--local]
                   [input]

positional arguments:
  input              Full path of item collection to process (s3 or local) (default: None)

options:
  -h, --help         show this help message and exit
  --logging LOGGING  DEBUG, INFO, WARN, ERROR, CRITICAL (default: INFO)
  --output OUTPUT    Write output payload to this URL (default: None)
  --workdir WORKDIR  Use this as work directory. Will be created. (default: None)
  --save-workdir     Save workdir after completion (default: False)
  --skip-upload      DEPRECATED: Skip uploading of generated assets and STAC Items (default: False)
  --skip-validation  DEPRECATED: Skip validation of input payload (default: False)
  --upload           Upload generated assets and resulting STAC Items (default: True)
  --no-upload        Don't upload generated assets and resulting STAC Items (default: True)
  --validate         Validate input payload (default: True)
  --no-validate      Don't validate input payload (default: True)
  --local            Run local mode (save-workdir = True, upload = False, workdir = 'local-output', output = 'local-output/output-payload.json') (default: False)
```

When running locally use the `--local` option which will store all output in a local folder called `local-output` and will
not try to upload the data files to s3.

```
$ task.py payload.json --local
```

## Testing

This repository uses [uv](https://docs.astral.sh/uv/getting-started/installation/) (as a workspace, see
[DEVELOPMENT.md](DEVELOPMENT.md)) and uses [pytest](https://docs.pytest.org/en/stable/) for testing.

Each lambda's `tests/test_task.py` iterates through the input payloads in its own `tests/fixtures`, which
contains a series of input and payload files, each pair in its own folder. For expected errors in tests
an `exception.txt` file is provided instead of an output payload.

To run all tests:

```
uv run pytest
```

To run a single lambda's tests:

```
uv run pytest lambdas/<name>
```

# Versions and Releases

![CalVer:YYYY.0M.0D\_MICRO](https://img.shields.io/badge/CalVer-YYYY.0M.0D__MICRO-00aa00.svg)

This project uses CalVer for versioning releases.  The format is specified as
`YYYY.0M.0D_MICRO`, where the tokens are:

| token | description                     | example(s)             |
|-------|---------------------------------|------------------------|
| YYYY  | the full year                   | 2006, 2016, 2106)      |
| 0M    | the zero-padded month           | 01, 02 ... 11, 12      |
| 0D    | the zero-padded day of month    | 01, 02 ... 30, 31      |
| MICRO | (optional) free form, as needed | alpha, rc0, post0, ... |
