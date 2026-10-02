# Changelog

All notable changes to this project will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This project adheres to [CalVer](https://calver.org/). See the
[README](README.md#versions-and-releases) for the specific CalVer format
used by this project.

## [Unreleased]

### Changed

- Restructured the repo into a uv workspace monorepo so it can hold multiple feeder
  lambdas: the root `pyproject.toml` now only holds shared dev tooling config, and each
  lambda lives in its own independent package under `lambdas/<name>/` (own
  `pyproject.toml`, `Dockerfile`, `docker-compose.yml`, `tests/`). The former
  `cirrus-task-example` code was replaced with a generic `template/` scaffold, kept
  outside of `lambdas/` so it isn't picked up as a uv workspace member. Added
  `scripts/new_lambda.py` to scaffold a new lambda from the template.
  See [DEVELOPMENT.md](DEVELOPMENT.md) for how to add a new lambda.
- Bumped dependency versions: `stactask` to 0.7.0, `mypy` to 1.18.2, `pre-commit` to
  4.6.2, `pytest` to 9.1.1, `ruff` to 0.16.10.
- Bumped `.python-version` to 3.14, the template's `uv` base image to 0.12.22, and its
  Lambda base images to `public.ecr.aws/lambda/python:3.14`. Removed the `lambgeo/lambda-gdal`
  stage from the template Dockerfile.

### Added

- Versioning section to README.md, and switched to using dot-delimited CalVer.

## [v2025.03.12]

### Changed

- Dockerfile now uses UV build scheme, along with lambgeo base. ([#2])
- CLI specified via pyproject.toml. ([#1])

## [v2025.03.11]

Initial release

[unreleased]: https://github.com/cirrus-geo/cirrus-task-example/compare/v2025.03.12..main
[v2025.03.12]: https://github.com/cirrus-geo/cirrus-task-example/compare/v2025.03.11..v2025.03.12
[v2025.03.11]: https://github.com/cirrus-geo/cirrus-task-example/tree/v2025.03.11
[#1]: https://github.com/cirrus-geo/cirrus-task-example/pull/1
[#2]: https://github.com/cirrus-geo/cirrus-task-example/pull/2
