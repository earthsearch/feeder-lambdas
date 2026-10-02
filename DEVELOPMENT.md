# Feeder Lambda Development

This repo is a monorepo holding multiple containerized Cirrus feeder lambdas, each built with
the [stac-task](https://github.com/stac-utils/stac-task) library. The stac-task Task class has
several convenient utility methods for performing common tasks (e.g., setting collection,
uploading STAC Item assets to s3).

See the [stac-task](https://github.com/stac-utils/stac-task) README for more information on
developing a task.

## Repository layout

Dependencies and tooling are managed with a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/):
the root `pyproject.toml` holds shared dev tooling (mypy/ruff/pytest config) and is not itself a
package, while every lambda under `lambdas/<name>/` is an independent, installable, deployable
package with its own `pyproject.toml`, `Dockerfile`, `docker-compose.yml`, and `tests/`.

```
template/               # scaffold for a new lambda, kept outside lambdas/ so it isn't
  pyproject.toml        # picked up as a uv workspace member
  Dockerfile
  docker-compose.yml
  src/lambda_template/
  tests/
lambdas/
  <your-lambda>/
    ...
```

## Adding a new lambda

Run the scaffold script from the repo root, passing the new lambda's name in kebab-case:

```
scripts/new_lambda.py <your-lambda-name>
```

This copies `template/` to `lambdas/<your-lambda-name>/`, renames `src/lambda_template` to
`src/<your_lambda_name>`, and replaces the template placeholders (`lambda-template`,
`lambda_template`, `LambdaTemplate`) throughout the copied files. Then:

1. Update dependencies in the new `pyproject.toml`.
2. Implement the lambda's logic in `src/<your_lambda_name>/task.py`.
3. Add test payload fixtures under `tests/fixtures` and update `tests/test_task.py`'s import.
4. Update the `COPY`/`CMD` lines in the new `Dockerfile` if needed.
5. Run `uv sync --all-packages` from the repo root to add the new member to `uv.lock`.

## Building

Each lambda's Docker image must be built with the **repo root** as the build context, since the
uv workspace lockfile lives there:

```
docker build -f lambdas/<your-lambda-name>/Dockerfile -t <your-lambda-name> .
```

or, from inside the lambda's own directory:

```
docker compose build
```

## Testing

`--all-packages` is required: the workspace root is virtual, so without it uv only installs the
root's dev tooling and the lambdas themselves are missing from the environment.

Run all lambdas' tests from the repo root:

```
uv run --all-packages pytest
```

Run a single lambda's tests:

```
uv run --package <your-lambda-name> pytest lambdas/<your-lambda-name>
```

## Deployment

The built Docker image for each lambda should be published to a registry that is accessible
from the Cirrus deployment. Ideally the image is in the same region as where the lambda will run.
