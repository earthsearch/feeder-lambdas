FROM ghcr.io/astral-sh/uv:0.12.22 AS uv

FROM public.ecr.aws/lambda/python:3.14 AS builder

RUN dnf update -y && \
  dnf install -y git libxml2-devel libxslt-devel gcc && \
  dnf clean all && \
  rm -rf /var/cache/dnf

# Enable bytecode compilation, to improve cold-start performance.
ENV UV_COMPILE_BYTECODE=1

# Disable installer metadata, to create a deterministic layer.
ENV UV_NO_INSTALLER_METADATA=1

# Enable copy mode to support bind mount caching.
ENV UV_LINK_MODE=copy

# NOTE: build context must be the repo root (`docker build -f lambdas/s2-safe-feeder/Dockerfile .`)
# so the uv workspace root pyproject.toml and lockfile are visible.
#
# Bundle the dependencies into the Lambda task root via `uv pip install --target`.
#
# Omit any other workspace members (`--package`) and development dependencies (`--no-dev`).
# This ensures that the Docker layer cache is only invalidated when this lambda's
# pyproject.toml or the shared uv.lock change, but remains robust to changes elsewhere
# in the workspace.
RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=lambdas/s2-safe-feeder,target=lambdas/s2-safe-feeder \
    uv export --frozen --no-emit-workspace --no-dev --no-editable --package s2-safe-feeder -o requirements.txt && \
    uv pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}" ./lambdas/s2-safe-feeder


FROM public.ecr.aws/lambda/python:3.14

# Copy the runtime dependencies from the builder stage.
COPY --from=builder ${LAMBDA_TASK_ROOT} ${LAMBDA_TASK_ROOT}

COPY lambdas/s2-safe-feeder/src/s2_safe_feeder/ ${LAMBDA_TASK_ROOT}/s2_safe_feeder/

WORKDIR ${LAMBDA_TASK_ROOT}

# Uncomment one of the following:

# 1. for lambda task, use CMD
CMD [ "s2_safe_feeder.task.lambda_handler" ]

# 2. for batch task, use ENTRYPOINT
#ENV PYTHONPATH="/var/task"
#ENTRYPOINT [ "./bin/s2-safe-feeder" ]
