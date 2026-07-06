# =========================================================================
# 🐳 STAGE 1: HIGH-PERFORMANCE WORKSPACE COMPILE BUILDER
# =========================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /workspace

# Mount cache and pull the complete metadata layout for compilation validation
COPY pyproject.toml uv.lock ./
COPY finance/ ./finance
COPY observability/ ./observability
COPY schemas/ ./schemas
COPY sales/ ./sales
COPY shipping/ ./shipping
COPY notifications/ ./notifications
COPY outbox_daemon/ ./outbox_daemon

# 🟢 SOLUTION: Sync the workspace with your pristine, explicit web extra feature flag! [1.1]
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package finance --extra web

# =========================================================================
# 🐍 STAGE 2: ULTRA-LEAN INDUSTRY-STANDARD ENVIRONMENT RUNTIME
# =========================================================================
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /workspace

# 🟢 SOLUTION: Pull ONLY your compiled virtual environment cleanly out of the builder! [1.1]
COPY --from=builder /workspace/.venv /workspace/.venv

# 🟢 SOLUTION: Only copy the required source code modules for this service shard into runtime! [1.1]
COPY finance/src /workspace/finance/src
COPY observability/src /workspace/observability/src

ENV PATH="/workspace/.venv/bin:$PATH"
ENV PYTHONPATH="/workspace/finance/src:/workspace/observability/src"
ENV PYTHONUNBUFFERED=1

EXPOSE 8001

# Execute the programmatic data query web tier via uvicorn cleanly
CMD ["python", "-m", "uvicorn", "finance.web:app", "--host", "0.0.0.0", "--port", "8001"]
