# =========================================================================
# 🐍 THE INDUSTRY-STANDARD ENTERPRISE RUNTIME CONTAINER (Debian-Slim)
# =========================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

WORKDIR /workspace

# Mount the compiler cache layer and copy the repository metadata blueprints
COPY pyproject.toml uv.lock ./

# Copy the entire monorepo directory tree structure to satisfy workspace members
COPY finance/ ./finance
COPY observability/ ./observability
COPY schemas/ ./schemas
COPY sales/ ./sales
COPY shipping/ ./shipping
COPY notifications/ ./notifications
COPY outbox_daemon/ ./outbox_daemon

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package finance

ENV PATH="/workspace/.venv/bin:$PATH"
ENV PYTHONPATH="/workspace/finance/src:/workspace/observability/src"
ENV PYTHONUNBUFFERED=1

# Execute the primary consumer app loop natively using your standard module lookup paths
CMD ["python", "-m", "finance.app"]
