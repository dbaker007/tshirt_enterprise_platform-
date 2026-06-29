# =========================================================================
# 🐍 THE INDUSTRY-STANDARD ENTERPRISE RUNTIME CONTAINER (Debian-Slim)
# =========================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

WORKDIR /workspace

# Mount the compiler cache layer and copy the repository metadata blueprints
# This captures your single, deterministic top-level lockfile context cleanly
COPY pyproject.toml uv.lock ./

# Copy the entire monorepo directory tree structure to satisfy workspace members
COPY shipping/ ./shipping
COPY finance/ ./finance
COPY notifications/ ./notifications
COPY observability/ ./observability
COPY schemas/ ./schemas
COPY sales/ ./sales
COPY outbox_daemon/ ./outbox_daemon

# 🟢 SOLUTION: Build an optimized, isolated runtime layer targeting ONLY shipping
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package shipping

# 🟢 SOLUTION: Route your primary system execution path directly into uv's .venv bin directory
ENV PATH="/workspace/.venv/bin:$PATH"
ENV PYTHONPATH="/workspace/shipping/src:/workspace/observability/src"
ENV PYTHONUNBUFFERED=1

# Execute the primary consumer app loop natively using your standard module lookup paths
CMD ["python", "-m", "shipping.app"]
