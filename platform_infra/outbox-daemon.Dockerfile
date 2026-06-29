# =========================================================================
# 🐍 THE INDUSTRY-STANDARD ENTERPRISE RUNTIME CONTAINER (Debian-Slim)
# =========================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

# 🟢 SOLUTION: Align the internal container workspace path with your cluster standard! [1.1]
WORKDIR /platform_app

# Mount the compiler cache layer and copy the repository metadata blueprints
# This captures your single, deterministic top-level lockfile context cleanly
COPY pyproject.toml uv.lock ./

# Copy the entire monorepo directory tree structure to satisfy workspace members
COPY outbox_daemon/ ./outbox_daemon
COPY observability/ ./observability
COPY schemas/ ./schemas
COPY sales/ ./sales
COPY finance/ ./finance
COPY shipping/ ./shipping
COPY notifications/ ./notifications

# 🟢 SOLUTION: Build an optimized, isolated runtime layer targeting ONLY the outbox daemon inside /platform_app [1.1]
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package outbox_daemon

# 🟢 SOLUTION: Route your primary system execution path directly into your aligned workspace virtual environment [1.1]
ENV PATH="/platform_app/.venv/bin:$PATH"
ENV PYTHONPATH="/platform_app/outbox_daemon/src:/platform_app/observability/src"
ENV PYTHONUNBUFFERED=1

# Execute the primary consumer app loop natively using your standard module lookup paths
CMD ["python", "-m", "outbox_daemon.main"]
