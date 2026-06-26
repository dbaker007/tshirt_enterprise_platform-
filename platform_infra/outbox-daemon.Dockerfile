# =========================================================================
# 🐍 THE INDUSTRY-STANDARD ENTERPRISE RUNTIME CONTAINER (Debian-Slim)
# =========================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

WORKDIR /platform_app

# Copy the master domain package manifest belonging to the Outbox family
COPY outbox_daemon/pyproject.toml ./

# 🟢 CACHE BUSTER WALL: Modifying this date string character forces Docker to invalidate 
# all downstream cache steps, guaranteeing a true, un-cached network package download pass!
ENV PLATFORM_BUILD_TIMESTAMP="2026-06-24_15:30"

# Mount the compiler cache layer and download requirements instantly
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-cache-dir -r pyproject.toml

# Perfect repository alignment: Copying directly from your flat root directory
COPY outbox_daemon/src/outbox_daemon /platform_app/outbox_daemon
COPY observability/src/observability /platform_app/observability
COPY schemas /platform_app/schemas

# Explicitly append your absolute application lookup variables
ENV PYTHONPATH="/platform_app:/platform_app/outbox_daemon:/platform_app/observability"
ENV PYTHONUNBUFFERED=1

# Execute the primary consumer app loop natively from the root workspace
CMD ["python", "-m", "outbox_daemon.main"]
