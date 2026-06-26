# =========================================================================
# 🐍 THE INDUSTRY-STANDARD ENTERPRISE RUNTIME CONTAINER (Debian-Slim)
# =========================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

WORKDIR /platform_app

# Copy the master domain package manifest belonging to the Shipping family
COPY shipping/pyproject.toml ./

# Mount the compiler cache layer and download requirements instantly
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r pyproject.toml

# Copy your core decoupled shipping application package
COPY shipping/src/shipping /platform_app/shipping

# Copy your shared enterprise telemetry package right into Python's path
COPY observability/src/observability /platform_app/observability
COPY schemas /platform_app/schemas

# Explicitly append your absolute application lookup variables
ENV PYTHONPATH="/platform_app:/platform_app/observability"
ENV PYTHONUNBUFFERED=1

# Execute the primary consumer app loop natively
CMD ["python", "-m", "shipping.app"]
