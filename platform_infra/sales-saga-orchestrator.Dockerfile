# =========================================================================
# 🐍 THE INDUSTRY-STANDARD ENTERPRISE APPLICATION RUNTIME (Debian-Slim)
# =========================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

WORKDIR /platform_app

# Copy ONLY the package configuration belonging to the Sales domain
COPY sales/pyproject.toml ./

# Mount the cache and download pre-compiled binary wheels instantly!
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r pyproject.toml

# 🟢 SOLUTION: Copy the overarching source package tree to capture all submodules uniformly!
COPY sales/src/sales /platform_app/sales
COPY observability/src/observability /platform_app/observability

COPY schemas /platform_app/schemas
# 🟢 SOLUTION: Explicitly mount the platform root to resolve parent namespace exploration tracks
ENV PYTHONPATH="/platform_app"
ENV PYTHONUNBUFFERED=1

# 🟢 SOLUTION: Execute the orchestration loop using your original path-based navigation standard!
CMD ["python", "sales/orchestrator/main.py"]
