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

# Copy only your decoupled orchestrator source code
COPY sales/src/sales /platform_app/sales
COPY observability/src/observability /platform_app/observability
COPY schemas /platform_app/schemas

# 🟢 FIX: Append your shared tracking package route directly into Python's path array! [1.1]
ENV PYTHONPATH="/platform_app:/platform_app/observability"
ENV PYTHONUNBUFFERED=1

CMD ["python", "sales/orchestrator/main.py"]
