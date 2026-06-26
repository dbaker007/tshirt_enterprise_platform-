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


COPY sales/src/sales/order_entry /platform_app/sales/order_entry

COPY observability/src/observability /platform_app/observability
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "sales.order_entry.main:app", "--host", "0.0.0.0", "--port", "8000"]
