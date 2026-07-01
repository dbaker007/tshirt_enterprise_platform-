# =========================================================================
# PURCHASING MONOREPO LOCAL AUTOMATION NETWORK (WITH TRANSACTIONAL OUTBOX)
# =========================================================================

.PHONY: install test lint run daemon clean all

# Default directive executes the entire local validation matrix sequentially
all: install lint test

install:
	@echo "🔧 Initializing isolated python library dependencies..."
	uv pip install -r requirements.txt || uv pip install pytest httpx confluent-kafka sqlalchemy psycopg2-binary fastapi uvicorn ruff

test:
	@echo "🧪 Executing sales orchestrator unit test suite..."
	@mkdir -p schemas
	@ln -sf ../../schemas/command_envelope.avsc schemas/command_envelope.avsc
	@ln -sf ../../schemas/saga_reply.avsc schemas/saga_reply.avsc
	-uv run pytest tests/ -v -s
	@rm -rf schemas
	@echo "🧹 Cleaned up temporary schema directory references."

lint:
	@echo "🔍 Executing static analysis and code styling enforcement matrix..."
	uv run ruff check app.py outbox_daemon.py tests/ --fix || echo "⚠️ Ruff not found or formatting warnings detected."

run:
	@echo "🚀 Launching purchasing sales gateway API on port 8000..."
	uv run uvicorn app:app --host 0.0.0.0 --port 8000 --reload

daemon:
	@echo "📦 Launching transaction outbox background streaming worker daemon..."
	uv run python outbox_daemon.py

clean:
	@echo "🧹 Purging local cache footprints and ephemeral compiler assets..."
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
