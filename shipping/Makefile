# =========================================================================
# SHIPPING DEPARTMENT LOCAL AUTOMATION NETWORK
# =========================================================================

.PHONY: sync lint run clean

sync:
	@echo "🔧 Synchronizing isolated python project dependencies via uv..."
	uv sync

lint:
	@echo "🔍 Executing static analysis and code styling checks..."
	uv run ruff check app.py --fix || echo "⚠️ Ruff not found or formatting warnings detected."

run:
	@echo "🚀 Launching shipping asynchronous logistics consumer worker..."
	uv run python app.py

clean:
	@echo "🧹 Purging local cache footprints and temporary compiler assets..."
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +

test:
	@echo "🧪 Executing shipping consumer integration test suite..."
	uv run pytest . -v -s

daemon:
	@echo "📦 Launching private shipping outbox background streaming worker daemon..."
	uv run python outbox_daemon.py
