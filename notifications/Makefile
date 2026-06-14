# =========================================================================
# NOTIFICATIONS DEPARTMENT LOCAL AUTOMATION PANEL
# =========================================================================

.PHONY: sync lint run daemon test clean

sync:
	@echo "🔧 Synchronizing isolated python project dependencies via uv..."
	uv sync

lint:
	@echo "🔍 Executing static analysis and code styling checks..."
	uv run ruff check app.py outbox_daemon.py --fix || echo "⚠️ Ruff check complete."

run:
	@echo "🚀 Launching notifications asynchronous consumer worker..."
	uv run python app.py

daemon:
	@echo "📦 Launching private notifications background streaming worker daemon..."
	uv run python outbox_daemon.py

test:
	@echo "🧪 Executing notifications consumer integration test suite..."
	uv run pytest tests/ -v -s

clean:
	@echo "🧹 Purging local cache footprints and temporary compiler assets..."
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
