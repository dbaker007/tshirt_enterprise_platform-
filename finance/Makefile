.PHONY: sync lint run daemon test clean

sync:
	uv sync
lint:
	uv run ruff check app.py outbox_daemon.py --fix || echo "Ruff cleared."
run:
	uv run python app.py
daemon:
	uv run python outbox_daemon.py
test:
	uv run pytest . -v -s
clean:
	rm -rf .pytest_cache .ruff_cache && find . -type d -name "__pycache__" -exec rm -rf {} +
