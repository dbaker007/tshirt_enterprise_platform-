# =========================================================================
# T-SHIRT ENTERPRISE PLATFORM - GLOBAL ORCHESTRATION MANIFEST
# =========================================================================

.PHONY: infra-up infra-down sync-all test-all clean-all status help daemons daemons-stop services services-stop

help:
	@echo "🌐 T-Shirt Enterprise Platform Global Control Panel"
	@echo "=================================================="
	@echo "make infra-up     - Spin up background infrastructure containers (Kafka, Postgres, Apicurio)"
	@echo "make infra-down   - Tear down background infrastructure containers and clear volumes"
	@echo "make sync-all     - Synchronize all package locks across all department repositories"
	@echo "make test-all     - Execute the entire integration test matrix across all modules"
	@echo "make daemons      - Launch all department Outbox Daemons concurrently in the background"
	@echo "make daemons-stop - Gracefully terminate all active background Outbox Daemons"
	@echo "make services     - Launch all live API and Consumer application loops concurrently"
	@echo "make services-stop - Gracefully terminate all active API and Consumer applications"
	@echo "make clean-all    - Purge cache blueprints and logs everywhere"
	@echo "make status       - Display active container runtimes and local python processes"

infra-up:
	@echo "🚀 Launching centralized platform core utility containers..."
	cd platform_infra && docker compose up -d

infra-down:
	@echo "🛑 Tearing down platform core containers and wiping volume allocations..."
	cd platform_infra && docker compose down -v

sync-all:
	@echo "🔧 Synchronizing project dependencies symmetrically via uv..."
	cd sales && uv sync
	cd shipping && uv sync
	cd finance && uv sync
	cd notifications && uv sync # ◄── UPDATED SYMMETRY

test-all:
	$(MAKE) services-stop
	$(MAKE) daemons-stop
	@echo "🧪 Executing Unified Global Integration Test Matrix..."
	@echo "====================================================="
	uv run pytest sales/ shipping/ finance/ notifications/ -v -s \
		--import-mode=importlib \
		-o pythonpath="" \
		-c /dev/null

daemons:
	@echo "📦 Spawning all transactional outbox daemons concurrently out-of-band..."
	@echo "📝 Process logs are streaming straight into local '.log' files..."
	cd sales && uv run python outbox_daemon.py > sales_daemon.log 2>&1 &
	cd shipping && uv run python outbox_daemon.py > shipping_daemon.log 2>&1 &
	cd finance && uv run python outbox_daemon.py > finance_daemon.log 2>&1 &
	cd notifications && uv run python outbox_daemon.py > notifications_daemon.log 2>&1 & # ◄── UPDATED SYMMETRY
	@echo "✔ [SUCCESS]: All Outbox Daemons successfully detached into background memory threads."

daemons-stop:
	@echo "🛑 Intercepting and killing all active background Outbox Daemon processes..."
	-pkill -f "outbox_daemon.py"
	@echo "✔ [SUCCESS]: Outbox streaming loops successfully disengaged."

services:
	@echo "🚀 Spawning Sales Gateway API and Department Graph Consumers concurrently..."
	cd sales && uv run uvicorn app:app --host 0.0.0.0 --port 8000 > sales_api.log 2>&1 &
	cd sales && uv run python saga_orchestrator.py > sales_orchestrator.log 2>&1 & 
	cd shipping && uv run python app.py > shipping_app.log 2>&1 &
	cd finance && uv run python app.py > finance_app.log 2>&1 &
	cd notifications && uv run python app.py > notifications_app.log 2>&1 &
	@echo "✔ [SUCCESS]: Sales Gateway active on port 8000. Consumers actively polling partitions."

services-stop:
	@echo "🛑 Intercepting and killing active application and server runtimes..."
	-pkill -f "uvicorn"
	-pkill -f "saga_orchestrator.py" 
	-pkill -f "python app.py"
	-pkill -f "python3 app.py"
	@echo "✔ [SUCCESS]: System services disengaged cleanly."


clean-all:
	@echo "🧹 Purging local cache footprints and logs..."
	cd sales && make clean
	cd shipping && make clean
	cd finance && make clean
	cd notifications && make clean # ◄── UPDATED SYMMETRY
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -f sales/*.log shipping/*.log finance/*.log notifications/*.log # ◄── UPDATED SYMMETRY

status:
	@echo "🔍 Active Container Runtime Memory Footprint:"
	@docker ps --format "table {{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Names}}"
	@echo "\n🔍 Active Local Background Python Processes:"
	@ps aux | grep -E "python|uvicorn" | grep -v grep || echo "No active background python threads detected."
