# =========================================================================
# T-SHIRT ENTERPRISE PLATFORM - GLOBAL ORCHESTRATION MANIFEST
# =========================================================================

.PHONY: bootstrap infra-up infra-down sync-all test-all clean-all status help daemons daemons-stop services services-stop db-status db-outbox db-ledgers kafka-lag kafka-offsets db-trace-audit

help:
	@echo "🌐 T-Shirt Enterprise Platform Global Control Panel"
	@echo "=================================================="
	@echo "make bootstrap    - Zero-friction environment setup (Infra + Sync + Test)"
	@echo "make infra-up     - Spin up background infrastructure containers"
	@echo "make infra-down   - Tear down background infrastructure containers and clear volumes"
	@echo "make sync-all     - Synchronize all package locks across repositories"
	@echo "make test-all     - Execute the entire integration test matrix across all modules"
	@echo "make daemons      - Launch all department Outbox Daemons concurrently"
	@echo "make daemons-stop - Gracefully terminate all active background Outbox Daemons"
	@echo "make services     - Launch all live API and Consumer application loops concurrently"
	@echo "make services-stop - Gracefully terminate all active API and Consumer applications"
	@echo "make clean-all    - Purge cache blueprints and logs everywhere"
	@echo "make status       - Display active container runtimes and local python processes"

# 🛠️ THE TECH LEAD ZERO-FRICTION BOOTSTRAP GATEWAY
bootstrap:
	@echo "🏁 Commencing Global Platform Bootstrapping Sequence..."
	@echo "====================================================="
	$(MAKE) infra-up
	@echo "⏳ Waiting 8 seconds for Kafka and PostgreSQL container clusters to warm up..."
	@sleep 8
	@echo "🔧 Compiling and linking workspace department modules via uv..."
	$(MAKE) sync-all
	@echo "✔  Shared virtual environment initialized successfully."
	$(MAKE) test-all
	@echo "====================================================="
	@echo "🎉 BOOTSTRAP COMPLETE: Platform is 100% verified and operational."

infra-up:
	@echo "🚀 Launching centralized platform core utility containers..."
	cd platform_infra && docker compose up -d

infra-down:
	@echo "🛑 Tearing down platform core containers and wiping volume allocations..."
	cd platform_infra && docker compose down -v --remove-orphans

sync-all:
	@echo "🔧 Synchronizing project dependencies symmetrically via uv..."
	uv sync

test-all:
	@echo "🧪 Executing Unified Global Integration Test Matrix..."
	@echo "====================================================="
	uv run pytest sales/ shipping/ finance/ notifications/ -v -s --import-mode=importlib

services:
	@echo "🚀 Spawning Application Graph Consumers with real-time log flushing..."
	(cd sales/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run uvicorn sales.app:app --host 0.0.0.0 --port 8000 > ../../sales_api.log 2>&1 &)
	(cd sales/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run python -u -m sales.saga_orchestrator > ../../sales_orchestrator.log 2>&1 &)
	(cd shipping/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run python -u -m shipping.app > ../../shipping_app.log 2>&1 &)
	(cd finance/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run python -u -m finance.app > ../../finance_app.log 2>&1 &)
	(cd notifications/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run python -u -m notifications.app > ../../notifications_app.log 2>&1 &)
	@echo "✔ [SUCCESS]: Application consumers safely aligned to execution targets."

daemons:
	@echo "🚀 Spawning Concrete Outbox Engines with real-time log flushing..."
	(cd sales/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run python -u -m sales.outbox_daemon > ../../sales_daemon.log 2>&1 &)
	(cd shipping/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run python -m shipping.outbox_daemon > ../../shipping_daemon.log 2>&1 &)
	(cd finance/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run python -u -m finance.outbox_daemon > ../../finance_daemon.log 2>&1 &)
	(cd notifications/src && OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 uv run python -u -m notifications.outbox_daemon > ../../notifications_daemon.log 2>&1 &)
	@echo "✔ [SUCCESS]: Shared Outbox Daemons safely spawned."

daemons-stop:
	@echo "🛑 Intercepting and killing all active background Outbox Daemon processes..."
	-pkill -f "outbox_daemon"
	@echo "✔ [SUCCESS]: Outbox streaming loops successfully disengaged."

services-stop:
	@echo "🛑 Intercepting and killing active application and server runtimes..."
	-pkill -f "uvicorn"
	-pkill -f "sales.saga_orchestrator"
	-pkill -f "shipping.app"
	-pkill -f "finance.app"
	-pkill -f "notifications.app"
	@echo "✔ [SUCCESS]: System services disengaged cleanly."

clean-all:
	@echo "🧹 Purging local cache footprints and logs..."
	cd sales && rm -rf .pytest_cache .pydantic_cache .ruff_cache build dist *.egg-info
	cd shipping && rm -rf .pytest_cache .pydantic_cache .ruff_cache build dist *.egg-info
	cd finance && rm -rf .pytest_cache .pydantic_cache .ruff_cache build dist *.egg-info
	cd notifications && rm -rf .pytest_cache .pydantic_cache .ruff_cache build dist *.egg-info
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -f *.log sales/*.log shipping/*.log finance/*.log notifications/*.log

status:
	@echo "🔍 Active Container Runtime Memory Footprint:"
	@docker ps --format "table {{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Names}}"
	@echo "\n🔍 Active Local Background Python Processes:"
	@ps aux | grep -E "python|uvicorn" | grep -v grep || echo "No active background python threads detected."

# =========================================================================
# 🏆 MASTER PLATFORM LIFE CYCLE RUNNERS
# =========================================================================

all-down:
	@echo "🛑 [ALL-DOWN]: Terminating local microservices mesh, background daemons, and storage stacks..."
	-$(MAKE) services-stop
	-$(MAKE) daemons-stop
	-$(MAKE) infra-down
	@echo "✔ [SUCCESS]: Entire platform hard footprint dismantled cleanly."

all-up:
	@echo "🚀 [ALL-UP]: Initializing brand-new infrastructure containers..."
	$(MAKE) infra-up
	@echo "⏳ [SLEEP ALERT]: Holding thread for 8 seconds to guarantee complete container synchronization..."
	sleep 8
	@echo "📡 [ALL-UP]: Launching Application Graph Consumers and Concrete Outbox Daemons concurrently..."
	$(MAKE) services
	$(MAKE) daemons
	@echo "✔ [SUCCESS]: Distributed mesh fully active. Ready to ingest 'uv run simulate_order.py' payloads!"

# ==============================================================================
# 🕵️ ENTERPRISE DIAGNOSTIC & OBSERVABILITY SHORCUT MATRIX
# ==============================================================================

# 1. Real-Time Master Status Aggregate Dashboard
db-status:
	@echo "\n📊 [SAGA ENGINE]: Current Master State Distribution..."
	@echo "====================================================="
	@docker exec -it enterprise_postgres_ledger psql -U platform_admin -d platform_shared_ledger -c \
		"SELECT saga_status, COUNT(*) FROM saga_states GROUP BY saga_status ORDER BY COUNT(*) DESC;"

# 2. Check the Transient Outbox Table Volumes
db-outbox:
	@echo "\n📥 [DATA PIPELINE]: Transient Outbox Table Record Backlogs..."
	@echo "==========================================================="
	@echo "Sales Outbox Count:"
	@docker exec -it enterprise_postgres_ledger psql -U platform_admin -d platform_shared_ledger -t -c "SELECT COUNT(*) FROM sales_outbox;" | tr -d ' '
	@echo "Finance Outbox Count:"
	@docker exec -it enterprise_postgres_ledger psql -U platform_admin -d platform_shared_ledger -t -c "SELECT COUNT(*) FROM finance_outbox;" | tr -d ' '
	@echo "Shipping Outbox Count:"
	@docker exec -it enterprise_postgres_ledger psql -U platform_admin -d platform_shared_ledger -t -c "SELECT COUNT(*) FROM shipping_outbox;" | tr -d ' '
	@echo "Notification Outbox Count:"
	@docker exec -it enterprise_postgres_ledger psql -U platform_admin -d platform_shared_ledger -t -c "SELECT COUNT(*) FROM notification_outbox;" | tr -d ' '

# 3. Cross-Department Column Matrix Group-By Audit
db-ledgers:
	@echo "\n🔬 [LEDGER AUDIT]: Shifting Microservice Checklist Metrics..."
	@echo "============================================================="
	@docker exec -it enterprise_postgres_ledger psql -U platform_admin -d platform_shared_ledger -c \
		"SELECT finance_status, shipping_status, notifications_status, COUNT(*) FROM saga_states WHERE saga_status = 'STARTED' GROUP BY finance_status, shipping_status, notifications_status;"

# 4. View Real-Time Kafka Consumer Group Lag
kafka-lag:
	@echo "\n📡 [BROKER NETWORK]: Active Consumer Group lag Matrix..."
	@echo "======================================================="
	@docker exec -it enterprise_kafka_broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups

# 5. Interrogate Raw Wire Partition Log-End Offsets
kafka-offsets:
	@echo "\n📡 [BROKER WIRE]: Current Raw Partition Message Offsets..."
	@echo "=========================================================="
	@docker exec -it enterprise_kafka_broker /usr/bin/kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic saga_replies --time -1

# 6. Audit Trace Context Payloads Passing the Ledger
db-trace-audit:
	@echo "\n🔏 [TRACE METRICS]: Last 5 Traceparent Context Signatures on Disk..."
	@echo "=================================================================="
	@docker exec -it enterprise_postgres_ledger psql -U platform_admin -d platform_shared_ledger -c \
		"SELECT order_id, saga_status, created_at FROM saga_states ORDER BY created_at DESC LIMIT 5;"
