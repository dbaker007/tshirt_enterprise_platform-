SHELL := /bin/sh
.DEFAULT_GOAL := help

# =========================================================================
# ⚙️ SYSTEM WORKSPACE AUTOMATION COMMANDS
# =========================================================================

.PHONY: help
help: ## Display this workspace help matrix map cleanly on your screen
	@echo "💡  Start Docker: open -a Docker"
	@echo "💡 Make sure to open a separate terminal tab and run: sudo cloud-provider-kind"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# =========================================================================
# 🎛️ SYSTEM INFRASTRUCTURE BASELINES (Idempotent Namespace Guard)
# =========================================================================

.PHONY: kube-infra-start
kube-infra-start: kube-namespace-init ## Deploy and initialize complete infrastructure core stack sequentially to prevent discovery collisions
	@echo "🗄️  1. Provisioning database schema ConfigMaps from local platform_infra assets..."
	@kubectl create configmap postgres-schema-config --from-file=schema.sql=platform_infra/postgres-schema.sql -n explorer-zone --dry-run=client -o yaml | kubectl apply -f -

	@echo "🐳  2. Pre-caching and side-loading native PostgreSQL image layers directly into containerd..."
	@docker pull postgres:15-bookworm
	@docker save postgres:15-bookworm | docker exec -i sandbox-fabric-control-plane ctr --namespace=k8s.io images import -
	
	@echo "🐳  3. Pre-caching and side-loading Apache Kafka and Schema Registry image layers directly into containerd..."
	@docker pull apache/kafka:3.7.0
	@docker save apache/kafka:3.7.0 | docker exec -i sandbox-fabric-control-plane ctr --namespace=k8s.io images import -
	@docker pull confluentinc/cp-schema-registry:7.6.0
	@docker save confluentinc/cp-schema-registry:7.6.0 | docker exec -i sandbox-fabric-control-plane ctr --namespace=k8s.io images import -
	
	@echo "🗄️  4. Standing up centralized transactional PostgreSQL Database Shard..."
	@kubectl apply -f platform_infra/postgres-db.yaml
	
	@echo "⏳ Waiting for PostgreSQL PersistentVolumeClaim to bind to host disk..."
	@kubectl wait --namespace explorer-zone --for=jsonpath='{.status.phase}'=Bound pvc/postgres-storage-claim --timeout=60s
	
	@echo "⏳ Waiting for PostgreSQL container storage volumes to mount cleanly..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/postgres-db --timeout=120s
	@echo "📊 5. Standing up centralized Jaeger Distributed Telemetry Core..."
	@kubectl apply -f platform_infra/jaeger.yaml
	@echo "⏳ Waiting for Jaeger collector engine network sockets to initialize..."
	@kubectl rollout status deployment/jaeger -n explorer-zone --timeout=120s
	@echo "🏁 6. Allowing cluster CoreDNS service discovery networks to stabilize..."
	@sleep 10
	@echo "📡 7. Standing up dual-listener KRaft Message Bus and Schema Registry sidecar..."
	@kubectl apply -f platform_infra/enterprise-kafka-broker.yaml
	@echo "⏳ Waiting for KRaft broker rollout stream to finalize..."
	@kubectl rollout status deployment/enterprise-kafka-broker -n explorer-zone --timeout=120s
	
	@echo "✔ [SUCCESS]: Core platform infrastructure tier is fully operational."


.PHONY: kube-platform-start
kube-platform-start: kube-cluster-init kube-infra-start ## Provision cluster, initialize core infrastructure, and execute rolling starts for all microservices
	@echo "🚀 Launching complete cluster application services layer..."
	@$(MAKE) kube-sales-api-start
	@$(MAKE) kube-orchestrator-start
	@$(MAKE) kube-shipping-start
	@$(MAKE) kube-finance-start
	@$(MAKE) kube-finance-api-start
	@$(MAKE) kube-ops-agent-start
	@$(MAKE) kube-notifications-start
	@$(MAKE) kube-outbox-daemon-start
	@$(MAKE) kube-ports-start
	@echo "✔ [SUCCESS]: Complete cluster application fabric grid is fully initialized."

.PHONY: kube-platform-stop
kube-platform-stop: ## Forcefully wipe all workloads, background pods, and temporary migration jobs from the cluster namespace
	@echo "🛑 Flushing all declarative deployment pods and transient jobs from cluster memory..."
	@kubectl delete deployments,statefulsets,replicasets,jobs,pods --all -n explorer-zone --force --grace-period=0 || true
	@$(MAKE) kube-ports-stop
	@echo "✔ [SUCCESS]: Complete cluster workspace canvas has been reset."

.PHONY: kube-cluster-init
kube-cluster-init: ## Provision a fresh, isolated local Kubernetes node instance from raw host hardware
	@echo "🏗️  Bootstrapping fresh Kind local cluster instance..."
	@if kind get clusters 2>/dev/null | grep -q "^sandbox-fabric$$"; then \
		echo "⚠️  Cluster 'sandbox-fabric' already exists on host. Skipping creation pass."; \
	else \
		kind create cluster --name sandbox-fabric; \
		echo "✔  Cluster context successfully bound to host kubeconfig profile."; \
		echo "⏳ Waiting for Kubernetes API server core layers to initialize..."; \
		until kubectl get namespace kube-system >/dev/null 2>&1; do sleep 2; done; \
		echo "⏳ Waiting for internal cluster core storage engine controllers to register..."; \
		until kubectl get deployment --all-namespaces | grep -q "local-path-provisioner"; do sleep 2; done; \
		echo "🟢 Patching Kind storage engine to enforce the cluster-wide default storage class..."; \
		until kubectl get storageclass standard >/dev/null 2>&1 || kubectl get storageclass local-path >/dev/null 2>&1; do sleep 2; done; \
		if kubectl get storageclass standard >/dev/null 2>&1; then \
			kubectl patch storageclass standard -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'; \
		else \
			kubectl patch storageclass local-path -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'; \
		fi; \
		echo "✔  Cluster system infrastructure layers are stable and ready for workloads."; \
	fi




.PHONY: local-platform-start
local-platform-start: system-init  ## Spawn ALL decoupled background application graph consumers concurrently on your Mac host hardware
	@echo "🚀 Spawning host application graph consumers with real-time log flushing..."
	@$(MAKE) local-sales-api-start
	@$(MAKE) local-orchestrator-start
	@$(MAKE) local-shipping-start
	@$(MAKE) local-finance-start
	@$(MAKE) local-notifications-start
	@$(MAKE) local-finance-api-start
	@$(MAKE) local-ops-agent-start
	@echo "✔ [SUCCESS]: All local host application services safely aligned to execution targets."

.PHONY: local-platform-stop
local-platform-stop: ## Forcefully terminate all detached background uv/uvicorn processes running on your Mac Mini
	@echo "🛑 Sweeping detached background host processes from memory..."
	@pkill -f "uv run" || true
	@pkill -f "uvicorn" || true
	@echo "✔ [SUCCESS]: Local host execution canvas reset completed."

# =========================================================================
# 🔬 CONCRETE WORKLOAD RULES - DOMAIN SPECIFIC TARGETS
# =========================================================================

# --- DOMAIN: SALES WEB GATEWAY ---
.PHONY: kube-sales-api-start
kube-sales-api-start: kube-namespace-init ## Compile, sideload, and execute a self-healing rolling update bounce on cluster Sales FastAPI gateway
	@echo "📦 Compiling cluster Sales Order Entry image..."
	@docker build --no-cache -t tshirt-enterprise-platform/sales-order-entry:latest -f platform_infra/sales-order-entry.Dockerfile .
	@kind load docker-image tshirt-enterprise-platform/sales-order-entry:latest --name sandbox-fabric
	@kubectl apply -f platform_infra/sales-order-entry.yaml
	@echo "🚀 Restarting gateway pods inside the cluster mesh..."
	@kubectl rollout restart deployment/sales-order-entry -n explorer-zone
	@echo "⏳ Waiting for fresh gateway container sockets to pass readiness gates..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/sales-order-entry --timeout=90s
	@echo "🔌 Self-Healing: Refreshing background host network port-forward tunnels..."
	@$(MAKE) kube-ports-start
	@echo "✔ [SUCCESS]: Sales API gateway is live, updated, and bridged to localhost:8000."

.PHONY: local-sales-api-start
local-sales-api-start: system-init ## Launch the Sales FastAPI gateway service locally on your Mac Mini host ports
	@echo "🔌 Starting local Sales Order Entry API Gateway on port 8000 with workspace watch modules active..."
	@PYTHONPATH="sales/src:observability/src" OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 \
		uv run uvicorn sales.order_entry.main:app \
			--host 0.0.0.0 \
			--port 8000 \
			--reload \
			--reload-dir sales \
			--reload-dir ui \
			> sales_api.log 2>&1 &


# --- DOMAIN: SAGA ORCHESTRATOR ---
.PHONY: kube-orchestrator-start
kube-orchestrator-start: kube-namespace-init ## Compile, sideload, and execute a rolling update bounce on cluster Saga Orchestrator engine
	@echo "📦 Compiling cluster Sales Saga Orchestrator image..."
	@docker build --no-cache -t tshirt-enterprise-platform/sales-saga-orchestrator:latest -f platform_infra/sales-saga-orchestrator.Dockerfile .
	@kind load docker-image tshirt-enterprise-platform/sales-saga-orchestrator:latest --name sandbox-fabric
	@kubectl apply -f platform_infra/sales-saga-orchestrator.yaml
	@echo "🚀 Restarting orchestrator pods inside the cluster mesh..."
	@kubectl rollout restart deployment/sales-saga-orchestrator -n explorer-zone
	@echo "⏳ Waiting for fresh orchestrator container sockets to pass readiness gates..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/sales-saga-orchestrator --timeout=90s

.PHONY: local-orchestrator-start
local-orchestrator-start: system-init ## Launch the Saga Orchestrator engine process locally on your Mac Mini host hardware
	@echo "🎛️ Starting local Sales Saga Orchestrator engine..."
	@PYTHONPATH="sales/src:observability/src" OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 \
		uv run python -u -m sales.orchestrator.main > sales_orchestrator.log 2>&1 &

# --- DOMAIN: SHIPPING ---
.PHONY: kube-shipping-start
kube-shipping-start: kube-namespace-init ## Compile, sideload, and execute a rolling update bounce on cluster Shipping consumer worker
	@echo "📦 Compiling cluster Shipping consumer service image..."
	@docker build --no-cache -t tshirt-enterprise-platform/shipping-service:latest -f platform_infra/shipping-service.Dockerfile .
	@kind load docker-image tshirt-enterprise-platform/shipping-service:latest --name sandbox-fabric
	@kubectl apply -f platform_infra/shipping-service.yaml
	@echo "🚀 Restarting shipping consumer pods inside the cluster mesh..."
	@kubectl rollout restart deployment/shipping-service -n explorer-zone
	@echo "⏳ Waiting for fresh shipping consumer container sockets to initialize..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/shipping-service --timeout=90s
	@echo "✔ [SUCCESS]: Shipping consumer application worker is live inside the cluster."

.PHONY: local-shipping-start
local-shipping-start: system-init ## Launch the Shipping fulfillment microservice locally on your Mac Mini host hardware
	@echo "🚚 Starting local Shipping Consumer Worker..."
	@PYTHONPATH="shipping/src:observability/src" OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 \
		uv run python -u -m shipping.app > shipping_app.log 2>&1 &

# --- DOMAIN: FINANCE ---
.PHONY: local-finance-start
local-finance-start: system-init ## Launch the Finance accounting microservice locally on your Mac Mini host hardware
	@echo "💰 Starting local Finance Consumer Worker..."
	@PYTHONPATH="finance/src:observability/src" OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 \
		uv run python -u -m finance.app > finance_app.log 2>&1 &

# --- DOMAIN: NOTIFICATIONS ---
.PHONY: local-notifications-start
local-notifications-start: system-init ## Launch the Notifications consumer microservice locally on your Mac Mini host hardware
	@echo "🔔 Starting local Notifications Consumer Worker..."
	@PYTHONPATH="notifications/src:observability/src" OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 \
		uv run python -u -m notifications.app > notifications_app.log 2>&1 &

# =========================================================================
# 🛠️ INTERNAL CLUSTER SUBSYSTEMS (Private targets wrapped by kube-infra-start)
# =========================================================================

.PHONY: kube-db-start
kube-db-start: kube-namespace-init
	@kubectl create configmap postgres-schema-config --from-file=schema.sql=platform_infra/postgres-schema.sql -n explorer-zone --dry-run=client -o yaml | kubectl apply -f -
	@kubectl apply -f platform_infra/postgres-db.yaml
	@kubectl rollout restart deployment/postgres-db -n explorer-zone
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/postgres-db --timeout=90s
	@kubectl delete job platform-schema-migrator -n explorer-zone --ignore-not-found=true
	@kubectl apply -f platform_infra/postgres-migrator-job.yaml

.PHONY: kube-kafka-start
kube-kafka-start: kube-namespace-init
	@kubectl apply -f platform_infra/enterprise-kafka-broker.yaml
	@kubectl rollout restart deployment/enterprise-kafka-broker -n explorer-zone
	@kubectl wait --namespace explorer-zone --for=condition=ready pod -l app=enterprise-kafka-broker --timeout=90s

.PHONY: kube-jaeger-start
kube-jaeger-start: kube-namespace-init
	@kubectl apply -f platform_infra/jaeger.yaml
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/jaeger --timeout=90s

.PHONY: kube-outbox-daemon-start
kube-outbox-daemon-start: kube-namespace-init ## Compile, sideload, and execute a rolling update bounce on cluster Outbox Daemon
	@echo "🗄️ Compiling cluster Universal Outbox Daemon service image..."
	@docker build --no-cache -t tshirt-enterprise-platform/outbox-daemon:latest -f platform_infra/outbox-daemon.Dockerfile .
	@kind load docker-image tshirt-enterprise-platform/outbox-daemon:latest --name sandbox-fabric
	@kubectl apply -f platform_infra/outbox-daemon.yaml
	@echo "🚀 Restarting outbox daemon pods inside the cluster mesh..."
	@kubectl rollout restart deployment/outbox-daemon -n explorer-zone
	@echo "⏳ Waiting for fresh outbox daemon container sockets to initialize..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/outbox-daemon --timeout=90s
	@echo "✔ [SUCCESS]: Universal Outbox Daemon is live inside the cluster."

.PHONY: kube-ports-start
kube-ports-start: kube-namespace-init ## Launch all background network ingress tunnels intelligently, verifying active pods are ready
	@./scripts/kube-ports.sh start

.PHONY: kube-ports-stop
kube-ports-stop: kube-namespace-init ## Forcefully evict and terminate all active background port-forwarding tunnels from Mac memory
	@./scripts/kube-ports.sh stop

.PHONY: system-status
system-status: ## Interrogate and render real-time mixed runtime matrix status grids (Kube vs Local Host)
	@./scripts/system-status.sh

.PHONY: system-init
system-init: ## Private target ensuring cluster namespace partition and host local network loops are aligned
	@kubectl create namespace explorer-zone --dry-run=client -o yaml | kubectl apply -f -
	@if ! grep -q "enterprise-kafka-broker" /etc/hosts; then \
		echo "🌐 Host Routing Patch Required. Injecting local cluster broker alias mapping to your /etc/hosts file..."; \
		sudo sh -c "echo '127.0.0.1 enterprise-kafka-broker' >> /etc/hosts"; \
	fi

# --- DOMAIN: SALES WEB GATEWAY ---
.PHONY: local-sales-api-stop
local-sales-api-stop: ## Forcefully terminate ONLY the local host Sales API process
	@echo "🛑 Stopping local Sales Order Entry API Gateway..."
	@pkill -9 -f "sales.order_entry.main:app" || true

# --- DOMAIN: SAGA ORCHESTRATOR ---
.PHONY: local-orchestrator-stop
local-orchestrator-stop: ## Forcefully terminate ONLY the local host Saga Orchestrator process
	@echo "🛑 Stopping local Sales Saga Orchestrator engine..."
	@pkill -9 -f "sales.orchestrator.main" || true

# --- DOMAIN: SHIPPING ---
.PHONY: local-shipping-stop
local-shipping-stop: ## Forcefully terminate ONLY the local host Shipping worker process
	@echo "🛑 Stopping local Shipping Consumer Worker..."
	@pkill -9 -f "shipping.app" || true

# --- DOMAIN: FINANCE ---
.PHONY: local-finance-stop
local-finance-stop: ## Forcefully terminate ONLY the local host Finance worker process
	@echo "🛑 Stopping local Finance Consumer Worker..."
	@pkill -9 -f "finance.app" || true

# --- DOMAIN: NOTIFICATIONS ---
.PHONY: local-notifications-stop
local-notifications-stop: ## Forcefully terminate ONLY the local host Notifications worker process
	@echo "🛑 Stopping local Notifications Consumer Worker..."
	@pkill -9 -f "notifications.app" || true

# --- DOMAIN: FINANCE ---
.PHONY: kube-finance-start
kube-finance-start: system-init ## Compile, sideload, and execute a rolling update bounce on cluster Finance consumer worker
	@echo "💰 Compiling cluster Finance consumer service image..."
	@docker build --no-cache -t tshirt-enterprise-platform/finance-service:latest -f platform_infra/finance-service.Dockerfile .
	@kind load docker-image tshirt-enterprise-platform/finance-service:latest --name sandbox-fabric
	@kubectl apply -f platform_infra/finance-service.yaml
	@echo "🚀 Restarting finance consumer pods inside the cluster mesh..."
	@kubectl rollout restart deployment/finance-service -n explorer-zone
	@echo "⏳ Waiting for fresh finance consumer container sockets to initialize..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/finance-service --timeout=90s
	@echo "✔ [SUCCESS]: Finance consumer application worker is live inside the cluster."

.PHONY: kube-notifications-start
kube-notifications-start: system-init ## Compile, sideload, and execute a rolling update bounce on cluster Notifications consumer worker
	@echo "🔔 Compiling cluster Notifications consumer service image..."
	@docker build --no-cache -t tshirt-enterprise-platform/notifications-service:latest -f platform_infra/notifications-service.Dockerfile .
	@kind load docker-image tshirt-enterprise-platform/notifications-service:latest --name sandbox-fabric
	@kubectl apply -f platform_infra/notifications-service.yaml
	@echo "🚀 Restarting notifications consumer pods inside the cluster mesh..."
	@kubectl rollout restart deployment/notifications-service -n explorer-zone
	@echo "⏳ Waiting for fresh notifications consumer container sockets to initialize..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/notifications-service --timeout=90s
	@echo "✔ [SUCCESS]: Notifications consumer application worker is live inside the cluster."

# =========================================================================
# 🧪 PLATFORM TESTING TRACKS (Cross-Domain Test Matrix Verification)
# =========================================================================
.PHONY: test-all
test-all: ## Execute all microservice unit tests sequentially and fail-fast if any single test drops out
	@echo "🧪 [TEST MATRIX]: Initiating full platform verification sweeps completely offline..."
	@echo "🔍 0. Executing static code compilation analysis across all packages..."
	@uv run python -c "import compileall, sys; sys.exit(0 if all([compileall.compile_dir(d, quiet=1) for d in ['finance', 'shipping', 'notifications', 'sales', 'outbox_daemon', 'observability']]) else 1)" || (echo "❌ [COMPILE ERROR]: Syntax or import violations detected in your codebase!" && exit 1)
	@echo "✔  [SUCCESS]: Static compilation analysis cleared clean."
	@FAILED=0; \
	echo "--------------------------------------------------------------------------------="; \
	echo "🛍️  1. Running Sales Domain Test Matrix..."; \
	(PYTHONPATH=".:sales/src:ui" DATABASE_URL="sqlite:///:memory:" OTEL_TRACES_EXPORTER="none" uv run pytest sales/tests/) || FAILED=1; \
	echo "--------------------------------------------------------------------------------="; \
	echo "💰 2. Running Finance Domain Test Matrix..."; \
	(DATABASE_URL="sqlite:///:memory:" OTEL_TRACES_EXPORTER="none" uv run pytest finance/tests/) || FAILED=1; \
	echo "--------------------------------------------------------------------------------="; \
	echo "🚚 3. Running Shipping Domain Test Matrix..."; \
	(DATABASE_URL="sqlite:///:memory:" OTEL_TRACES_EXPORTER="none" uv run pytest shipping/tests/) || FAILED=1; \
	echo "--------------------------------------------------------------------------------="; \
	echo "🔔 4. Running Notifications Domain Test Matrix..."; \
	(DATABASE_URL="sqlite:///:memory:" OTEL_TRACES_EXPORTER="none" uv run pytest notifications/tests/) || FAILED=1; \
	echo "--------------------------------------------------------------------------------="; \
	echo "🗄️  5. Running Observability Shared Library Test Matrix..."; \
	(DATABASE_URL="sqlite:///:memory:" OTEL_TRACES_EXPORTER="none" uv run pytest observability/tests/) || FAILED=1; \
	echo "--------------------------------------------------------------------------------="; \
	echo "📤 6. Running Universal Outbox Daemon Test Matrix..."; \
	(DATABASE_URL="sqlite:///:memory:" OTEL_TRACES_EXPORTER="none" uv run pytest outbox_daemon/tests/) || FAILED=1; \
	echo "================================================================================="; \
	if [ $$FAILED -ne 0 ]; then \
		echo "❌ [FAILURE]: One or more test suites failed verification. Aborting build matrix."; \
		exit 1; \
	fi; \
	echo "✔ [SUCCESS]: Complete platform testing matrix passed verification checks!"

.PHONY: kube-namespace-init
kube-namespace-init: kube-cluster-init ## Provision and isolate the core platform partition zone inside the cluster
	@echo "🛡️  Initializing isolated cluster logical partition namespace..."
	@if ! kubectl get namespace explorer-zone >/dev/null 2>&1; then \
		kubectl create namespace explorer-zone; \
		echo "✔  Namespace 'explorer-zone' successfully established."; \
	else \
		echo "⚠️  Namespace 'explorer-zone' already initialized. Skipping creation block."; \
	fi

.PHONY: test-integration
test-integration: ## Execute complete cross-domain integration test suite against the active cluster mesh
	@echo "📡 Running cross-domain Saga integration tests against live cluster..."
	@uv run pytest tests/integration/ -v -s

.PHONY: local-ops-agent-start
local-ops-agent-start: system-init ## Launch the Ops Agent natural language automation service locally
	@echo "🔌 Starting local Ops Agent Engine on port 8005..."
	@PYTHONPATH="ops_agent/src:observability/src" \
		OTEL_PROPAGATORS=tracecontext OTEL_TRACE_FLAGS=01 \
		uv run uvicorn ops_agent.main:app --host 0.0.0.0 --port 8005 --reload > ops_agent.log 2>&1 &

.PHONY: local-finance-api-start
local-finance-api-start: ## Launch the Finance programmatic FastAPI web view endpoint engine locally on port 8001
	@echo "🔌 Starting local Finance Data Shard Web API on port 8001..."
	@PYTHONPATH="finance/src:observability/src" \
		uv run uvicorn finance.web:app --host 0.0.0.0 --port 8001 > finance_api.log 2>&1 &

.PHONY: kube-finance-api-start
kube-finance-api-start: system-init ## Compile, sideload, and execute a rolling update bounce on cluster Finance web API server
	@echo "💰 Compiling cluster Finance data shard web API service image..."
	@docker build --no-cache -t tshirt-enterprise-platform/finance-api:latest -f platform_infra/finance-api.Dockerfile .
	@kind load docker-image tshirt-enterprise-platform/finance-api:latest --name sandbox-fabric
	@kubectl apply -f platform_infra/finance-api.yaml
	@echo "🚀 Restarting finance programmatic web API pods inside the cluster mesh..."
	@kubectl rollout restart deployment/finance-api -n explorer-zone
	@echo "⏳ Waiting for fresh finance web API container sockets to initialize..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/finance-api --timeout=90s
	@$(MAKE) kube-ports-start
	@echo "✔ [SUCCESS]: Finance programmatic web view engine API is live inside the cluster mesh."

.PHONY: local-ops-agent-stop
local-ops-agent-stop: ## Forcefully terminate ONLY the local host Ops Agent process
	@echo "🛑 Stopping local Ops Agent Engine..."
	@pkill -9 -f "ops_agent.main" || true

.PHONY: local-finance-api-stop
local-finance-api-stop: ## Forcefully terminate ONLY the local host Finance Web API process
	@echo "🛑 Stopping local Finance Data Shard Web API..."
	@pkill -9 -f "finance.web" || true

.PHONY: kube-ops-agent-start
kube-ops-agent-start: system-init ## Compile, sideload, and execute a rolling update bounce on cluster AI Operations Agent server
	@echo "🧠 Compiling cluster AI Operations Agent service image..."
	@docker build --no-cache -t tshirt-enterprise-platform/ops-agent:latest -f platform_infra/ops-agent.Dockerfile .
	@kind load docker-image tshirt-enterprise-platform/ops-agent:latest --name sandbox-fabric
	@kubectl apply -f platform_infra/ops-agent.yaml
	@echo "🚀 Restarting AI Operations Agent reasoning pods inside the cluster mesh..."
	@kubectl rollout restart deployment/ops-agent -n explorer-zone
	@echo "⏳ Waiting for fresh AI Operations Agent container sockets to initialize..."
	@kubectl wait --namespace explorer-zone --for=condition=available deployment/ops-agent --timeout=90s
	@$(MAKE) kube-ports-start
	@echo "✔ [SUCCESS]: AI Operations Agent reasoning engine is live inside the cluster mesh."
