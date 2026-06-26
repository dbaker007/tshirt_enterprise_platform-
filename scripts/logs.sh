#!/usr/bin/env bash
# =========================================================================
# 📝 MULTI-RUNTIME UNIFIED TELEMETRY LOG SHARD TRACKER
# =========================================================================
set -euo pipefail

SERVICE="${1:-}"
NAMESPACE="explorer-zone"

if [ -z "$SERVICE" ]; then
    echo "❌ [ERROR]: Please specify a microservice or infrastructure component name handle."
    echo "📊 Usage: ./scripts/logs.sh <sales-api|orchestrator|shipping|finance|notifications|daemons|postgres|kafka|jaeger|migrator>"
    exit 1
fi

# Standardized flags to allow clean polymorphic routing switches
KUBE_TARGET_TYPE="deployment"
LABEL_SELECTOR=""

case "$SERVICE" in
    # --- APPLICATION SERVICE ENGINES ---
    sales-api|api)
        LOCAL_MATCH="sales.order_entry.main:app"
        LOCAL_FILE="sales_api.log"
        KUBE_DEPLOY="sales-order-entry"
        ;;
    orchestrator|saga)
        LOCAL_MATCH="sales.orchestrator.main"
        LOCAL_FILE="sales_orchestrator.log"
        KUBE_DEPLOY="sales-saga-orchestrator"
        ;;
    shipping)
        LOCAL_MATCH="shipping.app"
        LOCAL_FILE="shipping_app.log"
        KUBE_DEPLOY="shipping-service"
        ;;
    finance)
        LOCAL_MATCH="finance.app"
        LOCAL_FILE="finance_api.log"
        KUBE_DEPLOY="finance-service"
        ;;
    notifications)
        LOCAL_MATCH="notifications.app"
        LOCAL_FILE="notifications_app.log"
        KUBE_DEPLOY="notifications-service"
        ;;
    # 🟢 FIXED: Explicitly maps 'daemons' or 'daemon' to the platform outbox processor!
    outbox|daemon|daemons)
        LOCAL_MATCH="outbox_daemon"
        LOCAL_FILE="outbox_daemon.log"
        KUBE_DEPLOY="outbox-daemon"
        ;;
        
    # --- CORE INFRASTRUCTURE WORKLOAD COMPONENTS ---
    postgres|db|ledger)
        LOCAL_MATCH="---PROXIED_TUNNEL_ONLY---"
        LOCAL_FILE="postgres_local.log"
        KUBE_DEPLOY="postgres-db"
        ;;
    kafka|broker)
        LOCAL_MATCH="---PROXIED_TUNNEL_ONLY---"
        LOCAL_FILE="kafka_local.log"
        KUBE_DEPLOY="enterprise-kafka-broker"
        ;;
    jaeger|telemetry|traces)
        LOCAL_MATCH="---PROXIED_TUNNEL_ONLY---"
        LOCAL_FILE="jaeger_local.log"
        KUBE_DEPLOY="jaeger"
        ;;
    migrator|schema|migration)
        LOCAL_MATCH="---CLUSTER_JOB_ONLY---"
        LOCAL_FILE="migrator_local.log"
        KUBE_TARGET_TYPE="pod"
        LABEL_SELECTOR="job-name=platform-schema-migrator"
        ;;
    *)
        echo "❌ [ERROR]: Unknown application or infrastructure component name: '$SERVICE'"
        exit 1
        ;;
esac

echo "🔎 Investigating runtime deployment state layers for component: [$SERVICE]..."

# --- CHECK LAYER 1: Mac Host Runtime Processes ---
if pgrep -f "$LOCAL_MATCH" >/dev/null 2>&1; then
    if [ -f "$LOCAL_FILE" ]; then
        echo "💻 [LOCAL DETECTED]: Active process ID found on host hardware. Trailing '$LOCAL_FILE' logs..."
        echo "--------------------------------------------------------------------------------"
        exec tail -n 50 -f "$LOCAL_FILE"
    else
        echo "⚠️  Process string matches local hardware, but target log '$LOCAL_FILE' is missing."
    fi
fi

# --- CHECK LAYER 2: Kubernetes Container Control Plane Cluster Mesh ---
if [ "$KUBE_TARGET_TYPE" = "deployment" ]; then
    if kubectl get deployment "$KUBE_DEPLOY" -n "$NAMESPACE" >/dev/null 2>&1; then
        echo "☸️  [KUBE DETECTED]: Sideloaded pod deployment found inside cluster mesh namespace [$NAMESPACE]."
        echo "--------------------------------------------------------------------------------"
        exec kubectl logs "deployment/$KUBE_DEPLOY" -n "$NAMESPACE" -f --tail=50 --all-containers=true
    fi
elif [ "$KUBE_TARGET_TYPE" = "pod" ]; then
    if kubectl get pods -n "$NAMESPACE" -l "$LABEL_SELECTOR" >/dev/null 2>&1; then
        echo "☸️  [KUBE BATCH JOB DETECTED]: Cluster execution pods matching label selection [$LABEL_SELECTOR]."
        echo "--------------------------------------------------------------------------------"
        exec kubectl logs -n "$NAMESPACE" -l "$LABEL_SELECTOR" -f --tail=50
    fi
fi

echo "❌ [ERROR]: Component '$SERVICE' does not appear to be running on your local Mac host or inside Kubernetes!"
exit 1
