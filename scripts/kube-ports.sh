#!/bin/sh
# =========================================================================
# 🔌 INTELLIGENT HOOK BACKPORT-FORWARD TUNNEL CONTROLLER
# =========================================================================
set -e

MODE=$1

usage() {
    echo "❌ Error: Missing or invalid port-forwarding command mode."
    echo "💡 Usage: ./scripts/kube-ports.sh [start | stop]"
    exit 1
}

if [ -z "$MODE" ]; then
    usage
fi

# Function to safely check if a deployment has active running pods before tunneling
check_and_forward() {
    LABEL=$1
    SERVICE=$2
    PORTS=$3
    LOG_FILE=$4
    NAME=$5

    # Check for active running pod rows (ignoring the column header line)
    POD_COUNT=$(kubectl get pods -n explorer-zone -l "$LABEL" --field-selector=status.phase=Running 2>/dev/null | wc -l)

    if [ "$POD_COUNT" -gt 1 ]; then
        echo "🟩 $NAME pod is active. Establishing background tunnel on localhost:$PORTS..."
        nohup kubectl port-forward "svc/$SERVICE" "$PORTS" -n explorer-zone > "../../$LOG_FILE" 2>&1 &
    else
        echo "⚠️ [SKIPPED]: $NAME service has no running pods. Tunnel aborted."
    fi
}

case "$MODE" in
    start)
        # 🟢 FIX: Forcefully clear out old, dead ghost tunnels out of Mac memory before spawning fresh ones!
        echo "🛑 Evicting old background tunnels from host memory to prevent port collisions..."
        pkill -f "port-forward" || true
        sleep 1
        
        echo "🔌 Evaluating cluster mesh state before opening host hardware ports..."
        check_and_forward "app=postgres-ledger" "postgres-service" "5432:5432" "kube_port_db.log" "Postgres Ledger"
        check_and_forward "app=sales-order-entry" "sales-order-entry-service" "8000:8000" "kube_port_api.log" "Sales Order API"
        check_and_forward "app=jaeger" "jaeger-query" "16686:16686" "kube_port_jaeger.log" "Jaeger Telemetry"
        check_and_forward "app=jaeger" "jaeger-collector" "4318:4318" "kube_port_otel.log" "Jaeger OTLP"
        check_and_forward "app=enterprise-kafka-broker" "enterprise-kafka-broker" "9092:9092" "kube_port_kafka.log" "Kafka Broker"
        check_and_forward "app=enterprise-kafka-broker" "enterprise-kafka-broker" "8081:8081" "kube_port_registry.log" "Schema Registry"

        echo "✔ Port-forward evaluation phase completed successfully."
        ;;
    stop)
        echo "🛑 Flushing background cluster network port-forward tunnels from host memory..."
        pkill -f "port-forward" || true
        echo "✔ [SUCCESS]: Host network port interfaces have been cleared."
        ;;
    *)
        usage
        ;;
esac
