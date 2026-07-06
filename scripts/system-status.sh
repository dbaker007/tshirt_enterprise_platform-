#!/bin/sh
# =========================================================================
# 📊 PLATFORM WORKSPACE METRIC MONITOR & MATRIX CHECKPOINT INSPECTOR
# =========================================================================
set -e

clear
echo "\033[1;35m📊 [PLATFORM GRAPH MATRIX]: Live Multi-Runtime Status Dashboard\033[0m"
echo "================================================================================="
printf "\033[1;36m%-28s %-12s %-10s %-25s\033[0m\n" "COMPONENT/SERVICE ENGINE" "RUNTIME LAYER" "STATUS" "UPTIME DURATION / METRICS"
echo "--------------------------------------------------------------------------------="

# Helper function to convert raw seconds to standardized HH:mm clock strings
format_time_hhmm() {
    SECONDS=$1
    if [ -z "$SECONDS" ] || [ "$SECONDS" -lt 0 ]; then
        echo "00:00"
    else
        HOURS=$((SECONDS / 3600))
        MINUTES=$(( (SECONDS % 3600) / 60 ))
        printf "%02d:%02d" $HOURS $MINUTES
    fi
}

# --- UTILITY: Inspect cluster deployment pod statuses natively ---
check_kube_deployment() {
    NAME=$1
    DEPLOY=$2
    LABEL_SELECTOR=$3
    
    # Explicitly target the first item and container index elements using [0] array syntax
    START_TIME=$(kubectl get pods -n explorer-zone -l "$LABEL_SELECTOR" -o jsonpath='{.items[0].status.containerStatuses[0].state.running.startedAt}' 2>/dev/null || echo "")
    
    if [ -n "$START_TIME" ] && [ "$START_TIME" != "" ]; then
        # 1. Parse out the ISO hour and minute strings directly using standard text slicing
        # Example input: "2026-06-23T12:06:52Z" -> Extracts "12" and "06"
        POD_HR=$(echo "$START_TIME" | cut -d'T' -f2 | cut -d':' -f1)
        POD_MIN=$(echo "$START_TIME" | cut -d':' -f2)
        
        # 2. Grab the current system UTC hour and minute text strings natively
        SYS_HR=$(date -u +%H)
        SYS_MIN=$(date -u +%M)
        
        # 3. Calculate absolute elapsed duration values cleanly
        DIFF_MIN=$(( (10#$SYS_HR * 60 + 10#$SYS_MIN) - (10#$POD_HR * 60 + 10#$POD_MIN) ))
        
        # Guard against day-boundary rollovers safely
        if [ "$DIFF_MIN" -lt 0 ]; then
            DIFF_MIN=$(( DIFF_MIN + 1440 ))
        fi
        
        DURATION=$(printf "%02d:%02d" $((DIFF_MIN / 60)) $((DIFF_MIN % 60)))
        STATUS_TXT="ONLINE"
        COLOR_CODE="32" # Green
    else
        DURATION="---"
        STATUS_TXT="OFFLINE"
        COLOR_CODE="31" # Red
    fi
    
    # Fetch replica configuration strings
    READY=$(kubectl get deployment "$DEPLOY" -n explorer-zone -o jsonpath='{.status.readyReplicas}/{.status.replicas}' 2>/dev/null || echo "0/0")
    if [ "$STATUS_TXT" = "ONLINE" ]; then
        printf "%-28s %-12s \033[0;${COLOR_CODE}m%-10s\033[0m %-25s\n" "$NAME" "Kubernetes" "$STATUS_TXT" "$DURATION ($READY Pods)"
    else
        printf "%-28s %-12s \033[0;${COLOR_CODE}m%-10s\033[0m %-25s\n" "$NAME" "Kubernetes" "$STATUS_TXT" "---"
    fi
}


# --- UTILITY: Inspect host background detached processes natively ---
check_local_process() {
    NAME=$1
    PATTERN=$2
    PID=$(pgrep -f "$PATTERN" | head -n 1 || echo "")
    
    if [ -n "$PID" ]; then
        ETIME_SEC=$(ps -p "$PID" -o etime= | tr -d '[:space:]' | awk -F: '{
            if (NF == 3) print ($1*3600) + ($2*60) + $3
            else if (NF == 2) print ($1*60) + $2
        }')
        CLOCK_STR=$(format_time_hhmm $ETIME_SEC)
        printf "%-28s %-12s \033[0;32m%-10s\033[0m %-25s\n" "$NAME" "Local Host" "ONLINE" "$CLOCK_STR (PID: $PID)"
    else
        printf "%-28s %-12s \033[0;30m%-10s\033[0m %-25s\n" "$NAME" "Local Host" "STOPPED" "---"
    fi
}

# --- UTILITY: Inspect network port-forwarding background bridges ---
check_port_forward() {
    PORT=$1
    NAME=$2
    COMPONENT_LABEL=$3
    PID=$(lsof -i tcp:"$PORT" -t -s TCP:LISTEN || echo "")
    
    if [ -n "$PID" ]; then
        CMD_CHECK=$(ps -p "$PID" -o command= 2>/dev/null || echo "")
        if echo "$CMD_CHECK" | grep -q "port-forward"; then
            ETIME_SEC=$(ps -p "$PID" -o etime= | tr -d '[:space:]' | awk -F: '{
                if (NF == 3) print ($1*3600) + ($2*60) + $3
                else if (NF == 2) print ($1*60) + $2
            }')
            CLOCK_STR=$(format_time_hhmm $ETIME_SEC)
            printf "%-28s %-12s \033[0;32m%-10s\033[0m %-25s\n" "${COMPONENT_LABEL}-Tunnel: ${PORT}" "Host Network" "BRIDGED" "$CLOCK_STR"
        else
            printf "%-28s %-12s \033[0;33m%-10s\033[0m %-25s\n" "${COMPONENT_LABEL}-Tunnel: ${PORT}" "Host Network" "OCCUPIED" "Locked"
        fi
    else
        printf "%-28s %-12s \033[0;31m%-10s\033[0m %-25s\n" "${COMPONENT_LABEL}-Tunnel: ${PORT}" "Host Network" "CLOSED" "---"
    fi
}

# =========================================================================
# ⚙️ SYSTEM INSPECTION SWEEPS TRACKS
# =========================================================================

# 1. Evaluate Core Infrastructure Layer Foundation Shards (Explicit true pod labels passed!)
check_kube_deployment "Postgres Database Shard" "postgres-db" "app=postgres-ledger"
check_kube_deployment "Apache Kafka KRaft Broker" "enterprise-kafka-broker" "app=enterprise-kafka-broker"
check_kube_deployment "Jaeger Telemetry Core" "jaeger" "app=jaeger"

# 2. Evaluate Cluster Application Process Fabric Shards (Explicit true pod labels passed!)
check_kube_deployment "Sales Order Entry API" "sales-order-entry" "app=sales-order-entry"
check_kube_deployment "Sales Saga Orchestrator" "sales-saga-orchestrator" "app=sales-saga-orchestrator"
check_kube_deployment "Shipping Worker" "shipping-service" "app=shipping-service"
check_kube_deployment "Finance Worker" "finance-service" "app=finance-service"
check_kube_deployment "Finance Data Shard API" "finance-api" "app=finance-api"
check_kube_deployment "Notifications Worker" "notifications-service" "app=notifications-service"
check_kube_deployment "Universal Outbox Daemon" "outbox-daemon" "app=outbox-daemon"
check_kube_deployment "AI Operations Agent" "ops-agent" "app=ops-agent"

echo "--------------------------------------------------------------------------------="

# 3. Evaluate Local Host Python Microservice Consumer Threads
check_local_process "Sales Order Entry API" "sales.order_entry.main:app"
check_local_process "Sales Saga Orchestrator" "sales.orchestrator.main"
check_local_process "Local Shipping Service" "shipping.app"
check_local_process "Local Finance Service" "finance.app"
check_local_process "Local Finance Shard API" "finance.web"
check_local_process "Local Notifications Service" "notifications.app"
check_local_process "Local Ops Agent Engine" "ops_agent.main"

echo "--------------------------------------------------------------------------------="

# 4. Evaluate Network Ingress Tunnels Port Shards
check_port_forward "5432" "Postgres Bridge" "Postgres Database Shard"
check_port_forward "9092" "Kafka Broker Bridge" "Apache Kafka KRaft Broker"
check_port_forward "8081" "Schema Registry Bridge" "Confluent Schema Registry"
check_port_forward "8000" "FastAPI Bridge" "Sales Order Entry API"
check_port_forward "8005" "Ops Agent Engine Bridge" "AI Operations Agent"

# 🟢 SOLUTION: Inject your independent hardware port tunnel check onto port 8001! [1.1]
check_port_forward "8001" "Finance Data API Bridge" "Finance Data Shard API"

check_port_forward "16686" "Jaeger Dashboard Bridge" "Jaeger Telemetry Core"
check_port_forward "4318" "Jaeger OTLP Bridge" "Jaeger OTLP Core"
echo "================================================================================="
