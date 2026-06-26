#!/usr/bin/env bash
# =========================================================================
# 🪓 PLATFORM ARCHITECTURE DATA PURGE ORCHESTRATOR
# =========================================================================
set -euo pipefail

NAMESPACE="explorer-zone"
POSTGRES_DEPLOY="postgres-db"

echo "🧹 Initiating complete cross-domain database shard wipe sequence..."

if ! kubectl get deployment "$POSTGRES_DEPLOY" -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "❌ [ERROR]: Database pod deployment/$POSTGRES_DEPLOY not found in namespace [$NAMESPACE]!"
    exit 1
fi

echo "🗄️ Connected to cluster database platform engine. Purging table states..."
echo "--------------------------------------------------------------------------------"

# Inline execution function to keep shell code neat
run_sql_cmd() {
    kubectl exec -n "$NAMESPACE" "deployment/$POSTGRES_DEPLOY" -i -- psql -U platform_admin -d platform_shared_ledger -q -c "$1"
}

echo "  ├── Wiping Central Saga Conductor Checklist Log Shard..."
run_sql_cmd "TRUNCATE TABLE saga_states RESTART IDENTITY CASCADE;"

echo "  ├── Wiping Localized Financial Accounting Shard..."
run_sql_cmd "TRUNCATE TABLE finance_ledger RESTART IDENTITY CASCADE;"
run_sql_cmd "TRUNCATE TABLE invoices RESTART IDENTITY CASCADE;"

echo "  ├── Wiping Localized Shipping & Distribution Logistics Shard..."
run_sql_cmd "TRUNCATE TABLE shipping_ledger RESTART IDENTITY CASCADE;"

echo "  ├── Wiping Localized Customer Communications Notification Shard..."
run_sql_cmd "TRUNCATE TABLE communication_ledger RESTART IDENTITY CASCADE;"
run_sql_cmd "TRUNCATE TABLE customers RESTART IDENTITY CASCADE;"

echo "  ├── Draining Universal Platform Transactional Outbox Log Shard..."
run_sql_cmd "TRUNCATE TABLE platform_outbox RESTART IDENTITY CASCADE;"

echo "--------------------------------------------------------------------------------"
echo "✔ [SUCCESS]: Complete platform database tier has been reset to an absolute zero state."
