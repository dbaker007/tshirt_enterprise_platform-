#!/bin/sh
# =========================================================================
# 📊 PLATFORM TRANSACTIONAL SAGA STATE TRANSACTION METRICS
# =========================================================================
set -e

echo "\n📊 [SAGA ENGINE]: Current Master State Distribution..."
echo "====================================================="

# Streams a clean group aggregation pass straight into the live deployment handle
exec kubectl exec -n explorer-zone deployment/postgres-db -- psql -U platform_admin -d platform_shared_ledger -c "
  SELECT 
    saga_status AS \"Saga Lifecycle Status\", 
    COUNT(*) AS \"Active Transaction Count\" 
  FROM saga_states 
  GROUP BY saga_status 
  ORDER BY COUNT(*) DESC;
"
