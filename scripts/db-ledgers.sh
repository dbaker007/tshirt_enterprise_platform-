#!/bin/sh
# =========================================================================
# 🔬 MULTI-DOMAIN MICROSERVICE STATE CHECKLIST LEDGER AUDIT
# =========================================================================
set -e

echo "\n🔬 [LEDGER AUDIT]: Cross-Domain Microservice Step Checklist Matrices..."
echo "========================================================================="

# Streams a clean cross-tab group aggregation pass straight into the live deployment handle
exec kubectl exec -n explorer-zone deployment/postgres-db -- psql -U platform_admin -d platform_shared_ledger -c "
  SELECT 
    saga_status AS \"Global Saga Status\",
    finance_status AS \"Finance Step\", 
    shipping_status AS \"Shipping Step\", 
    notifications_status AS \"Notification Step\", 
    COUNT(*) AS \"Record Volume\" 
  FROM saga_states 
  GROUP BY saga_status, finance_status, shipping_status, notifications_status
  ORDER BY saga_status ASC, COUNT(*) DESC;
"
