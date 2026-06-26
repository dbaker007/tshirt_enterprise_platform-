#!/bin/sh
# =========================================================================
# 📥 UNIFIED PLATFORM OUTBOX LEDGER BACKLOG INTERROGATOR
# =========================================================================
set -e

echo "\n📥 [DATA PIPELINE]: Unified Outbox Table Record BackLOGS..."
echo "==========================================================="

# Streams a single SQL group pass straight into your active Postgres deployment
exec kubectl exec -n explorer-zone deployment/postgres-db -- psql -U platform_admin -d platform_shared_ledger -c "
  SELECT 
    topic AS \"Target Queue/Topic\", 
    COUNT(*) AS \"Backlog Volume Count\" 
  FROM platform_outbox 
  GROUP BY topic 
  ORDER BY topic ASC;
"
