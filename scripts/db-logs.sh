#!/bin/sh
# =========================================================================
# 🛰️ POSTGRES LEDGER ACTIVE INTERROGATOR TOOLKIT
# =========================================================================
set -e

echo "🛰️ Attaching log trail pipe to active Postgres instance..."
exec kubectl logs -n explorer-zone -l app=postgres-ledger -f
