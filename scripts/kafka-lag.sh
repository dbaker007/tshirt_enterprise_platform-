#!/bin/sh
# =========================================================================
# 📡 [BROKER NETWORK]: Active Consumer Group Lag Matrix
# =========================================================================
set -e

echo "\n📡 [BROKER NETWORK]: Active Consumer Group Lag Matrix..."
echo "======================================================="

exec kubectl exec -n explorer-zone deployment/enterprise-kafka-broker -c kafka-engine -- \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
