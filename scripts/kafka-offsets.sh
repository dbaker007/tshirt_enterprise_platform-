#!/bin/sh
# =========================================================================
# 📡 [BROKER WIRE]: Current Raw Partition Message Offsets
# =========================================================================
set -e

echo "\n📡 [BROKER WIRE]: Current Raw Partition Message Offsets..."
echo "=========================================================="

exec kubectl exec -n explorer-zone deployment/enterprise-kafka-broker -c kafka-engine -- \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic saga_replies
