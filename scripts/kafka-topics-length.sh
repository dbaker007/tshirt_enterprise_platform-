#!/bin/sh
# =========================================================================
# 📊 AUTOMATED KAFKA QUEUE INTERROGATOR & ABSOLUTE MESSAGE Footprint Counter
# =========================================================================
set -e

echo "📡 Counting active message footprints across cluster topic queues..."

# 1. Fetch the raw topic list from the cluster engine, ignoring administrative channels
TOPICS=$(kubectl exec -n explorer-zone deployment/enterprise-kafka-broker -c kafka-engine -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -v -E "^(__consumer_offsets|_schemas)$$" 2>/dev/null)

# 2. Iterate through each discovered topic, calculate true log-end offsets, and summarize the total volume depth
for topic in $TOPICS; do
  # 🟢 FIX: Forcefully pass --time -1 to query the true maximum log-end position array on the broker disk [1.1]
  count=$(kubectl exec -n explorer-zone deployment/enterprise-kafka-broker -c kafka-engine -- \
    /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$topic" --time -1 2>/dev/null \
    | awk -F ":" '{sum += $3} END {print (sum == "" ? 0 : sum)}')
    
  echo "📊 Topic: [$topic] ──► Length/Messages: [ $count ]"
done
