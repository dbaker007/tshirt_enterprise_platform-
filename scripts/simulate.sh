#!/bin/sh
# =========================================================================
# 🎛️ SYSTEM TRANSACTION AND LOAD SIMULATION TESTBENCH
# =========================================================================
set -e

# Target the consolidated engine relative to the scripts execution root
ENGINE_SCRIPT="scripts/simulations/engine.py"

usage() {
    echo "❌ Error: Missing or invalid simulation mode."
    echo "💡 Usage: ./scripts/simulate.sh [success | fail_shipping | fail_finance | load]"
    exit 1
}

# Ensure at least one argument parameter value is supplied to the shell loop
if [ -z "$1" ]; then
    usage
fi

case "$1" in
    success)
        echo "🛒 Preparing standard approved forward transaction path..."
        SIMULATION_TRIGGER_MODE="SUCCESS" exec uv run python3 "$ENGINE_SCRIPT"
        ;;
    fail_shipping)
        echo "🚨 Preparing regional compliance lock simulation (Triggers Shipping Failure)..."
        SIMULATION_TRIGGER_MODE="FAIL_SHIPPING" exec uv run python3 "$ENGINE_SCRIPT"
        ;;
    fail_finance)
        echo "🚨 Preparing credit limit / risk breach simulation (Triggers Finance Failure)..."
        SIMULATION_TRIGGER_MODE="FAIL_FINANCE" exec uv run python3 "$ENGINE_SCRIPT"
        ;;
    load)
        echo "🚀 Initializing high-concurrency 100-order platform stress test matrix..."
        SIMULATION_TRIGGER_MODE="LOAD" exec uv run python3 "$ENGINE_SCRIPT"
        ;;
    *)
        usage
        ;;
esac
