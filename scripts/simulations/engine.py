import os
import sys
import time
import uuid

import httpx

# =========================================================================
# ⚙️ CENTRAL SIMULATION PARAMETERS AND PAYLOAD PATTERNS
# =========================================================================
TARGET_GATEWAY_URL = os.getenv("SALES_API_URL", "http://localhost:8000/sales/")
TRIGGER_MODE = os.getenv("SIMULATION_TRIGGER_MODE", "SUCCESS").upper()


def generate_base_payload(mode: str) -> dict:
    """Generates order payload contexts dynamically based on the requested failure mode."""
    # Default Clean Base: Standard approved parameters ($45.99, Ohio shipping)
    order_amount = 45.99
    shipping_state = "OH"
    buyer_name = "Bob Vance"
    buyer_email = f"bob-{uuid.uuid4().hex[:6]}@vanceair.com"

    if mode == "FAIL_FINANCE":
        # Over $200 forces a hard fraud rejection breach inside the Finance shard
        order_amount = 250.75
        buyer_name = "Risky Buyer"

    elif mode == "FAIL_SHIPPING":
        # Shipping to Michigan (MI) triggers a hard compliance constraint block
        shipping_state = "MI"
        buyer_name = "Michigan Buyer"

    return {
        "amount": order_amount,
        "item_id": "SHIRT_PREMIUM_RED",
        "customer": {"name": buyer_name, "email": buyer_email},
        "shipping_address": {"state": shipping_state},
    }


# =========================================================================
# 🚀 TRANSACTION EXECUTION METHODS
# =========================================================================
def dispatch_single_order(mode_override: str = None) -> bool:
    """Fires a single transactional payload straight into the Sales API Gateway routing endpoint."""
    active_mode = mode_override or TRIGGER_MODE
    payload = generate_base_payload(active_mode)

    print(
        f"📡 [DISPATCHING]: Mode: [{active_mode}] | Amount: ${payload['amount']} | State: {payload['shipping_address']['state']}"
    )

    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(TARGET_GATEWAY_URL, json=payload)

        if response.status_code == 200:
            data = response.json()
            print(
                f"   └── ✔  [SUCCESS]: Status: {data.get('status')} | Order UUID: {data.get('order_id')}"
            )
            return True
        else:
            print(
                f"   └── ❌ [API REJECTION]: HTTP {response.status_code} | Detail: {response.text}"
            )
            return False

    except Exception as network_err:
        print(
            f"   └── 💥 [CONNECTION FAILURE]: Could not reach Sales API Gateway -> {str(network_err)}"
        )
        return False


def execute_high_volume_stress_load():
    """Concurrently loops and alternates scenarios back-to-back to generate an organic, balanced ledger slate."""
    print(
        f"📈 [STRESS TESTBENCH]: Spawning 100 concurrent transactional orders against gateway..."
    )

    success_count = 0
    failure_count = 0

    # Generate an organic mix: 80% Success, 10% Finance Failures, 10% Shipping Failures
    simulation_mix = ["SUCCESS"] * 80 + ["FAIL_FINANCE"] * 10 + ["FAIL_SHIPPING"] * 10

    start_time = time.time()

    for i, target_mode in enumerate(simulation_mix, start=1):
        print(f"[{i}/100] ", end="")
        if dispatch_single_order(mode_override=target_mode):
            success_count += 1
        else:
            failure_count += 1
        # Brief sub-millisecond sleep to simulate human line pacing
        time.sleep(0.05)

    duration = time.time() - start_time
    print(
        f"\n========================================================================="
    )
    print(f"🏁 [LOAD COMPLETED]: Dispatched 100 orders in {duration:.2f} seconds.")
    print(
        f"📊 Summary -> Gateway Handshakes: {success_count} Passed | {failure_count} Blocked"
    )
    print(f"=========================================================================")


# =========================================================================
# 🎛️ SYSTEM ROUTING SWITCH
# =========================================================================
if __name__ == "__main__":
    if TRIGGER_MODE == "LOAD":
        execute_high_volume_stress_load()
    else:
        dispatch_single_order()
