import os
import random
import sys
import time
import uuid

import httpx

# =========================================================================
# ⚙️ CENTRAL SIMULATION PARAMETERS AND PAYLOAD PATTERNS
# =========================================================================
TARGET_GATEWAY_URL = os.getenv("SALES_API_URL", "http://localhost:8000/sales/")
TRIGGER_MODE = os.getenv("SIMULATION_TRIGGER_MODE", "SUCCESS").upper()

# Whitelisted array of names for rich database and frontend dashboard visualization
BUYER_NAMES_POOL = [
    "Bob Vance",
    "Phyllis Vance",
    "Michael Scott",
    "Jim Halpert",
    "Pam Beesly",
    "Dwight Schrute",
    "Angela Martin",
    "Oscar Martinez",
    "Kevin Malone",
    "Stanley Hudson",
    "Creed Bratton",
    "Kelly Kapoor",
    "Ryan Howard",
    "Toby Flenderson",
    "Darryl Philbin",
    "Andy Bernard",
]

# Strategic US States pool including Kentucky (KY) to support your upcoming tool-calling feature
US_STATES_POOL = ["OH", "PA", "NY", "IL", "IN", "KY", "TN", "WV", "CA", "TX", "FL"]


def generate_base_payload(mode: str) -> dict:
    """Generates order payload contexts dynamically with realistic, randomized distributions."""
    buyer_name = random.choice(BUYER_NAMES_POOL)

    # Generate clean, unique, domain-valid matching email targets
    email_prefix = buyer_name.lower().replace(" ", "-")
    buyer_email = f"{email_prefix}-{uuid.uuid4().hex[:4]}@vanceair.com"

    if mode == "FAIL_FINANCE":
        # Simulate variable fraud hold thresholds: price ranges randomly between $201.00 and $550.00
        order_amount = round(random.uniform(201.00, 550.00), 2)
        shipping_state = random.choice(US_STATES_POOL)

    elif mode == "FAIL_SHIPPING":
        # Simulate standard compliance rejections: strict block against Michigan delivery routes
        order_amount = round(random.uniform(15.00, 195.00), 2)
        shipping_state = "MI"

    else:
        # Standard Success Path: price ranges randomly between $10.00 and $199.99
        order_amount = round(random.uniform(10.00, 199.99), 2)
        shipping_state = random.choice(US_STATES_POOL)

    return {
        "amount": order_amount,
        "item_id": random.choice(
            ["SHIRT_PREMIUM_RED", "SHIRT_ULTRA_LUXURY", "SHIRT_STANDARD_BLUE"]
        ),
        "customer": {"name": buyer_name, "email": buyer_email},
        "shipping_address": {
            "street": f"{random.randint(100, 9999)} Transaction Way",
            "city": "Metropolis",
            "state": shipping_state,
            "postal_code": f"{random.randint(10000, 99999)}",
        },
    }


# =========================================================================
# 🚀 TRANSACTION EXECUTION METHODS
# =========================================================================
def dispatch_single_order(mode_override: str = None) -> bool:
    """Fires a single transactional payload straight into the Sales API Gateway routing endpoint."""
    active_mode = mode_override or TRIGGER_MODE
    payload = generate_base_payload(active_mode)

    print(
        f"📡 [DISPATCHING]: Mode: [{active_mode}] | Name: {payload['customer']['name']} | Amount: ${payload['amount']} | State: {payload['shipping_address']['state']}"
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
    """Concurrently loops and alternates scenarios back-to-back to generate an organic, balanced slate."""
    print(
        f"📈 [STRESS TESTBENCH]: Spawning 100 concurrent transactional orders against gateway..."
    )

    success_count = 0
    failure_count = 0

    # Generate an organic mix: 80% Success, 10% Finance Failures, 10% Shipping Failures
    simulation_mix = ["SUCCESS"] * 80 + ["FAIL_FINANCE"] * 10 + ["FAIL_SHIPPING"] * 10

    # Shuffle the list to simulate randomized real-world user traffic patterns
    random.shuffle(simulation_mix)

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
