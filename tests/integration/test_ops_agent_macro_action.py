# tests/integration/test_ops_agent_macro_action.py

import json
import time
import uuid

import httpx
import pytest


@pytest.fixture(scope="session")
def ops_agent_api_url():
    """Returns the base local host URL pointing to your active local Ops Agent engine port."""
    return "http://localhost:8005"


def test_ops_agent_llm_macro_action_execution(gateway_api_url, ops_agent_api_url):
    """
    PRODUCTION INTEGRATION TEST: Seeds a transaction profile using a randomized, high-entropy
    identity fingerprint to guarantee isolated test execution without purging the main database [1.1].
    """
    # 🟢 SOLUTION: Generate a randomized unique identifier signature to prevent cross-test data pollution [1.1]
    unique_suffix = uuid.uuid4().hex[:6]
    target_customer_name = f"Miles OBrien {unique_suffix}"

    # Step A: Seed a specific order matching precise macro search boundaries
    checkout_payload = {
        "item_id": "SHIRT_ULTRA_LUXURY",
        "amount": 820.00,  # Fits between min $500 and max $1000
        "customer": {
            "name": target_customer_name,
            "email": f"obrien-{unique_suffix}@starfleet.internal",
        },
        "shipping_address": {
            "street": "12 Transporter Pad Row",
            "city": "Louisville",
            "state": "KY",  # Target state constraint
            "postal_code": "40202",  # Target zip code constraint
        },
    }

    with httpx.Client() as client:
        response = client.post(f"{gateway_api_url}/sales/", json=checkout_payload)

    assert response.status_code == 200
    order_id = response.json()["order_id"]

    # Give backend system orchestration layers a stable 2 seconds to land on the hold desk naturally
    time.sleep(2.0)

    # 🟢 Step B: Fire the prompt directing Llama to isolate our specific randomized identity fingerprint [1.1]
    agent_payload = {
        "prompt": (
            f"Please find and approve all pending fraud holds for customer '{target_customer_name}' "
            f"located in the state of Kentucky with zip code 40202 where the order "
            f"value is between $500 and $1000 right now."
        )
    }

    with httpx.Client(timeout=60.0) as client:
        agent_response = client.post(
            f"{ops_agent_api_url}/api/agent/command", json=agent_payload
        )

    assert agent_response.status_code == 200
    execution_result = agent_response.json()

    # Verify the autonomous tool constraints match our exact inputs
    assert execution_result["status"] == "COMPLETED"
    assert execution_result["tools_utilized_count"] >= 1

    # Extract the first metric entry dictionary out of the trace list collection safely via index offsets
    traces_list = execution_result["executed_tool_traces"]
    assert isinstance(traces_list, list), (
        "Expected executed_tool_traces to return as an array list collection structure."
    )

    trace = traces_list[0]
    assert trace["tool_name"] == "list_pending_holds"

    # Extract the matched arguments generated natively by Llama's attention heads [1.1]
    args = trace["arguments_passed"]

    # Validate the complete multi-parameter suite contract [1.1]
    assert args["state_code"].upper().strip() == "KY"
    assert float(args["min_amount"]) == 500.0
    assert float(args["max_amount"]) == 1000.0
    assert str(args["zip_code"]).strip() == "40202"
    assert args["action_verdict"].upper().strip() == "APPROVE"

    # Verify our high-entropy unique target customer name was extracted precisely [1.1]
    extracted_name = str(args["customer_name"]).lower()
    assert unique_suffix in extracted_name
    assert "obrien" in extracted_name

    # Verify Python backend processing loops committed database shard mutations correctly
    raw_output = trace.get("tool_raw_output", {})
    assert raw_output.get("status") == "COMPLETED"
    assert raw_output.get("successful_mutations_count", 0) >= 1
    assert raw_output.get("failed_mutations_count") == 0

    # Resilient Polling Pass to verify database state updates cleared Kafka smoothly
    max_retries = 15
    polling_interval = 1.0
    order_cleared = False

    for attempt in range(max_retries):
        with httpx.Client() as client:
            final_ui_res = client.get(f"{gateway_api_url}/api/ui/pending-reviews")
        if final_ui_res.status_code == 200:
            current_holds = [str(h.get("order_id")) for h in final_ui_res.json()]
            if str(order_id) not in current_holds:
                order_cleared = True
                break
        time.sleep(polling_interval)

    assert order_cleared, (
        f"Macro execution completed, but order {order_id} is still stuck inside the active review queue."
    )
