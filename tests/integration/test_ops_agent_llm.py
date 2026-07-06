# tests/integration/test_ops_agent_llm.py

import json
import time

import httpx
import pytest


@pytest.fixture(scope="session")
def ops_agent_api_url():
    """Returns the base local host URL pointing to your active local Ops Agent engine port."""
    return "http://localhost:8005"


def test_ops_agent_endpoint_connectivity(ops_agent_api_url):
    """Verifies that the Ops Agent microservice is healthy and reachable over the host network."""
    with httpx.Client() as client:
        response = client.get(f"{ops_agent_api_url}/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert data["domain"] == "OPS_AGENT"


def test_ops_agent_llm_compound_parameter_matching(gateway_api_url, ops_agent_api_url):
    """
    INTEGRATION TEST: Seeds a specific transaction profile, fires a highly restrictive,
    multi-constraint natural language query, and verifies that Llama maps ALL
    compound parameters precisely into the tool execution frame [1.1].
    """
    # 🟢 Step A: Seed a specific order matching precise search boundaries
    checkout_payload = {
        "item_id": "SHIRT_ULTRA_LUXURY",
        "amount": 750.00,  # Fits between min $500 and max $1000
        "customer": {
            "name": "Montgomery Scott",  # Target name constraint
            "email": "scotty@starfleet.internal",
        },
        "shipping_address": {
            "street": "42 Engineering Deck Lane",
            "city": "Louisville",
            "state": "KY",  # Target state constraint
            "postal_code": "40202",  # Target zip code constraint
        },
    }

    with httpx.Client() as client:
        response = client.post(f"{gateway_api_url}/sales/", json=checkout_payload)

    assert response.status_code == 200
    order_id = response.json()["order_id"]

    # Give the backend system orchestration layers a stable 2 seconds to land on the hold desk
    time.sleep(2.0)

    # 🟢 Step B: Fire an explicit prompt forcing Llama to utilize all 5 tool parameters simultaneously! [1.1]
    agent_payload = {
        "prompt": (
            "Look up all pending fraud holds for customer 'Montgomery Scott' "
            "located in the state of Kentucky with zip code 40202 where the order "
            "value is between $500 and $1000."
        )
    }

    with httpx.Client(timeout=60.0) as client:
        agent_response = client.post(
            f"{ops_agent_api_url}/api/agent/command", json=agent_payload
        )

    assert agent_response.status_code == 200
    execution_result = agent_response.json()

    # 🟢 Step C: Verify the autonomous tool constraints match our exact inputs
    assert execution_result["status"] == "COMPLETED"
    assert execution_result["tools_utilized_count"] >= 1

    trace = execution_result["executed_tool_traces"][0]
    assert trace["tool_name"] == "list_pending_holds"

    # Extract the matched arguments generated natively by Llama's attention heads [1.1]
    args = trace["arguments_passed"]

    # Validate the complete parameter suite contract [1.1]
    assert args["state_code"].upper().strip() == "KY"
    assert float(args["min_amount"]) == 500.0
    assert float(args["max_amount"]) == 1000.0
    assert "montgomery" in args["customer_name"].lower()
    assert str(args["zip_code"]).strip() == "40202"

    # Confirm the tool safely processed the request and found our seeded transaction record [1.1]
    assert trace.get("records_returned_count", 0) >= 1

    # 🟢 SOLUTION: Loop over the raw tool result output set to verify all rows meet filtering constraints [1.1]
    tool_raw_output = trace.get("tool_raw_output", [])
    assert isinstance(tool_raw_output, list), (
        "Expected tool raw output to be a list collection."
    )

    for matched_order in tool_raw_output:
        order_amount = float(matched_order.get("amount", 0.0))
        address_obj = matched_order.get("shipping_address", {})

        # 1. Assert Minimum Amount boundary holds true
        assert order_amount >= 500.0, (
            f"Isolation Breach! Order amount {order_amount} is below $500 minimum threshold constraint."
        )

        # 2. Assert Maximum Amount boundary holds true
        assert order_amount <= 1000.0, (
            f"Isolation Breach! Order amount {order_amount} is above $1000 maximum threshold constraint."
        )

        # 3. Assert State Code matches accurately
        assert str(address_obj.get("state", "")).upper().strip() == "KY", (
            f"Geographical Breach! Found invalid state location: {address_obj.get('state')}"
        )

        # 4. Assert Customer Name substring match holds true
        assert "montgomery" in str(matched_order.get("customer_name", "")).lower(), (
            f"Identity Leak! Found unexpected customer name row: {matched_order.get('customer_name')}"
        )

        # 5. Assert Postal Zip Code matches precisely
        assert str(address_obj.get("postal_code", "")).strip() == "40202", (
            f"Geographical Breach! Found invalid zip code target: {address_obj.get('postal_code')}"
        )
