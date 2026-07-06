# tests/integration/test_hold_queries_tool.py

import asyncio
import time

import httpx
import pytest
from finance.db import FinanceLedger

# Import your native query aggregator tool components cleanly
from ops_agent.tools import hold_queries
from ops_agent.tools.hold_queries import list_pending_holds
from sqlalchemy import text


@pytest.fixture(scope="session")
def finance_web_api_url():
    """Returns the base local host URL pointing to your active independent Finance web tier port."""
    return "http://localhost:8001"


def test_finance_web_api_endpoint_connectivity(finance_web_api_url):
    """Verifies that the independent Finance Web API pod is healthy and reachable over the host network tunnel."""
    with httpx.Client() as client:
        response = client.get(f"{finance_web_api_url}/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert data["service"] == "FINANCE_WEB_API"


def test_list_pending_holds_aggregates_and_filters_correctly(
    gateway_api_url, finance_db_session, orchestrator_db_session
):
    """INTEGRATION TEST: Seeds a simulated high-value fraud hold directly into the
    Finance shard database ledger, maps metadata, and verifies that the programmatic
    read aggregator tool extracts, filters, and formats the data correctly.
    """
    # Overwrite the service routing endpoint token to map onto your host's local port tunnel!
    # This forces the tool to use localhost:8000 while running natively on your Mac machine layout tracks
    original_sales_url = hold_queries.SALES_SERVICE_URL
    hold_queries.SALES_SERVICE_URL = gateway_api_url

    try:
        # 🟢 Step A: Fire a mock checkout transaction over the REST gateway to create an order trace
        simulated_checkout_payload = {
            "item_id": "SHIRT_ULTRA_LUXURY",
            "amount": 420.00,  # Over $200 threshold triggers fraud tracking pathways
            "customer": {
                "name": "Kentucky Operations Tester",
                "email": "ky-ops-test@platform.internal",
            },
            "shipping_address": {
                "street": "789 Bourbon Trail",
                "city": "Louisville",
                "state": "KY",
                "postal_code": "40202",
            },
        }

        with httpx.Client() as client:
            response = client.post(
                f"{gateway_api_url}/sales/", json=simulated_checkout_payload
            )

        assert response.status_code == 200
        response_data = response.json()
        order_id = response_data["order_id"]

        # 🟢 Step B: Manually force the localized Finance database shard into a hold state
        try:
            with finance_db_session.begin_nested():
                record = (
                    finance_db_session.query(FinanceLedger)
                    .filter(FinanceLedger.order_id == str(order_id))
                    .first()
                )
                if not record:
                    record = FinanceLedger(
                        order_id=str(order_id), execution_status="PENDING_HUMAN_REVIEW"
                    )
                    finance_db_session.add(record)
                else:
                    record.execution_status = "PENDING_HUMAN_REVIEW"
                finance_db_session.flush()
            finance_db_session.commit()
        except Exception as db_err:
            finance_db_session.rollback()
            pytest.fail(
                f"Failed to seed simulated fraud hold into the private finance database shard: {str(db_err)}"
            )

        # 🟢 Step C: Invoke your raw python tool function natively with the state filter active!
        max_retries = 15
        polling_interval = 1.0
        tool_raw_payload = None
        target_card = None

        for attempt in range(max_retries):
            finance_db_session.expire_all()
            orchestrator_db_session.expire_all()

            # Initialize a native python execution thread frame using asyncio.run()
            try:
                tool_raw_payload = asyncio.run(list_pending_holds(state_code="KY"))
            except Exception as loop_err:
                pytest.fail(
                    f"Asynchronous network event loop frame dropped exception: {str(loop_err)}"
                )

            # 🟢 SOLUTION: Handle dictionary macro response envelopes and list types symmetrically [1.1]
            if isinstance(tool_raw_payload, dict):
                records_list = tool_raw_payload.get("original_records", [])
            else:
                records_list = (
                    tool_raw_payload if isinstance(tool_raw_payload, list) else []
                )

            target_card = next(
                (h for h in records_list if h.get("order_id") == order_id), None
            )

            print(
                f"\n🕵️ [POLLING LOOP PASS {attempt + 1}]: Target Card Data -> {target_card}"
            )

            if target_card:
                break

            time.sleep(polling_interval)

        print(f"\n📊 [FINAL DUMP]: Total payload returned: {tool_raw_payload}")

        assert target_card is not None, (
            f"Key Mismatch Bug Confirmed! The tool successfully pulled the underlying rows, but "
            f"dropped order {order_id} in-memory because it evaluated flat 'shipping_state' instead of the nested schema layout!"
        )

    finally:
        # Restore the cluster-internal domain name token contract to protect your pod deployments completely
        hold_queries.SALES_SERVICE_URL = original_sales_url
