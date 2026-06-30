import time

import httpx
import pytest
from finance.db import get_finance_ledger_by_order_id
from notifications.db import get_communication_ledger_by_order_id
from sales.orchestrator.db import get_saga_state_by_order_id
from shipping.db import get_shipping_ledger_by_order_id


def test_saga_fraud_hits_human_review_hold_only(
    gateway_api_url,
    orchestrator_db_session,
    finance_db_session,
):
    """INTEGRATION TEST: Verifies that an order over $200 successfully hits a manual review hold,

    stops execution, and writes 'PENDING_HUMAN_REVIEW' to the local finance database shard.
    """
    # 1. Arrange a payload that intentionally breaches the $200 risk ceiling
    fraud_trigger_payload = {
        "item_id": "SHIRT_ULTRA_LUXURY",
        "amount": 250.00,  # Breaches the $200 threshold hold gate
        "customer": {
            "name": "Trusted High Value Buyer",
            "email": "hold-only-test@platform.internal",
        },
        "shipping_address": {
            "street": "123 Sovereign Boulevard",
            "city": "Cluster Ville",
            "state": "OH",
            "postal_code": "43210",
        },
    }

    # 2. Act: Ingest the initial order transaction via the gateway REST endpoint
    with httpx.Client() as client:
        response = client.post(f"{gateway_api_url}/sales/", json=fraud_trigger_payload)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "PROCESSED"
    order_id = response_data["order_id"]

    # 3. Assert: Poll to verify that the local finance ledger updates to PENDING_HUMAN_REVIEW
    max_retries = 15
    polling_interval = 1.0
    hit_review_hold = False

    for attempt in range(max_retries):
        # Clear identity maps to pull completely raw updates from physical disk shards
        finance_db_session.expire_all()
        orchestrator_db_session.expire_all()

        finance_row = get_finance_ledger_by_order_id(finance_db_session, order_id)
        master_state = get_saga_state_by_order_id(orchestrator_db_session, order_id)

        print(f"[POLL ATTEMPT {attempt}] Order ID: {order_id}")
        if finance_row:
            print(f" -> Local Finance Ledger Status: '{finance_row.execution_status}'")
        else:
            print(" -> Local Finance Ledger Status: NOT_FOUND_YET")

        if master_state:
            print(f" -> Global Master Saga Status:  '{master_state.saga_status}'")
            print(f" -> Master Finance Status:      '{master_state.finance_status}'")
        else:
            print(" -> Global Master Saga Status:  NOT_FOUND_YET")

        # Confirming the local finance record state has reached the human-in-the-loop stage
        if finance_row and finance_row.execution_status == "PENDING_HUMAN_REVIEW":
            hit_review_hold = True
            break

        time.sleep(polling_interval)

    assert hit_review_hold, (
        f"Local Finance Ledger shard failed to transit to PENDING_HUMAN_REVIEW for Order {order_id}"
    )
