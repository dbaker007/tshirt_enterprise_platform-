import time

import httpx
import pytest
from finance.db import get_finance_ledger_by_order_id
from notifications.db import get_communication_ledger_by_order_id
from sales.orchestrator.db import get_saga_state_by_order_id
from shipping.db import get_shipping_ledger_by_order_id


def test_saga_fraud_rejection_and_rollback_path(
    gateway_api_url,
    orchestrator_db_session,
    finance_db_session,
    shipping_db_session,
    notifications_db_session,
):
    """INTEGRATION TEST: Verifies that an order over $200 hits a manual review hold,

    and triggers a full saga rollback once a REJECT override verdict is submitted via REST. [1.1]
    """
    # 1. Arrange a payload that intentionally breaches the $200 risk ceiling
    fraud_trigger_payload = {
        "item_id": "SHIRT_ULTRA_LUXURY",
        "amount": 250.00,  # Breaches the $200 threshold
        "customer": {
            "name": "Suspicious Buyer",
            "email": "fraud-trigger-test@platform.internal",
        },
        "shipping_address": {
            "street": "789 Rogue Boulevard",
            "city": "Cluster Ville",
            "state": "OH",
            "postal_code": "43210",
        },
    }

    # 2. Act Part A: Ingest the initial order transaction via the gateway
    with httpx.Client() as client:
        response = client.post(f"{gateway_api_url}/sales/", json=fraud_trigger_payload)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "PROCESSED"
    order_id = response_data["order_id"]

    # 3. Act Part B: Poll until the Finance shard enters the PENDING_HUMAN_REVIEW wait state
    max_retries = 10
    polling_interval = 1.0
    hit_review_hold = False

    for attempt in range(max_retries):
        finance_db_session.expire_all()
        finance_row = get_finance_ledger_by_order_id(finance_db_session, order_id)

        if finance_row and finance_row.execution_status == "PENDING_HUMAN_REVIEW":
            hit_review_hold = True
            break
        time.sleep(polling_interval)

    assert hit_review_hold, (
        f"Finance shard failed to enter PENDING_HUMAN_REVIEW for Order {order_id}"
    )

    # 4. Act Part C: POST the manual REJECT verdict payload to the new gateway endpoint
    with httpx.Client() as client:
        override_response = client.post(
            f"{gateway_api_url}/sales/override",
            json={"order_id": order_id, "verdict": "REJECT"},
        )
    assert override_response.status_code == 200

    # 5. Assert: Verify the orchestrator cascades the manual rejection down to sibling shards
    rollback_converged = False

    for attempt in range(max_retries):
        # Evict identity caches to read fresh changes from disk
        orchestrator_db_session.expire_all()
        finance_db_session.expire_all()
        shipping_db_session.expire_all()
        notifications_db_session.expire_all()

        master_state = get_saga_state_by_order_id(orchestrator_db_session, order_id)
        finance_row = get_finance_ledger_by_order_id(finance_db_session, order_id)
        shipping_row = get_shipping_ledger_by_order_id(shipping_db_session, order_id)
        notifications_row = get_communication_ledger_by_order_id(
            notifications_db_session, order_id
        )

        # Check if the Orchestrator has locked the global status at REJECTED
        if master_state and master_state.saga_status == "REJECTED":
            if (
                master_state.shipping_status == "ROLLED_BACK"
                and master_state.notifications_status == "ROLLED_BACK"
            ):
                assert master_state.finance_status == "FAILED"

                # Cross-assert database shards match the manual cancellation outcome
                assert finance_row is not None
                assert finance_row.execution_status == "PAYMENT_REJECTED"

                assert shipping_row is not None
                assert shipping_row.execution_status == "FREIGHT_ROUTE_RELEASED"

                assert notifications_row is not None
                assert notifications_row.execution_status == "ROLLED_BACK"

                rollback_converged = True
                break

        time.sleep(polling_interval)

    assert rollback_converged, (
        f"Saga failed to complete human-triggered compensations for Order: {order_id}"
    )
