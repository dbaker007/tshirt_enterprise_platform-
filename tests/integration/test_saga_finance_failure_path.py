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
    Conforms cleanly to the dual wire_status/ledger_status orchestrator blueprint.
    """
    fraud_trigger_payload = {
        "item_id": "SHIRT_ULTRA_LUXURY",
        "amount": 250.00,
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

    with httpx.Client() as client:
        response = client.post(f"{gateway_api_url}/sales/", json=fraud_trigger_payload)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "PROCESSED"
    order_id = response_data["order_id"]

    max_retries = 15
    polling_interval = 1.0
    hit_review_hold = False

    # Act Part B: Poll the master state orchestrator layout matrix to verify hold interception
    for attempt in range(max_retries):
        finance_db_session.expire_all()
        finance_row = get_finance_ledger_by_order_id(finance_db_session, order_id)

        # 🟢 SOLUTION: Poll the local shard table where the node explicitly commits its PENDING_HUMAN_REVIEW token!
        if finance_row and finance_row.execution_status == "PENDING_HUMAN_REVIEW":
            hit_review_hold = True
            break
        time.sleep(polling_interval)

    assert hit_review_hold, (
        f"Finance local database shard failed to enter PENDING_HUMAN_REVIEW hold state for Order {order_id}"
    )

    # Act Part C: Execute the manual rejection payload straight via the updated REST interface
    with httpx.Client() as client:
        override_response = client.post(
            f"{gateway_api_url}/sales/override",
            json={"order_id": order_id, "verdict": "REJECT"},
        )
    assert override_response.status_code == 200

    rollback_converged = False

    for attempt in range(max_retries):
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

        if master_state and master_state.saga_status == "REJECTED":
            # Verify sibling shards processed their respective compensation rollback instructions
            if (
                master_state.shipping_status == "ROLLED_BACK"
                and master_state.notifications_status == "ROLLED_BACK"
            ):
                # 🟢 NEW SAGA PATTERN: Master tracker cell contains the descriptive string token "PAYMENT_REJECTED"
                assert master_state.finance_status == "PAYMENT_REJECTED"

                # Cross-assert private database shard records reflect the manual operational outcome
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
