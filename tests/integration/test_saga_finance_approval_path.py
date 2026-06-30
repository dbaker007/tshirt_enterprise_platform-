import time

import httpx
import pytest
from finance.db import get_finance_ledger_by_order_id
from notifications.db import get_communication_ledger_by_order_id
from sales.orchestrator.db import get_saga_state_by_order_id
from shipping.db import get_shipping_ledger_by_order_id


def test_saga_fraud_approval_override_and_completion_path(
    gateway_api_url,
    orchestrator_db_session,
    finance_db_session,
    shipping_db_session,
    notifications_db_session,
):
    """INTEGRATION TEST: Verifies that an order over $200 hits a manual review hold,

    and successfully completes the forward checkout pipeline once an APPROVE
    override verdict is submitted via the REST API gateway route [1.1].
    """
    fraud_trigger_payload = {
        "item_id": "SHIRT_ULTRA_LUXURY",
        "amount": 250.00,
        "customer": {
            "name": "Trusted High Value Buyer",
            "email": "approved-override-test@platform.internal",
        },
        "shipping_address": {
            "street": "123 Sovereign Boulevard",
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

    for attempt in range(max_retries):
        orchestrator_db_session.expire_all()
        master_state = get_saga_state_by_order_id(orchestrator_db_session, order_id)

        if master_state and master_state.finance_status == "PENDING_HUMAN_REVIEW":
            hit_review_hold = True
            break
        time.sleep(polling_interval)

    assert hit_review_hold, (
        f"Master Saga state failed to transition to PENDING_HUMAN_REVIEW for Order {order_id}"
    )

    with httpx.Client() as client:
        override_response = client.post(
            f"{gateway_api_url}/sales/override",
            json={"order_id": order_id, "verdict": "APPROVE"},
        )
    assert override_response.status_code == 200

    approval_converged = False

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

        if master_state and master_state.saga_status == "IN_TRANSIT":
            assert master_state.finance_status == "SUCCESS"
            assert master_state.shipping_status == "SUCCESS"
            assert master_state.notifications_status == "SUCCESS"

            assert finance_row is not None
            assert finance_row.execution_status == "CREDIT_APPROVED"

            assert shipping_row is not None
            assert shipping_row.execution_status == "SHIPMENT_SECURED"

            assert notifications_row is not None
            assert notifications_row.execution_status == "NOTIFICATION_SENT"

            approval_converged = True
            break

        time.sleep(polling_interval)

    assert approval_converged, (
        f"Saga failed to complete human-triggered approval resumption loops for Order: {order_id}"
    )
