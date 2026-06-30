import time

import httpx
import pytest
from finance.db import get_finance_ledger_by_order_id
from notifications.db import get_communication_ledger_by_order_id
from sales.orchestrator.db import get_saga_state_by_order_id
from shipping.db import get_shipping_ledger_by_order_id


def test_saga_shipping_rejection_and_rollback_path(
    gateway_api_url,
    orchestrator_db_session,
    finance_db_session,
    shipping_db_session,
    notifications_db_session,
):
    """INTEGRATION TEST: Verifies that an order targeting Michigan (MI) triggers

    a Shipping geographic rejection, locks the saga at REJECTED, and rolls back sibling shards.
    """
    shipping_trigger_payload = {
        "item_id": "SHIRT_STANDARD_BLUE",
        "amount": 25.00,
        "customer": {
            "name": "Midwest Buyer",
            "email": "mi-shipping-test@platform.internal",
        },
        "shipping_address": {
            "street": "123 Wolverine Lane",
            "city": "Detroit",
            "state": "MI",
            "postal_code": "48201",
        },
    }

    with httpx.Client() as client:
        response = client.post(
            f"{gateway_api_url}/sales/", json=shipping_trigger_payload
        )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "PROCESSED"
    order_id = response_data["order_id"]

    max_retries = 10
    polling_interval_seconds = 1.0
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
            if (
                master_state.finance_status == "ROLLED_BACK"
                and master_state.notifications_status == "ROLLED_BACK"
            ):
                assert master_state.shipping_status == "FAILED"

                assert shipping_row is not None
                assert shipping_row.execution_status == "LEGAL_REJECTION_MI"

                assert finance_row is not None
                assert finance_row.execution_status == "CREDIT_LINE_RELEASED"

                assert notifications_row is not None
                assert notifications_row.execution_status == "ROLLED_BACK"

                rollback_converged = True
                break

        time.sleep(polling_interval_seconds)

    assert rollback_converged, (
        f"Saga failed to complete shipping-driven compensation rollbacks within timeout window for Order: {order_id}"
    )
