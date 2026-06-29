import time

import httpx
import pytest
from finance.db import get_finance_ledger_by_order_id
from notifications.db import get_communication_ledger_by_order_id
from sales.orchestrator.db import get_saga_state_by_order_id
from shipping.db import get_shipping_ledger_by_order_id


def test_saga_dual_failure_race_condition_path(
    gateway_api_url,
    orchestrator_db_session,
    finance_db_session,
    shipping_db_session,
    notifications_db_session,
):
    """INTEGRATION TEST: Verifies that an order with a dual breach ($500 + MI) is handled

    safely across both race condition paths, confirming global transactional consistency [1.1].
    """
    # 1. Arrange a payload that breaks both domain constraints simultaneously
    dual_failure_payload = {
        "item_id": "SHIRT_ULTRA_LUXURY_MI",
        "amount": 500.00,  # Breaches the $200 finance fraud threshold
        "customer": {
            "name": "Dual Breach Buyer",
            "email": "dual-failure-test@platform.internal",
        },
        "shipping_address": {
            "street": "999 Double Trouble Way",
            "city": "Grand Rapids",
            "state": "MI",  # Breaches the restricted shipping location rule [1.1]
            "postal_code": "49503",
        },
    }

    # 2. Act: Dispatch the request to the live FastAPI gateway
    with httpx.Client() as client:
        response = client.post(f"{gateway_api_url}/sales/", json=dual_failure_payload)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "PROCESSED"
    order_id = response_data["order_id"]

    # 3. Assert: Asynchronously poll for dynamic convergence layout matching the active race winner
    max_retries = 15
    polling_interval_seconds = 1.0
    dual_failure_converged = False

    for attempt in range(max_retries):
        # Evict SQLAlchemy identity caches to read fresh state from disk shards
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

        # Evaluate if the Orchestrator has locked the global status at REJECTED [1.1]
        if master_state and master_state.saga_status == "REJECTED":
            # 🟢 VARIANT A: Shipping processed first; Finance was interrupted and rolled back natively [1.1]
            shipping_wins = (
                master_state.shipping_status == "FAILED"
                and master_state.finance_status == "ROLLED_BACK"
                and master_state.notifications_status == "ROLLED_BACK"
            )

            # 🟢 VARIANT B: Finance processed first; Shipping was aborted and rolled back natively [1.1]
            finance_wins = (
                master_state.finance_status == "FAILED"
                and master_state.shipping_status == "ROLLED_BACK"
                and master_state.notifications_status == "ROLLED_BACK"
            )

            if shipping_wins or finance_wins:
                # Cross-assert local database shard row consistency to guarantee state integrity [1.1]
                assert finance_row is not None
                assert finance_row.execution_status in [
                    "CREDIT_LINE_RELEASED",
                    "PAYMENT_REJECTED",
                ]

                assert shipping_row is not None
                assert shipping_row.execution_status in [
                    "LEGAL_REJECTION_MI",
                    "FREIGHT_ROUTE_RELEASED",
                ]

                assert notifications_row is not None
                assert notifications_row.execution_status == "ROLLED_BACK"

                dual_failure_converged = True
                break

        time.sleep(polling_interval_seconds)

    assert dual_failure_converged, (
        f"Saga failed to converge on a valid concurrent failure matrix layout for Order: {order_id}"
    )
