import time

import httpx
import pytest
from finance.db import get_finance_ledger_by_order_id
from notifications.db import get_communication_ledger_by_order_id

# 🟢 IMPORT THE SYMMETRICAL DATABASE UTILITY FUNCTIONS WE JUST CREATED
from sales.orchestrator.db import get_saga_state_by_order_id
from shipping.db import get_shipping_ledger_by_order_id


def test_saga_happy_path_execution(
    gateway_api_url,
    orchestrator_db_session,
    finance_db_session,
    shipping_db_session,
    notifications_db_session,
):
    """INTEGRATION TEST: Verifies that an order under $200 successfully clears

    all downstream microservice checklists and transitions to IN_TRANSIT.
    """
    # 1. Arrange a valid, compliant payload data layout
    checkout_payload = {
        "item_id": "SHIRT_PREMIUM_RED",
        "amount": 45.50,  # Under the $200 risk threshold
        "customer": {
            "name": "Integration Tester",
            "email": "happy-path-test@platform.internal",
        },
        "shipping_address": {
            "street": "456 Automation Way",
            "city": "Cluster Ville",
            "state": "OH",  # Valid geographic area
            "postal_code": "43210",
        },
    }

    # 2. Act: POST the request directly to the live, running FastAPI gateway socket
    with httpx.Client() as client:
        response = client.post(f"{gateway_api_url}/sales/", json=checkout_payload)

    # Validate that the API gateway ingested the payload and returned an invoice ID
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "PROCESSED"

    order_id = response_data["order_id"]
    assert order_id is not None

    # 3. Assert: Asynchronously poll the database shards for convergence checks
    # Because Kafka and the Outbox Daemon operate in the background, we retry for up to 10s
    max_retries = 10
    polling_interval_seconds = 1.0
    saga_cleared = False

    for attempt in range(max_retries):
        # Refresh the database tracking row states cleanly from disk registers
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

        # Evaluate if the transaction matrix has completely finalized on the data plane
        if master_state and master_state.saga_status == "IN_TRANSIT":
            # Verify that the central tracking metrics match exactly
            assert master_state.finance_status == "SUCCESS"
            assert master_state.shipping_status == "SUCCESS"
            assert master_state.notifications_status == "SUCCESS"

            # Cross-verify that individual shard databases mirror this state identically
            assert finance_row is not None
            assert finance_row.execution_status == "CREDIT_APPROVED"

            assert shipping_row is not None
            assert shipping_row.execution_status == "SHIPMENT_SECURED"

            assert notifications_row is not None
            assert notifications_row.execution_status == "NOTIFICATION_SENT"

            saga_cleared = True
            break

        time.sleep(polling_interval_seconds)

    # If the retry loop expires without saga_cleared becoming True, fail the test build
    assert saga_cleared, (
        f"Saga transaction matrix failed to converge on IN_TRANSIT within timeout window for Order: {order_id}"
    )
