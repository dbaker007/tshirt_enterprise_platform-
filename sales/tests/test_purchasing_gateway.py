import pytest
from fastapi.testclient import TestClient
from sales.app import app
from sales.db import Customer, Outbox, SagaState

from .test_db import get_clean_test_db_session


@pytest.fixture(scope="function")
def clean_db():
    db = get_clean_test_db_session()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="module")
def api_client():
    with TestClient(app) as client:
        yield client


def test_sales_endpoint_atomically_persists_invoice_and_stages_three_commands(
    api_client, clean_db
):
    """SCENARIO: Verifies a checkout API call stages 3 worker commands concurrently

    inside the database outbox table within a single transaction layer.
    """
    mock_transaction = {
        "customer": {"name": "Alex Mercer", "email": "alex.mercer@protonmail.com"},
        "amount": 89.95,
        "item_id": "SHIRT_PREMIUM_RED_XL",
    }

    response = api_client.post("/sales/", json=mock_transaction)
    assert response.status_code == 200

    response_data = response.json()
    generated_uuid = response_data["order_id"]

    # Assert basic relational persistence records exist
    saved_customer = (
        clean_db.query(Customer)
        .filter(Customer.email == "alex.mercer@protonmail.com")
        .first()
    )
    assert saved_customer is not None

    # Assert Saga Checklist row was initialized cleanly
    saga_log = (
        clean_db.query(SagaState).filter(SagaState.order_id == generated_uuid).first()
    )
    assert saga_log is not None
    assert saga_log.status == "STARTED"

    # 🛡️ THE ORCHESTRATOR VERIFICATION LOCK: Assert exactly 3 commands are staged inside the outbox
    staged_events = clean_db.query(Outbox).filter(Outbox.key == generated_uuid).all()
    assert len(staged_events) == 3

    topics = [e.topic for e in staged_events]
    assert "finance_commands" in topics
    assert "shipping_commands" in topics
    assert "notifications_commands" in topics
