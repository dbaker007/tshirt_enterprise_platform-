import httpx
import pytest
from sales.db import Customer, Invoice, Outbox, SagaState

from .test_db import get_clean_test_db_session


@pytest.fixture(scope="function")
def clean_db():
    db = get_clean_test_db_session()
    try:
        yield db
    finally:
        db.close()


def test_sales_endpoint_atomically_persists_invoice_and_stages_three_commands(clean_db):
    """SCENARIO: Verifies a checkout API call stages 3 worker commands concurrently

    by hitting the live running Uvicorn endpoint on port 8000 out-of-band.
    """
    mock_transaction = {
        "customer": {"name": "Alex Mercer", "email": "alex.mercer@protonmail.com"},
        "amount": 89.95,
        "item_id": "SHIRT_PREMIUM_RED_XL",
    }

    # 🚀 LIVE BLACK-BOX CLIENT: Send a raw HTTP POST request to your active server
    # over the loopback socket, preventing duplicate initialization clashes entirely!
    response = httpx.post("http://localhost:8000/sales/", json=mock_transaction)

    assert response.status_code == 200

    # Clear your test-side cache context map to read the true committed disk row states
    clean_db.expire_all()

    # Query the database session to verify structural parity on the shared disk
    saved_customer = (
        clean_db.query(Customer)
        .filter(Customer.email == "alex.mercer@protonmail.com")
        .first()
    )
    assert saved_customer is not None
    assert saved_customer.customer_name == "Alex Mercer"

    saved_invoice = (
        clean_db.query(Invoice).filter(Invoice.customer_id == saved_customer.id).first()
    )
    assert saved_invoice is not None
    assert saved_invoice.amount == 89.95

    saved_saga = (
        clean_db.query(SagaState)
        .filter(SagaState.order_id == saved_invoice.order_id)
        .first()
    )
    assert saved_saga is not None
    assert saved_saga.saga_status == "STARTED"

    staged_outbox_events = (
        clean_db.query(Outbox)
        .filter(Outbox.partition_key == saved_invoice.order_id)
        .all()
    )
    assert len(staged_outbox_events) == 3
