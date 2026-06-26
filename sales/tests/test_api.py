from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sales.order_entry.main import app

# Standardized client configuration for standard test loops
client = TestClient(app, raise_server_exceptions=True)


@patch("sales.order_entry.main.resolve_or_create_customer")
@patch("sales.order_entry.main.persist_invoice_record")
@patch("sales.order_entry.main.initialize_saga_state_tracking")
@patch("sales.order_entry.main.stage_saga_command_envelopes")
def test_create_sale_endpoint_returns_processed_status(
    mock_stage_outbox, mock_init_saga, mock_persist_invoice, mock_resolve_customer
):
    """Verifies that the /sales/ endpoint successfully captures valid payloads and returns indices."""
    # 1. Setup mock records to match database entity objects
    mock_customer = MagicMock()
    mock_customer.id = 111
    mock_customer.customer_name = "Test Buyer"
    mock_customer.email = "test-buyer@gmail.com"
    mock_resolve_customer.return_value = mock_customer

    mock_invoice = MagicMock()
    mock_invoice.id = 222
    mock_invoice.amount = 45.99
    mock_persist_invoice.return_value = mock_invoice

    valid_payload = {
        "amount": 45.99,
        "item_id": "SHIRT_STANDARD_BLUE",
        "customer": {"email": "test-buyer@gmail.com"},
    }

    # Intercept SessionLocal to prevent live network connection attempts
    with patch("sales.order_entry.main.SessionLocal"):
        response = client.post("/sales/", json=valid_payload)

    assert response.status_code == 200
    json_data = response.json()
    assert json_data["status"] == "PROCESSED"
    assert json_data["invoice_id"] == 222
    assert "order_id" in json_data


@patch("sales.order_entry.main.persist_invoice_record")
def test_create_sale_endpoint_rejects_malformed_string_amounts(mock_persist_invoice):
    """Verifies that the /sales/ endpoint instantly throws an HTTP 400 error on non-numeric strings."""
    malformed_payload = {
        "amount": "$10,000",  # ❌ Strict validation wall trigger
        "item_id": "SHIRT_STANDARD_BLUE",
        "customer": {"email": "bad-formatting@gmail.com"},
    }

    with patch("sales.order_entry.main.SessionLocal"):
        response = client.post("/sales/", json=malformed_payload)

    assert response.status_code == 400
    assert "Invalid payload amount format" in response.json()["detail"]

    # Guarantee the transaction pipeline was immediately blocked and never triggered database workers
    mock_persist_invoice.assert_not_called()


def test_create_sale_endpoint_handles_internal_exceptions_gracefully():
    """Verifies that the API correctly bubbles database processing exceptions as standard HTTP 500 responses."""

    local_client = TestClient(app, raise_server_exceptions=False)

    with patch("sales.order_entry.main.SessionLocal") as mock_session_maker:
        # 1. Instantiate a mock database session object context
        mock_db_instance = MagicMock()
        mock_session_maker.return_value = mock_db_instance

        # 2. Force the inner commit execution step to drop a database timeout error!
        mock_db_instance.commit.side_effect = RuntimeError(
            "Database flush transaction hold-lock timeout."
        )

        payload = {
            "amount": 10.00,
            "item_id": "SHIRT_STANDARD_BLUE",
            "customer": {"email": "system-error-test@gmail.com"},
        }

        response = local_client.post("/sales/", json=payload)

    assert response.status_code == 500
    assert "Transaction processing failure" in response.json()["detail"]
