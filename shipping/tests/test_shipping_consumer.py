import sys
from unittest.mock import MagicMock, patch

# GATELOCK PATTERN: Cleanly bypass network handshakes during local unit tests
with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient"),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer"),
):
    from shipping.app import ShippingConsumerApplication
    from shipping.graph import shipping_graph_engine


@patch("shipping.graph.stage_shipping_secured_event")
def test_shipping_consumer_michigan_delivery_breach_rollback(mock_persistence):
    """Verifies that the Shipping logistics engine accurately intercepts restricted geography
    payloads inside its shipping_address dictionary and triggers a legal hold database event.
    """
    # 1. Instantiate the refactored child class wrapper using faked network interfaces
    app = ShippingConsumerApplication()

    # 2. 🛠️ FIXED: Build the true, nested schema layout expected by your graph node!
    breached_payload = {
        "order_id": "shipping-breach-uuid-404",
        "customer_email": "alex.trace@enterprise.io",
        "shipping_address": {"state": "MI"},
    }

    # 3. Invoke the underlying LangGraph logistics pipeline engine natively
    graph_state_output = shipping_graph_engine.invoke(
        {"order_event": breached_payload, "action_type": "PROCESS_LOGISTICS"}
    )

    # 4. HARD STATE ASSERTIONS: Prove the workflow engine returned the correct status token
    assert graph_state_output.get("status") == "COMPLETED"

    # 5. 🛠️ FIXED: Assert that the database persistence tool was called to write the
    # explicit LEGAL_REJECTION_MI failure log token to your Postgres ledger!
    mock_persistence.assert_called_once_with(
        order_event=breached_payload,
        ledger_status="LEGAL_REJECTION_MI",
        status_msg="FAILED",
        reason_text="Fulfillment Aborted: Legal distribution constraint prohibits shirt logistics inside Michigan.",
    )
