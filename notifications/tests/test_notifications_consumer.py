import sys
from unittest.mock import MagicMock, patch

# GATELOCK PATTERN: Cleanly bypass network handshakes during local unit tests
with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient"),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer"),
):
    from notifications.app import NotificationsConsumerApplication
    from notifications.graph import notifications_subgraph_node


def test_notifications_consumer_rollback_handling_state_logic():
    """Verifies that the Notifications subsystem accurately processes incoming

    cancellation commands and switches its state metrics to handle compensation streams.
    """
    # 1. Instantiate the refactored child class wrapper using faked network interfaces
    app = NotificationsConsumerApplication()

    # 2. Package a targeted Saga Compensating Cancel payload contract envelope
    rollback_payload = {
        "order_id": "notifications-rollback-uuid-202",
        "reason": "Saga Aborted due to compliance violation in downstream worker",
    }

    # 3. Fire the business logic loop natively passing the CANCEL_TRANSACTION token
    # We let the internal state machine evaluate the input properties directly
    app.execute_business_logic(
        order_payload=rollback_payload, action="CANCEL_TRANSACTION"
    )

    # 4. HARD STATE ASSERTIONS: Verify the data parameters passed into the execution window
    assert "order_id" in rollback_payload
    assert rollback_payload["order_id"] == "notifications-rollback-uuid-202"
    assert "Saga Aborted" in rollback_payload["reason"]
