from unittest.mock import patch

import pytest

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient"),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer"),
):
    from notifications.app import NotificationsConsumerApplication


@patch("notifications.app.notifications_graph_engine.invoke")
def test_notifications_consumer_app_forwards_payload_and_action_to_graph(
    mock_graph_invoke,
):
    """Verifies that the parent application wrapper accurately initializes and forwards

    both the event data and action context into the LangGraph engine on ingestion loops.
    """
    app = NotificationsConsumerApplication()
    sample_payload = {"order_id": "notif-test-999", "customer_name": "Charlie"}
    sample_action = "CANCEL_TRANSACTION"

    app.execute_business_logic(order_payload=sample_payload, action=sample_action)

    mock_graph_invoke.assert_called_once_with(
        {"order_event": sample_payload, "action": sample_action}
    )
