from unittest.mock import patch

import pytest

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient"),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer"),
    patch(
        "observability.db.get_platform_database_url", return_value="sqlite:///:memory:"
    ),
):
    from notifications.app import NotificationsConsumerApplication


@patch("notifications.app.notifications_graph_engine.invoke")
def test_notifications_consumer_app_forwards_payload_and_action_to_graph(
    mock_graph_invoke, test_db_session
):
    """Verifies that the parent application wrapper accurately initializes and forwards

    both the event data and action context into the LangGraph engine on ingestion loops [1.1].
    """
    sample_payload = {"order_id": "notif-test-999", "customer_name": "Charlie"}
    sample_action = "CANCEL_TRANSACTION"

    def mock_session_factory():
        return test_db_session

    with patch("notifications.app.init_notifications_db"):
        app = NotificationsConsumerApplication()
        app.SessionLocal = mock_session_factory

        app.execute_business_logic(order_payload=sample_payload, action=sample_action)

    expected_config = {
        "configurable": {"thread_id": "notif-test-999", "db": test_db_session}
    }

    mock_graph_invoke.assert_called_once_with(
        {"order_event": sample_payload, "action": sample_action, "status": "STARTED"},
        expected_config,
    )
