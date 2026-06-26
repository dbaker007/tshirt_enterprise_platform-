from unittest.mock import patch

import pytest

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient"),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer"),
):
    from finance.app import FinanceConsumerApplication


@patch("finance.app.finance_graph_engine.invoke")
def test_finance_consumer_app_forwards_payload_and_action_to_graph(mock_graph_invoke):
    """Verifies that the parent application wrapper accurately initializes and forwards

    both the event data and action context into the LangGraph engine on ingestion loops.
    """
    app = FinanceConsumerApplication()
    sample_payload = {"order_id": "api-test-uuid-001", "amount": 99.99}
    sample_action = "NEW_SALE"

    app.execute_business_logic(order_payload=sample_payload, action=sample_action)

    mock_graph_invoke.assert_called_once_with(
        {"order_event": sample_payload, "status": sample_action}
    )
