from unittest.mock import patch

import pytest

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient", create=True),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer", create=True),
):
    from shipping.app import ShippingConsumerApplication


@patch("shipping.app.shipping_graph_engine.invoke")
def test_shipping_consumer_app_forwards_payload_and_action_to_graph(mock_graph_invoke):
    """Verifies that the parent application wrapper accurately initializes and forwards

    both the event data and action context into the LangGraph engine on ingestion loops.
    """
    app = ShippingConsumerApplication()
    sample_payload = {"order_id": "ship-test-999", "shipping_address": {"state": "TX"}}
    sample_action = "NEW_SALE"

    app.execute_business_logic(order_payload=sample_payload, action=sample_action)

    mock_graph_invoke.assert_called_once_with(
        {"order_event": sample_payload, "action_type": sample_action}
    )
