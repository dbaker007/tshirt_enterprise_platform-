import sys

from observability.framework.app_base import MicroserviceConsumerApp

from shipping.graph import shipping_graph_engine


class ShippingConsumerApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Shipping LangGraph logistics pipeline.
    """

    def __init__(self):
        super().__init__(
            service_name="shipping-fulfillment-service",
            group_base_id="enterprise_shipping_processing_group",
            topic_channel="shipping_commands",
            schema_filename="command_envelope.avsc",
        )

    def execute_business_logic(self, order_payload: dict, action: str):
        # Fire your decoupled LangGraph execution engine cleanly inside the parent context window
        shipping_graph_engine.invoke(
            {"order_event": order_payload, "action_type": action}
        )


if __name__ == "__main__":
    # Instantiate the application object and launch the continuous polling loop natively
    app = ShippingConsumerApplication()
    app.start_polling_loop()
