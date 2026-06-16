import sys

from observability.framework.app_base import MicroserviceConsumerApp

from notifications.graph import notifications_subgraph_node


class NotificationsConsumerApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Notifications subgraph.
    """

    def __init__(self):
        super().__init__(
            service_name="notifications-alert-service",
            group_base_id="enterprise_notifications_processing_group",
            topic_channel="notifications_commands",
            schema_filename="command_envelope.avsc",
        )

    def execute_business_logic(self, order_payload: dict, action: str):
        # Invoke your decoupled subgraph node directly inside the parent context window
        notifications_subgraph_node(order_payload, action)


if __name__ == "__main__":
    # Instantiate the application object and launch the continuous polling loop natively
    app = NotificationsConsumerApplication()
    app.start_polling_loop()
