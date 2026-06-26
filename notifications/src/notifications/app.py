from observability.framework.app_base import MicroserviceConsumerApp

from notifications.db import init_notifications_db

# 🟢 FIX: Symmetrically align your imports with the compiled graph engine standard!
from notifications.graph import notifications_graph_engine


class NotificationsConsumerApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Notifications subgraph.
    """

    def __init__(self):
        init_notifications_db()
        super().__init__(
            service_name="notifications-alert-service",
            group_base_id="enterprise_notifications_processing_group",
            topic_channel="notifications_commands",
            schema_filename="command_envelope.avsc",
        )

    # 🟢 FIX: Explicitly forward the raw action contract variable down to your graph engine!
    def execute_business_logic(self, order_payload: dict, action: str):
        self.logger.info(
            f"📥 [NOTIFICATIONS CONSUMER INGEST]: Processing action context: {action} for order payload."
        )
        notifications_graph_engine.invoke(
            {
                "order_event": order_payload,
                "action": str(
                    action
                ),  # Explicitly binds "CANCEL_TRANSACTION" to the graph state register
                "status": "STARTED",
            }
        )


if __name__ == "__main__":
    # Instantiate the application object and launch the continuous polling loop natively
    app = NotificationsConsumerApplication()
    app.start_polling_loop()
