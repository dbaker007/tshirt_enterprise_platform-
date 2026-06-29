from observability.db import get_platform_database_url
from observability.framework.app_base import MicroserviceConsumerApp
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from notifications.db import init_notifications_db
from notifications.graph import notifications_graph_engine


class NotificationsConsumerApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Notifications subgraph.
    """

    def __init__(self):
        # 1. Dynamically retrieve the centralized production target string [1.1]
        database_url = get_platform_database_url()

        # 2. Instantiate and own the database driver connection engine pool safely [1.1]
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        # 3. Idempotently initialize and draw table schemas natively on boot [1.1]
        init_notifications_db(self.engine)

        super().__init__(
            service_name="notifications-alert-service",
            group_base_id="enterprise_notifications_processing_group",
            topic_channel="notifications_commands",
            schema_filename="command_envelope.avsc",
        )

    def execute_business_logic(self, order_payload: dict, action: str):
        self.logger.info(
            f"📥 [NOTIFICATIONS CONSUMER INGEST]: Received control signal: {action}"
        )

        order_id = order_payload.get("order_id", "unknown-uuid")

        # 🟢 EXPLICIT DEPENDENCY INJECTION: Instantiate a clean local database connection pass [1.1]
        db = self.SessionLocal()

        try:
            # Declare the configuration mapping context including the live database session [1.1]
            config = {"configurable": {"thread_id": str(order_id), "db": db}}

            # Forward both the state payload and the configuration context cleanly
            notifications_graph_engine.invoke(
                {
                    "order_event": order_payload,
                    "action": str(action),
                    "status": "STARTED",
                },
                config,
            )

            # 🟢 COMMIT BOUNDARY: Atomically flush database transactions after successful execution [1.1]
            db.commit()

        except Exception as pipeline_err:
            # 🟢 FAIL-SAFE: Roll back transaction immediately to prevent corrupt writes [1.1]
            db.rollback()
            self.logger.error(
                f"❌ Application transaction processing failure: {str(pipeline_err)}"
            )
            raise pipeline_err
        finally:
            db.close()


if __name__ == "__main__":
    app = NotificationsConsumerApplication()
    app.start_polling_loop()
