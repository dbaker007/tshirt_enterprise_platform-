from observability.framework.app_base import MicroserviceConsumerApp

from finance.db import init_finance_db
from finance.graph import finance_graph_engine


class FinanceConsumerApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Finance LangGraph pipeline.
    """

    def __init__(self):
        init_finance_db()
        super().__init__(
            service_name="finance-auditing-service",
            group_base_id="enterprise_finance_processing_group",
            topic_channel="finance_commands",
            schema_filename="command_envelope.avsc",
        )

    def execute_business_logic(self, order_payload: dict, action: str):
        self.logger.info(
            f"📥 [FINANCE CONSUMER INGEST]: Received control signal: {action}"
        )
        finance_graph_engine.invoke(
            {
                "order_event": order_payload,
                "action": str(action),  # Enforces the global "action" contract handle!
                "status": "STARTED",  # Resets state status loop metrics natively
            }
        )


if __name__ == "__main__":
    # Instantiate the application object and launch the continuous polling loop natively
    app = FinanceConsumerApplication()
    app.start_polling_loop()
