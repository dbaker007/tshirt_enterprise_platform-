import sys

from observability.framework.app_base import MicroserviceConsumerApp

from finance.graph import finance_graph_engine


class FinanceConsumerApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Finance LangGraph pipeline.
    """

    def __init__(self):
        super().__init__(
            service_name="finance-auditing-service",
            group_base_id="enterprise_finance_processing_group",
            topic_channel="finance_commands",
            schema_filename="command_envelope.avsc",
        )

    def execute_business_logic(self, order_payload: dict, action: str):
        # Simply fire your decoupled LangGraph execution engine cleanly inside the parent window
        finance_graph_engine.invoke({"order_event": order_payload})


if __name__ == "__main__":
    # Instantiate the application object and launch the continuous polling loop natively
    app = FinanceConsumerApplication()
    app.start_polling_loop()
