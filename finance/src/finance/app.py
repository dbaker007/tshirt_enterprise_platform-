import asyncio

from langgraph.errors import GraphInterrupt
from langgraph.types import Command
from observability.db import get_platform_database_url
from observability.framework.app_base import MicroserviceConsumerApp
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from finance.db import get_finance_checkpointer, init_finance_db
from finance.graph import builder


class FinanceConsumerApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Finance LangGraph pipeline.
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
        init_finance_db(self.engine)

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
        try:
            asyncio.run(self._run_async_graph_pipeline(order_payload, action))
        except GraphInterrupt:
            self.logger.info(
                f"⏸️  [SAGA SUSPENDED ON DISK]: Order UUID: {order_payload.get('order_id')} successfully paused for manual operator review."
            )

    async def _run_async_graph_pipeline(self, order_payload: dict, action: str):
        order_id = order_payload.get("order_id", "unknown-uuid")
        saver = await get_finance_checkpointer()
        db = self.SessionLocal()

        try:
            async with saver as active_saver:
                active_graph = builder.compile(checkpointer=active_saver)

                config = {
                    "configurable": {
                        "thread_id": str(order_id),
                        "db": db,
                    }
                }

                current_state = await active_graph.aget_state(config)

                if str(action) == "CANCEL_TRANSACTION":
                    self.logger.info(
                        f"🚨 [COMPENSATION OVERRIDE]: Forcing immediate rollback execution path for thread [{order_id}]"
                    )
                    await active_graph.ainvoke(
                        {
                            "order_event": order_payload,
                            "action": "CANCEL_TRANSACTION",
                            "status": "STARTED",
                        },
                        config,
                    )
                elif str(action) in ["APPROVE", "REJECT"]:
                    self.logger.info(
                        f"🧑‍✈️ [RESUMING PAUSED THREAD]: Feeding clean human override token -> [{action}] into thread [{order_id}]"
                    )
                    await active_graph.ainvoke(Command(resume=str(action)), config)
                else:
                    self.logger.info(
                        f"🚀 [INITIALIZING NEW THREAD]: Spawning state track records for thread [{order_id}]"
                    )
                    await active_graph.ainvoke(
                        {
                            "order_event": order_payload,
                            "action": str(action),
                            "status": "NEW_ORDER",
                        },
                        config,
                    )

                post_execution_state = await active_graph.aget_state(config)

                should_purge_thread = str(action) == "CANCEL_TRANSACTION" or (
                    str(action) == "REJECT"
                    and post_execution_state
                    and not post_execution_state.next
                )

                if should_purge_thread:
                    try:
                        await active_saver.adelete_thread(config)
                        self.logger.info(
                            f"🗑️ [APPLICATION PURGE]: Evicted cancelled thread context [{order_id}] from storage."
                        )
                    except Exception as e:
                        self.logger.error(
                            f"⚠️ Failed to purge checkpointer thread history out-of-band: {str(e)}"
                        )

            db.commit()

        except Exception as pipeline_err:
            db.rollback()
            self.logger.error(
                f"❌ Application transaction processing failure: {str(pipeline_err)}"
            )
            raise pipeline_err
        finally:
            db.close()


if __name__ == "__main__":
    app = FinanceConsumerApplication()
    app.start_polling_loop()
