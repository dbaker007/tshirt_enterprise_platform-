import asyncio
import logging

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

        # 🟢 EXPLICIT DEPENDENCY INJECTION: Instantiate a clean local database connection pass [1.1]
        db = self.SessionLocal()

        try:
            async with saver as active_saver:
                # 🟢 SOLUTION: Compile a fresh, stateful engine instance natively bound to the active saver token [1.1]
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
                # 🟢 SOLUTION: Clean, explicit extraction matching your true RESUME_REVIEW control signal [1.1]
                elif str(action) == "RESUME_REVIEW" or (
                    current_state and current_state.next
                ):
                    # Fetch the explicit verdict field passed natively through your framework envelope context [1.1]
                    verdict = str(order_payload.get("verdict", "REJECT")).upper()

                    self.logger.info(
                        f"🧑‍✈️ [RESUMING PAUSED THREAD]: Feeding clean human override token -> [{verdict}] into thread [{order_id}]"
                    )
                    await active_graph.ainvoke(Command(resume=str(verdict)), config)
                else:
                    self.logger.info(
                        f"🚀 [INITIALIZING NEW THREAD]: Spawning state track records for thread [{order_id}]"
                    )
                    await active_graph.ainvoke(
                        {
                            "order_event": order_payload,
                            "action": str(action),
                            "status": "STARTED",
                        },
                        config,
                    )

                # Pure lifecycle cleanup logic execution block [1.1]
                if str(action) == "CANCEL_TRANSACTION" or (
                    current_state
                    and current_state.next
                    and order_payload.get("verdict") == "REJECT"
                ):
                    try:
                        # Invoke the deletion method straight on your active, opened saver token channel [1.1]
                        await active_saver.adelete_thread(config)
                        self.logger.info(
                            f"🗑️ [APPLICATION PURGE]: Evicted cancelled thread context [{order_id}] from storage."
                        )
                    except Exception as e:
                        self.logger.error(
                            f"⚠️ Failed to purge checkpointer thread history out-of-band: {str(e)}"
                        )

            # 🟢 COMMIT BOUNDARY: Atomically flush database transactions to disk after a successful pass [1.1]
            db.commit()

        except Exception as pipeline_err:
            # 🟢 FAIL-SAFE: Safely rollback database transactions to shield tables from corrupt logs [1.1]
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
