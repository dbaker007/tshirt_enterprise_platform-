# sales/src/sales/orchestrator/main.py

import uuid

from observability.db import get_platform_database_url
from observability.framework.app_base import MicroserviceConsumerApp
from observability.outbox import stage_outbox_message
from opentelemetry import trace
from sales.orchestrator.db import SagaState, init_orchestrator_db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class SalesSagaOrchestratorApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry
    context management out-of-band, executing the Central Sales Saga workflow check-offs.
    """

    def __init__(self):
        # 1. Dynamically retrieve the centralized production target string [1.1]
        database_url = get_platform_database_url()

        # 2. Instantiate and own the database driver connection engine pool safely [1.1]
        # 🟢 SOLUTION: Keep the database connection pool clean, raw, and un-intercepted [1.1]
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        # 3. Idempotently initialize and draw table schemas natively on boot [1.1]
        init_orchestrator_db(self.engine)

        super().__init__(
            service_name="sales-saga-orchestrator",
            group_base_id="enterprise_master_saga_orchestrator_group",
            topic_channel="saga_replies",
            schema_filename="saga_reply.avsc",
        )

    def execute_business_logic(self, order_payload: dict, action: str):
        # 🟢 EXPLICIT DEPENDENCY INJECTION: Open local database transaction boundary [1.1]
        db = self.SessionLocal()
        try:
            self.process_incoming_saga_reply(order_payload, db)
            # 🟢 COMMIT BOUNDARY: Flush transactions after successful check-offs [1.1]
            db.commit()
        except Exception as e:
            db.rollback()
            self.logger.error(
                f"❌ Saga Orchestrator execution transaction failure: {str(e)}"
            )
            raise e
        finally:
            db.close()

    def issue_compensating_cancellations(self, db, order_id: str, triggering_dept: str):
        """SAGA CORE ENGINE: Commits an atomic multi-row database write to issue
        compensating cancellations to all other active worker departments.
        """
        self.logger.warning(
            f"🚨 SAGA INTERVENTION ENGINE | Order UUID: {order_id} | Failed at: {triggering_dept}"
        )

        state_record = (
            db.query(SagaState).filter(SagaState.order_id == str(order_id)).first()
        )

        customer_name = getattr(state_record, "customer_name", "Unknown Buyer")
        customer_email = getattr(
            state_record, "customer_email", "unknown@platform.internal"
        )
        order_amount = float(getattr(state_record, "amount", 0.0))
        item_id = getattr(state_record, "item_id", "SHIRT_STANDARD_BLUE")

        shipping_state = getattr(state_record, "shipping_state", "OH")
        street_address = getattr(state_record, "shipping_street", "123 Transaction Way")
        city_name = getattr(state_record, "shipping_city", "Default Ville")
        postal_code = getattr(state_record, "shipping_postal", "00000")

        context_payload = {
            "customer_name": str(customer_name),
            "customer_email": str(customer_email),
            "amount": order_amount,
            "item_id": str(item_id),
            "shipping_address": {
                "street": str(street_address),
                "city": str(city_name),
                "state": str(shipping_state),
                "postal_code": str(postal_code),
            },
        }

        all_workers = [
            ("finance_commands", "FINANCE"),
            ("shipping_commands", "SHIPPING"),
            ("notifications_commands", "NOTIFICATIONS"),
        ]

        for queue_topic, dept_token in all_workers:
            if dept_token == triggering_dept:
                continue

            envelope = {
                "command_id": str(uuid.uuid4()),
                "order_id": str(order_id),
                "action": "CANCEL_TRANSACTION",
                "payload": context_payload,
            }

            # 🟢 SOLUTION: Dispatches straight to your pristine, un-translated public outbox logging framework [1.1]
            stage_outbox_message(
                db=db, topic=queue_topic, partition_key=order_id, payload=envelope
            )
            self.logger.info(
                f"   ├── Staged Compensating Rollback Command -> Channel: {queue_topic}"
            )

    def process_incoming_saga_reply(self, reply: dict, db):
        """Processes worker responses against the central checklist logs to guide the Saga path."""
        tracer = trace.get_tracer("sales-saga-orchestrator")

        with tracer.start_as_current_span("process_incoming_saga_reply") as span:
            order_id = reply.get("order_id")
            dept = reply.get("department")
            status = reply.get("status")  # Binary Indicator: "SUCCESS" or "FAILED"
            ledger_status = reply.get(
                "ledger_status"
            )  # Descriptive Status: "FAILED_LEGAL", "SUCCESS", "ROLLED_BACK"

            state = db.query(SagaState).filter(SagaState.order_id == order_id).first()
            if not state:
                self.logger.error(
                    f"SagaState record not found on disk for Order UUID: {order_id}"
                )
                return

            span.set_attribute("order.correlation_id", str(order_id))
            span.set_attribute("worker.department", str(dept))
            span.set_attribute("worker.status_outcome", str(status))

            self.logger.info(
                f"Evaluating Status Reply | Department: {dept} | Wire Status: {status} | Ledger Status: {ledger_status} | Order: {order_id}"
            )

            # 🟢 STEP 1: Direct Mapping - Blindly stamp the descriptive ledger_status value passed in
            if dept == "FINANCE":
                state.finance_status = ledger_status
            elif dept == "SHIPPING":
                state.shipping_status = ledger_status
            elif dept == "NOTIFICATIONS":
                state.notifications_status = ledger_status

            # 🟢 STEP 2: Failure Evaluation - Evaluates the strict binary indicator to run compensations
            if status == "FAILED":
                if state.saga_status != "REJECTED":
                    state.saga_status = "REJECTED"
                    self.logger.warning(
                        f"🚨 Global Saga Rejected by {dept} for Order {order_id}. Issuing compensation workflows."
                    )
                    self.issue_compensating_cancellations(
                        db, order_id, triggering_dept=dept
                    )
                else:
                    self.logger.info(
                        f"🔄 Compensation already in flight or executed for Order {order_id}. Skipping duplicate trigger."
                    )

            # 🟢 STEP 3: Forward Success Boundary Evaluation - Checks for uniform cross-domain success string tokens
            elif (
                state.saga_status == "STARTED"
                and state.finance_status == "SUCCESS"
                and state.shipping_status == "SUCCESS"
                and state.notifications_status == "SUCCESS"
            ):
                state.saga_status = "IN_TRANSIT"
                self.logger.info(
                    f"🏆 IN_TRANSIT | All Workers Cleared Checklist | Saga Order: {order_id} Dispatched."
                )


if __name__ == "__main__":
    app = SalesSagaOrchestratorApplication()
    app.start_polling_loop()
