import uuid

from observability.framework.app_base import MicroserviceConsumerApp
from observability.outbox import stage_outbox_message
from opentelemetry import trace
from sales.orchestrator.db import SagaState, SessionLocal, init_orchestrator_db


class SalesSagaOrchestratorApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Central Sales Saga workflow check-offs.
    """

    def __init__(self):
        init_orchestrator_db()
        super().__init__(
            service_name="sales-saga-orchestrator",
            group_base_id="enterprise_master_saga_orchestrator_group",
            topic_channel="saga_replies",
            schema_filename="saga_reply.avsc",
        )

    def execute_business_logic(self, order_payload: dict, action: str):
        self.process_incoming_saga_reply(order_payload)

    def issue_compensating_cancellations(self, db, order_id: str, triggering_dept: str):
        """SAGA CORE ENGINE: Commits an atomic multi-row database write to issue
        compensating cancellations to all other active worker departments.
        """
        self.logger.warning(
            f"🚨 SAGA INTERVENTION ENGINE | Order UUID: {order_id} | Failed at: {triggering_dept}"
        )

        # 🟢 FIX: Fetch the actual, existing state row to extract the true transaction metrics!
        state_record = (
            db.query(SagaState).filter(SagaState.order_id == str(order_id)).first()
        )

        # Fall back gracefully to defaults ONLY if the relational row lookup fails completely
        customer_name = getattr(state_record, "customer_name", "Unknown Buyer")
        customer_email = getattr(
            state_record, "customer_email", "unknown@platform.internal"
        )
        order_amount = float(getattr(state_record, "amount", 0.0))
        item_id = getattr(state_record, "item_id", "SHIRT_STANDARD_BLUE")

        # Safely extract the original nested address blocks preserved inside your state schema model
        # (Assuming your table stores these fields or maps them to a JSON/text attribute)
        shipping_state = getattr(state_record, "shipping_state", "OH")
        street_address = getattr(state_record, "shipping_street", "123 Transaction Way")
        city_name = getattr(state_record, "shipping_city", "Default Ville")
        postal_code = getattr(state_record, "shipping_postal", "00000")

        # Clean dictionary structure populated with real, active transaction data!
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
                "payload": context_payload,  # Passed directly as a clean native dictionary object
            }

            stage_outbox_message(
                db=db, topic=queue_topic, partition_key=order_id, payload=envelope
            )
            self.logger.info(
                f"   ├── Staged Compensating Rollback Command -> Channel: {queue_topic}"
            )

    def process_incoming_saga_reply(self, reply: dict):
        """Processes worker responses against the central checklist logs to guide the Saga path."""
        tracer = trace.get_tracer("sales-saga-orchestrator")

        with tracer.start_as_current_span("process_incoming_saga_reply") as span:
            db = SessionLocal()
            order_id = reply.get("order_id")
            dept = reply.get("department")
            status = reply.get("status")
            reason = str(reply.get("reason", "")).lower()

            try:
                state = (
                    db.query(SagaState).filter(SagaState.order_id == order_id).first()
                )
                if not state:
                    self.logger.error(
                        f"SagaState record not found on disk for Order UUID: {order_id}"
                    )
                    return

                span.set_attribute("order.correlation_id", str(order_id))
                span.set_attribute("worker.department", str(dept))
                span.set_attribute("worker.status_outcome", str(status))

                self.logger.info(
                    f"Evaluating Status Reply | Department: {dept} | Outcome: {status} | Order: {order_id}"
                )

                # 🟢 STEP 1: Parse the clean protocol status contract rules
                # Differentiate between a lagging forward pass and an actual compensation rollback completion
                if (
                    "compensation" in reason
                    or "released" in reason
                    or "rolled" in reason
                ):
                    mapped_status = "ROLLED_BACK"
                elif status == "SUCCESS":
                    mapped_status = "SUCCESS"
                else:
                    mapped_status = "FAILED"

                # 🟢 STEP 2: Update individual checklist registers dynamically
                if dept == "FINANCE":
                    state.finance_status = mapped_status
                elif dept == "SHIPPING":
                    state.shipping_status = mapped_status
                elif dept == "NOTIFICATIONS":
                    state.notifications_status = mapped_status

                # 🟢 STEP 3: Centralized Global Lifecycle State Evolution
                # Trigger initial failure rejection and dispatch compensations immediately
                if mapped_status == "FAILED" and state.saga_status != "REJECTED":
                    state.saga_status = "REJECTED"
                    self.issue_compensating_cancellations(
                        db, order_id, triggering_dept=dept
                    )

                # Symmetrical forward path completion verification check
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

                # 🟢 STEP 4: ATOMIC UNIT OF WORK TRANSACTION PASS
                # Execute exactly one single commit block at the absolute end of a successful cycle
                db.commit()

            except Exception as e:
                db.rollback()
                span.record_exception(e)
                span.set_status(
                    trace.Status(trace.StatusCode.ERROR, description=str(e))
                )
                self.logger.error(
                    f"Saga Conductor Database Process Exception: {str(e)}"
                )
            finally:
                db.close()


if __name__ == "__main__":
    app = SalesSagaOrchestratorApplication()
    app.start_polling_loop()
