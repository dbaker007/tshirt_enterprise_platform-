import json
import logging
import uuid

from observability.framework.app_base import MicroserviceConsumerApp
from observability.outbox import stage_outbox_message
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
        print(
            f"\n📥 [ORCHESTRATOR RAW INGESTION]: Received packet layout: {repr(reply)} (Type: {type(reply)})"
        )

        db = SessionLocal()
        order_id = reply.get("order_id")
        dept = reply.get("department")
        status = reply.get("status")

        print(
            f"   ├── Parsed Keys -> order_id: {repr(order_id)} | dept: {repr(dept)} | status: {repr(status)}"
        )

        try:
            state = db.query(SagaState).filter(SagaState.order_id == order_id).first()
            if not state:
                print(
                    f"   |── ❌ [LOOKUP FAILURE]: SagaState record not found on disk for Order UUID: {order_id}"
                )
                return

            print(
                f"   ├── 🔍 [PRE-MUTATION STATE DUMP]: Order UUID: {order_id}\n"
                f"   │   ├── Global Saga Status:   {state.saga_status}\n"
                f"   │   ├── Finance Register:     {state.finance_status}\n"
                f"   │   ├── Shipping Register:    {state.shipping_status}\n"
                f"   │   └── Notification Register: {state.notifications_status}"
            )

            if state.saga_status in ["COMPLETED", "IN_TRANSIT", "ROLLED_BACK"]:
                print(
                    f"   └── ⚠️ [ALREADY TERMINAL]: State is already {state.saga_status}. Skipping."
                )
                return

            self.logger.info(
                f"Evaluating Status Reply | Department: {dept} | Outcome: {status} | Order: {order_id}"
            )

            # Enforce clean protocol contract status mapping rules
            if status == "SUCCESS":
                mapped_status = "SUCCESS"
            elif status == "ROLLED_BACK":
                mapped_status = "ROLLED_BACK"
            else:
                mapped_status = "FAILED"

            # 🟢 COMPENSATION LIFECYCLE ROUTING BLOCK
            if state.saga_status == "REJECTED":
                # Guard: Drop lagging forward success replies if they match an already populated success column
                current_register_val = getattr(state, f"{dept.lower()}_status")
                if mapped_status == "SUCCESS" and current_register_val in [
                    "SUCCESS",
                    "ROLLED_BACK",
                ]:
                    print(
                        f"   └── ⚠️  [LAGGING FORWARD DROP]: Worker {dept} sent SUCCESS, but register reads {current_register_val}. "
                        f"Bypassing overwrite to protect compensation context."
                    )
                    db.commit()
                    return

                print(
                    f"   └── 🛠️  [COMPENSATION RECORDED]: Logging rollback step for {dept} -> {mapped_status}"
                )
                if dept == "FINANCE":
                    state.finance_status = mapped_status
                elif dept == "SHIPPING":
                    state.shipping_status = mapped_status
                elif dept == "NOTIFICATIONS":
                    state.notifications_status = mapped_status

                # Check if all sibling registers have completed their terminal failure or rollback cycles
                if (
                    state.finance_status in ["FAILED", "ROLLED_BACK"]
                    and state.shipping_status in ["FAILED", "ROLLED_BACK"]
                    and state.notifications_status in ["FAILED", "ROLLED_BACK"]
                ):
                    state.saga_status = "ROLLED_BACK"
                    print(
                        f"   └── 🔄 [SAGA EVOLUTION]: All compensation paths verified! Flipped order {order_id} to ROLLED_BACK."
                    )

                db.commit()
                print(
                    f"   └── 💾 [DB COMMIT]: Saved compensation fields.\n"
                    f"       ├── Global Saga Status:   {state.saga_status}\n"
                    f"       ├── Finance Register:     {state.finance_status}\n"
                    f"       ├── Shipping Register:    {state.shipping_status}\n"
                    f"       └── Notification Register: {state.notifications_status}"
                )
                return

            # --- STANDARD FORWARD PROCESSING BLOCK ---
            if dept == "FINANCE":
                state.finance_status = mapped_status
            elif dept == "SHIPPING":
                state.shipping_status = mapped_status
            elif dept == "NOTIFICATIONS":
                state.notifications_status = mapped_status

            # Trigger initial failure rejection and dispatch compensations immediately
            if mapped_status == "FAILED" and state.saga_status != "REJECTED":
                state.saga_status = "REJECTED"
                self.issue_compensating_cancellations(
                    db, order_id, triggering_dept=dept
                )
                db.commit()
                print(
                    f"   └── 🚨 [INITIATED REJECTION]: Hard REJECTED recorded. Compensations dispatched for Order: {order_id}"
                )
                return

            # Symmetrical forward path verification check
            if (
                state.saga_status == "STARTED"
                and state.finance_status == "SUCCESS"
                and state.shipping_status == "SUCCESS"
                and state.notifications_status == "SUCCESS"
            ):
                state.saga_status = "IN_TRANSIT"
                self.logger.info(
                    f"🏆 IN_TRANSIT | All Workers Cleared Checklist | Saga Order: {order_id} Dispatched."
                )

            db.commit()
            print(
                f"   └── 💾 [DB COMMIT]: Saved state fields to ledger disk.\n"
                f"       ├── Global Saga Status:   {state.saga_status}\n"
                f"       ├── Finance Register:     {state.finance_status}\n"
                f"       ├── Shipping Register:    {state.shipping_status}\n"
                f"       └── Notification Register: {state.notifications_status}"
            )

        except Exception as e:
            db.rollback()
            print(f"   └── 💡 [CRITICAL CONDUCTOR EXCEPTION]: {str(e)}")
            self.logger.error(f"Saga Conductor Database Process Exception: {str(e)}")
        finally:
            db.close()


if __name__ == "__main__":
    app = SalesSagaOrchestratorApplication()
    app.start_polling_loop()
