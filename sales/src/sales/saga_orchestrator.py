import json
import uuid

from observability.framework.app_base import MicroserviceConsumerApp

from sales.db import Outbox, SagaState, SessionLocal


class SalesSagaOrchestratorApplication(MicroserviceConsumerApp):
    """Concrete child class that inherits all Kafka polling loops and telemetry

    context management out-of-band, executing the Central Sales Saga workflow check-offs.
    """

    def __init__(self):
        super().__init__(
            service_name="sales-saga-orchestrator",
            group_base_id="enterprise_master_saga_orchestrator_group",
            topic_channel="saga_replies",
            schema_filename="saga_reply.avsc",
        )

    def execute_business_logic(self, order_payload: dict, action: str):
        # Process worker responses against your central state log checklists
        self.process_incoming_saga_reply(order_payload)

    def issue_compensating_cancellations(self, db, order_id: str, triggering_dept: str):
        """SAGA CORE ENGINE: Commits an atomic multi-row database write to issue

        compensating cancellations to all other active worker departments.
        """
        self.logger.warning(
            f"🚨 SAGA INTERVENTION ENGINE | Order UUID: {order_id} | Failed at: {triggering_dept}"
        )

        context_dict = {
            "order_id": order_id,
            "reason": f"Saga Aborted due to compliance violation in {triggering_dept}",
        }
        serialized_context = json.dumps(context_dict)

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
                "order_id": order_id,
                "action": "CANCEL_TRANSACTION",
                "payload": serialized_context,
            }

            rollback_command_row = Outbox(
                topic=queue_topic, partition_key=order_id, payload=json.dumps(envelope)
            )
            db.add(rollback_command_row)
            self.logger.info(
                f"   ├── Staged Compensating Rollback Command -> Channel: {queue_topic}"
            )

    def process_incoming_saga_reply(self, reply: dict):
        """Processes worker responses against the central checklist logs to guide the Saga path."""
        # 🔬 DEBUG ACCORDION GATE 1: Dump the raw string payload dict shape right on ingestion
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
                # 🔬 DEBUG ACCORDION GATE 2: Track if the DB lookup fails due to structure mismatches
                print(
                    f"   |── ❌ [LOOKUP FAILURE]: SagaState record not found on disk for Order UUID: {order_id}"
                )
                self.logger.warning(
                    f"Saga state log record not found for Order UUID: {order_id}. Skipping."
                )
                return

            if state.saga_status in ["COMPLETED", "REJECTED", "IN_TRANSIT"]:
                print(
                    f"   └── ⚠️ [ALREADY TERMINAL]: State is already {state.status}. Skipping."
                )
                return

            self.logger.info(
                f"Evaluating Status Reply | Department: {dept} | Outcome: {status} | Order: {order_id}"
            )

            # Assign single, explicit department status metrics
            if dept == "FINANCE":
                state.finance_status = status
            elif dept == "SHIPPING":
                state.shipping_status = status
            elif dept == "NOTIFICATIONS":
                state.notifications_status = status

            # Check for explicit failure conditions safely
            if (
                status == "FAILED"
                or state.finance_status == "PAYMENT_REJECTED"
                or state.shipping_status == "FAILED"
            ):
                state.saga_status = "REJECTED"
                self.issue_compensating_cancellations(
                    db, order_id, triggering_dept=dept
                )

                db.commit()
                print(
                    f"   └── 🚨 [STATE SET]: Hard REJECTED recorded for Order: {order_id}"
                )
                return

            if (
                state.finance_status == "CREDIT_APPROVED"
                and state.shipping_status == "SHIPMENT_SECURED"
                and state.notifications_status == "SUCCESS"
            ):
                state.saga_status = "IN_TRANSIT"
                print(
                    f"   └── 🚚 [STATE EVOLUTION]: All guards cleared green! Flipped Order {order_id} to IN_TRANSIT."
                )
                self.logger.info(
                    f"🏆 IN_TRANSIT | All Workers Cleared Checklist | Saga Order: {order_id} Successfully Dispatched."
                )

            db.commit()
            print(
                f"   └── 💾 [DB COMMIT]: Saved state fields to ledger disk. Status: {state.saga_status} | Fin: {state.finance_status} | Ship: {state.shipping_status} | Notif: {state.notifications_status}"
            )

        except Exception as e:
            db.rollback()
            print(f"   └── 💥 [CRITICAL CONDUCTOR EXCEPTION]: {str(e)}")
            self.logger.error(f"Saga Conductor Database Process Exception: {str(e)}")
        finally:
            db.close()


if __name__ == "__main__":
    # Instantiate the application object and launch the continuous polling loop natively
    app = SalesSagaOrchestratorApplication()
    app.start_polling_loop()
