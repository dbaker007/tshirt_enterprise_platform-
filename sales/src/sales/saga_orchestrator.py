import json
import logging
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from observability.framework.app_base import MicroserviceConsumerApp

from sales.db import Outbox, SagaState, SessionLocal, init_sales_db


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
                topic=queue_topic, key=order_id, payload=json.dumps(envelope)
            )
            db.add(rollback_command_row)
            self.logger.info(
                f"   ├── Staged Compensating Rollback Command -> Channel: {queue_topic}"
            )

    def process_incoming_saga_reply(self, reply: dict):
        """Processes worker responses against the central checklist logs to guide the Saga path."""
        db = SessionLocal()
        order_id = reply.get("order_id")
        dept = reply.get("department")
        status = reply.get("status")

        try:
            state = db.query(SagaState).filter(SagaState.order_id == order_id).first()
            if not state:
                self.logger.warning(
                    f"Saga state log record not found for Order UUID: {order_id}. Skipping."
                )
                return

            if state.status in ["COMPLETED", "REJECTED"]:
                return

            self.logger.info(
                f"Evaluating Status Reply | Department: {dept} | Outcome: {status} | Order: {order_id}"
            )

            if dept == "FINANCE":
                state.finance_status = status
            elif dept == "SHIPPING":
                state.shipping_status = status
            elif dept == "NOTIFICATIONS":
                state.notifications_status = status

            if status == "FAILED" or state.finance_status == "PAYMENT_REJECTED":
                state.status = "ROLLING_BACK"
                self.issue_compensating_cancellations(
                    db, order_id, triggering_dept=dept
                )
                state.status = "REJECTED"
                db.commit()
                self.logger.warning(
                    f"Saga Complete Fail State Logged | Order UUID: {order_id} has been fully canceled."
                )
                return

            if (
                state.finance_status in ["SUCCESS", "CREDIT_APPROVED"]
                and state.shipping_status in ["SUCCESS", "SHIPMENT_SECURED"]
                and state.notifications_status
                in ["SUCCESS", "PENDING_FINANCIAL_CLEARANCE"]
            ):
                state.status = "COMPLETED"
                self.logger.info(
                    f"🏆 SUCCESS | All Workers Cleared Checklist | Saga Order: {order_id} Successfully Closed."
                )

            db.commit()

        except Exception as e:
            db.rollback()
            self.logger.error(f"Saga Conductor Database Process Exception: {str(e)}")
        finally:
            db.close()


if __name__ == "__main__":
    # Instantiate the application object and launch the continuous polling loop natively
    app = SalesSagaOrchestratorApplication()
    app.start_polling_loop()
