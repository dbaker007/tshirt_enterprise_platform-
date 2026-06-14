import json
import logging
import os
import sys
import uuid
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from sales.db import Outbox, SagaState, SessionLocal, init_sales_db

# =========================================================================
# 1. ENTERPRISE LOGGER SPECIFICATION
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SAGA_ORCHESTRATOR")

SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def initialize_orchestrator_dependencies() -> AvroDeserializer:
    init_sales_db()
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    # 3-hop relative path traversal to load the strict envelope contract from root schemas/ hub
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "schemas", "command_envelope.avsc"
    )
    with open(schema_path, "r") as f:
        schema_str = f.read()
    return AvroDeserializer(client, schema_str, lambda obj, ctx: obj)


# =========================================================================
# 2. THE MULTI-LANE COMPENSATION DISTRIBUTOR (Fulfills Task 4)
# =========================================================================
def issue_compensating_cancellations(db, order_id: str, triggering_dept: str):
    """SAGA CORE ENGINE: Commits an atomic multi-row database write to issue

    compensating cancellations to all other active worker departments.
    """
    logger.warning(
        f"🚨 SAGA INTERVENTION ENGINE | Order UUID: {order_id} | Failed at: {triggering_dept}"
    )

    # Pack up our contextual rollback envelope data
    context = {
        "order_id": order_id,
        "reason": f"Saga Aborted due to compliance violation in {triggering_dept}",
    }
    serialized_context = json.dumps(context)

    # 15-Department Scalable Strategy Map: Define our global destination channels
    all_workers = [
        (
            "finance_commands",
            "FINANCE",
        ),  # ◄── NOW EXPLICITLY TRACKED FOR SHIPPING ROLLBACKS
        ("shipping_commands", "SHIPPING"),
        ("notifications_commands", "NOTIFICATIONS"),
    ]

    for queue_topic, dept_token in all_workers:
        # Task 4 Strict Constraint Check: Skip the generating department to prevent infinite bounce loops!
        if dept_token == triggering_dept:
            continue

        envelope = {
            "command_id": str(uuid.uuid4()),
            "order_id": order_id,
            "action": "CANCEL_TRANSACTION",  # ◄── THE EXPLICIT COMPENSATION INSTRUCTION
            "payload": serialized_context,
        }

        rollback_command_row = Outbox(
            topic=queue_topic, key=order_id, payload=json.dumps(envelope)
        )
        db.add(rollback_command_row)
        logger.info(
            f"   ├── Staged Compensating Rollback Command -> Channel: {queue_topic}"
        )


def process_incoming_saga_reply(reply: dict):
    """Processes worker responses against the central checklist logs to guide the Saga path."""
    db = SessionLocal()
    order_id = reply.get("order_id")
    dept = reply.get("department")
    status = reply.get("status")

    try:
        # Fetch the active state tracking model checklist row straight from Postgres
        state = db.query(SagaState).filter(SagaState.order_id == order_id).first()
        if not state:
            logger.warning(
                f"Saga state log record not found for Order UUID: {order_id}. Skipping."
            )
            return

        # If the Saga was already flagged as failed or completed, ignore subsequent straggler replies
        if state.status in ["COMPLETED", "REJECTED"]:
            return

        logger.info(
            f"Evaluating Status Reply | Department: {dept} | Outcome: {status} | Order: {order_id}"
        )

        # Map and latch the specific reporting worker's status column
        if dept == "FINANCE":
            state.finance_status = status
        elif dept == "SHIPPING":
            state.shipping_status = status
        elif dept == "NOTIFICATIONS":
            state.notifications_status = status

        # EVALUATION STEP LANE A: Catch and intercept failures instantly to launch compensation loops
        if status == "FAILED" or state.finance_status == "PAYMENT_REJECTED":
            state.status = "ROLLING_BACK"
            # TRIGGER TASK 4: Write all cancellations to the outbox database table atomically
            issue_compensating_cancellations(db, order_id, triggering_dept=dept)
            state.status = "REJECTED"
            db.commit()
            logger.warning(
                f"Saga Complete Fail State Logged | Order UUID: {order_id} has been fully canceled and rolled back."
            )
            return

        # EVALUATION STEP LANE B: Evaluate if every single worker checked off successfully
        if (
            state.finance_status in ["SUCCESS", "CREDIT_APPROVED"]
            and state.shipping_status in ["SUCCESS", "SHIPMENT_SECURED"]
            and state.notifications_status in ["SUCCESS", "PENDING_FINANCIAL_CLEARANCE"]
        ):
            state.status = "COMPLETED"
            logger.info(
                f"🏆 SUCCESS | All Workers Cleared Checklist | Saga Order: {order_id} Successfully Closed."
            )

        db.commit()

    except Exception as e:
        db.rollback()
        logger.error(f"Saga Conductor Database Process Exception: {str(e)}")
    finally:
        db.close()


# =========================================================================
# 3. CONCURRENT POLLING INTERFACE
# =========================================================================
if __name__ == "__main__":
    avro_deserializer = initialize_orchestrator_dependencies()

    # Shared schema registry link for reading incoming replies
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "schemas", "saga_reply.avsc"
    )
    with open(schema_path, "r") as f:
        reply_schema_str = f.read()

    reply_deserializer = AvroDeserializer(
        client, reply_schema_str, lambda obj, ctx: obj
    )

    CONSUMER_CONFIG = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "enterprise_master_saga_orchestrator_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe(["saga_replies"])

    logger.info(
        "Sales Saga Orchestrator Core Active, Schema-Locked, and Polling 'saga_replies'..."
    )
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    break

            try:
                context = SerializationContext(msg.topic(), MessageField.VALUE)
                reply_payload = reply_deserializer(msg.value(), context)

                # Feed the worker outcome straight into the state controller logic
                process_incoming_saga_reply(reply_payload)
                consumer.commit(msg, asynchronous=False)
            except Exception as e:
                logger.error(f"Orchestrator Stream Processing Error: {str(e)}")
                consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        logger.info("Orchestrator core loop disengaging safely.")
    finally:
        consumer.close()
