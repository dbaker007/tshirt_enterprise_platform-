import json
import logging
import os
import sys
from typing import Any, Dict, Literal

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

# Import your active repository functions directly from your sibling database file
from finance.db import execute_financial_clearance_and_stage_outbox, init_finance_db

# =========================================================================
# 1. ENTERPRISE LOGGER SPECIFICATION
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("FINANCE_SERVICE")

# Platform Infrastructure Network Parameters
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


# =========================================================================
# 2. THE REPO GRAPH STATE MODEL
# =========================================================================
class FinanceState(TypedDict):
    """The data payload state bucket passed sequentially between nodes."""

    order_event: Dict[str, Any]
    status: str


# =========================================================================
# 3. THE GRAPH NODE ACTION FUNCTIONS
# =========================================================================
def check_fraud_rules(state: FinanceState) -> Dict[str, Any]:
    """Inspects the transaction amount payload to determine risk profiles."""
    event = state["order_event"]
    amount = float(event.get("amount", 0.0))

    logger.info(f"LangGraph Evaluating Fraud Rules | Amount: ${amount}")

    # PLAYGROUND RULE: Any checkout order over $200.00 triggers an instant fraud flag!
    if amount > 200.00:
        logger.warning(
            f"Fraud Threshold Boundary Exceeded | Amount: ${amount} | Routing to Rejection Lane."
        )
        return {"status": "TRIGGER_FRAUD_ALARM"}

    return {"status": "PASSED_RISK_CHECKS"}


def execute_approval(state: FinanceState) -> Dict[str, Any]:
    """Node representing successful risk passage. Atomically stores approval."""
    event = state["order_event"]
    logger.info(
        f"Saga Step Approved | Staging CREDIT_APPROVED on database outbox table. Order UUID: {event.get('order_id')}"
    )

    # Invokes your production db.py function to stage the success outbox record
    execute_financial_clearance_and_stage_outbox(event, status_msg="CREDIT_APPROVED")
    return {"status": "COMPLETED"}


def execute_rejection(state: FinanceState) -> Dict[str, Any]:
    """Node representing transaction failure. Atomically stores rejection."""
    event = state["order_event"]
    logger.warning(
        f"Saga Step Rejected | Staging PAYMENT_REJECTED on database outbox table. Order UUID: {event.get('order_id')}"
    )

    # Invokes your production db.py function to stage the failure outbox record (Saga anchor)
    execute_financial_clearance_and_stage_outbox(event, status_msg="PAYMENT_REJECTED")
    return {"status": "COMPLETED"}


# =========================================================================
# 4. THE CONDITIONAL ROUTING EDGE
# =========================================================================
def route_fraud_decision(
    state: FinanceState,
) -> Literal["execute_rejection", "execute_approval"]:
    """Inspects the current state status properties to select the next processing node."""
    if state["status"] == "TRIGGER_FRAUD_ALARM":
        return "execute_rejection"
    return "execute_approval"


# =========================================================================
# 5. ASSEMBLING THE WORKFLOW MATRIX
# =========================================================================
builder = StateGraph(FinanceState)

builder.add_node("check_fraud_rules", check_fraud_rules)
builder.add_node("execute_approval", execute_approval)
builder.add_node("execute_rejection", execute_rejection)

builder.add_edge(START, "check_fraud_rules")
builder.add_conditional_edges("check_fraud_rules", route_fraud_decision)
builder.add_edge("execute_approval", END)
builder.add_edge("execute_rejection", END)

finance_graph_engine = builder.compile()


# =========================================================================
# 6. HIGH-PERFORMANCE UTILITY HELPERS
# =========================================================================
def initialize_consumer_dependencies() -> AvroDeserializer:
    """Executes all system-wide data and network connections exactly once on boot."""
    # 1. Connect and verify relational schemas in PostgreSQL
    init_finance_db()

    # 2. Open connection channel to the central schema container
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # 3. Read the single root data contract contract file from disk
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "schemas", "command_envelope.avsc"
    )

    with open(schema_path, "r") as f:
        schema_str = f.read()

    # 4. Compile the deserializer engine object
    return AvroDeserializer(schema_registry_client, schema_str, lambda obj, ctx: obj)


def extract_command_payload(msg, deserializer) -> Dict[str, Any]:
    """Extracts and unpacks the internal order payload dictionary from the raw

    Kafka message bytes using the strictly typed CommandEnvelope contract.
    """
    context = SerializationContext(msg.topic(), MessageField.VALUE)
    command_envelope = deserializer(msg.value(), context)

    # FIXED: Extract the dictionary tree natively without executing string loads!
    order_payload = command_envelope.get("payload", {})

    if "order_id" not in order_payload:
        order_payload["order_id"] = command_envelope.get("order_id")

    return order_payload


# =========================================================================
# 7. MAIN RUNTIME EXECUTION LOOP
# =========================================================================
if __name__ == "__main__":
    # Boot dependency factory once out-of-band to secure connections
    avro_deserializer = initialize_consumer_dependencies()

    CONSUMER_CONFIG = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "enterprise_finance_processing_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe(["finance_commands"])

    pid = os.getpid()
    logger.info(
        f"Service Booted | Process ID: {pid} | Polling 'finance_commands' channel..."
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
                    logger.error(f"Kafka Broker Stream Fault: {msg.error()}")
                    break

            try:
                # High-speed payload translation pass-through line
                order_payload = extract_command_payload(msg, avro_deserializer)

                logger.info(
                    f"Command Ingested | Processing Order UUID: {order_payload.get('order_id')}"
                )

                # Execute LangGraph nodes and commit local state
                finance_graph_engine.invoke({"order_event": order_payload})

                consumer.commit(msg, asynchronous=False)

            except Exception as stream_err:
                logger.error(f"Data Pipeline Processing Exception: {str(stream_err)}")
                consumer.commit(msg, asynchronous=False)

    except KeyboardInterrupt:
        logger.info("Shutdown Signal Intercepted. Exiting safely.")
    finally:
        consumer.close()
