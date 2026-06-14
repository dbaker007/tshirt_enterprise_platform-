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

# Import your clean repository functions directly from your sibling database file
from shipping.db import init_shipping_db, stage_shipping_secured_event

# =========================================================================
# 1. ENTERPRISE LOGGER SPECIFICATION
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SHIPPING_SERVICE")

# Platform Infrastructure Network Parameters
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


# =========================================================================
# 2. THE REPO GRAPH STATE MODEL
# =========================================================================
class ShippingState(TypedDict):
    """The data payload state bucket passed sequentially between nodes."""

    order_event: Dict[str, Any]
    action_type: str
    status: str


# =========================================================================
# 3. THE GRAPH NODE ACTION FUNCTIONS (The Core Business Logic Hub)
# =========================================================================
# ❌ LOCATE AND STRIP OUT THIS DEFECTIVE SHORT-CIRCUIT ROUTE:
# if action == "CANCEL_TRANSACTION":
#     return {"status": "TRIGGER_ROLLBACK"}


# 🛠️ REPLACE IT WITH THIS STATE-SAFE RESOLUTION:
def evaluate_geography_compliance(state: ShippingState) -> Dict[str, Any]:
    """BUSINESS LOGIC NODE: Inspects address records to determine regional legality."""
    action = state["action_type"]
    event = state["order_event"]

    if action == "CANCEL_TRANSACTION":
        # FIXED: Passes the order_event payload forward so subsequent nodes do not hit KeyErrors!
        return {"status": "TRIGGER_ROLLBACK", "order_event": event}

    address_info = event.get("shipping_address", {})
    target_state = address_info.get("state", "").upper().strip()

    logger.info(
        f"LangGraph Evaluating Geography Compliance | Destination State: {target_state}"
    )

    if target_state == "MI" or target_state == "MICHIGAN":
        logger.warning(
            f"Legal Restrictive Violation Intercepted | Order UUID: {event.get('order_id')} is blocked from Michigan shipping routes."
        )
        return {"status": "TRIGGER_LEGAL_HOLD", "order_event": event}

    return {"status": "PASSED_COMPLIANCE", "order_event": event}


def execute_fulfillment(state: ShippingState) -> Dict[str, Any]:
    """Node representing successful rule validation. Atomically secures freight routes."""
    event = state["order_event"]
    logger.info(
        f"Fulfillment Node Approved | Securing freight routes for Order: {event.get('order_id')}"
    )

    stage_shipping_secured_event(
        order_event=event,
        ledger_status="SHIPMENT_SECURED",
        status_msg="SUCCESS",
        reason_text="Fulfillment cleared: Shipping route successfully locked on carrier schedule.",
    )
    return {"status": "COMPLETED"}


def execute_legal_rejection(state: ShippingState) -> Dict[str, Any]:
    """Node representing compliance blocker path. Atomically triggers a Saga rollback."""
    event = state["order_event"]
    logger.warning(
        f"Fulfillment Node Aborted | Staging Legal holds for Order: {event.get('order_id')}"
    )

    stage_shipping_secured_event(
        order_event=event,
        ledger_status="LEGAL_REJECTION_MI",
        status_msg="FAILED",
        reason_text="Fulfillment Aborted: Legal distribution constraint prohibits shirt logistics inside Michigan.",
    )
    return {"status": "COMPLETED"}


def execute_compensation_rollback(state: ShippingState) -> Dict[str, Any]:
    """Node representing compensation rollback instruction received from conductor."""
    event = state["order_event"]
    logger.info(
        f"Compensation Node Fired | Releasing freight routes for Order: {event.get('order_id')}"
    )

    stage_shipping_secured_event(
        order_event=event,
        ledger_status="FREIGHT_ROUTE_RELEASED",
        status_msg="SUCCESS",
        reason_text="Compensation rollback completed: Logistics inventory returned to open queue pools.",
    )
    return {"status": "COMPLETED"}


# =========================================================================
# 4. THE CONDITIONAL ROUTING EDGE
# =========================================================================
def route_fulfillment_decision(
    state: ShippingState,
) -> Literal[
    "execute_compensation_rollback", "execute_legal_rejection", "execute_fulfillment"
]:
    """Inspects the current graph state flags to navigate the workflow topology paths."""
    if state["status"] == "TRIGGER_ROLLBACK":
        return "execute_compensation_rollback"
    elif state["status"] == "TRIGGER_LEGAL_HOLD":
        return "execute_legal_rejection"
    return "execute_fulfillment"


# =========================================================================
# 5. ASSEMBLING THE WORKFLOW MATRIX
# =========================================================================
builder = StateGraph(ShippingState)

builder.add_node("evaluate_geography_compliance", evaluate_geography_compliance)
builder.add_node("execute_fulfillment", execute_fulfillment)
builder.add_node("execute_legal_rejection", execute_legal_rejection)
builder.add_node("execute_compensation_rollback", execute_compensation_rollback)

builder.add_edge(START, "evaluate_geography_compliance")
builder.add_conditional_edges(
    "evaluate_geography_compliance", route_fulfillment_decision
)
builder.add_edge("execute_fulfillment", END)
builder.add_edge("execute_legal_rejection", END)
builder.add_edge("execute_compensation_rollback", END)

shipping_graph_engine = builder.compile()


# =========================================================================
# 6. HIGH-PERFORMANCE UTILITY HELPERS
# =========================================================================
def initialize_consumer_dependencies() -> AvroDeserializer:
    init_shipping_db()
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "schemas", "command_envelope.avsc"
    )
    with open(schema_path, "r") as f:
        schema_str = f.read()
    return AvroDeserializer(schema_registry_client, schema_str, lambda obj, ctx: obj)


def extract_command_payload(msg, deserializer) -> tuple[Dict[str, Any], str]:
    context = SerializationContext(msg.topic(), MessageField.VALUE)
    command_envelope = deserializer(msg.value(), context)
    action = command_envelope.get("action")
    order_id = command_envelope.get("order_id")
    order_payload = command_envelope.get("payload", {})

    if "order_id" not in order_payload:
        order_payload["order_id"] = order_id

    return order_payload, action


# =========================================================================
# 7. MAIN RUNTIME EXECUTION LOOP
# =========================================================================
if __name__ == "__main__":
    avro_deserializer = initialize_consumer_dependencies()

    CONSUMER_CONFIG = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "enterprise_shipping_processing_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe(["shipping_commands"])

    pid = os.getpid()
    logger.info(
        f"Service Booted | Process ID: {pid} | Polling 'shipping_commands' channel..."
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
                order_payload, action = extract_command_payload(msg, avro_deserializer)

                # Fire your compiled LangGraph engine state machine loop!
                shipping_graph_engine.invoke(
                    {"order_event": order_payload, "action_type": action}
                )

                consumer.commit(msg, asynchronous=False)

            except Exception as stream_err:
                logger.error(f"Data Pipeline Processing Exception: {str(stream_err)}")
                consumer.commit(msg, asynchronous=False)

    except KeyboardInterrupt:
        logger.info(f"Shutdown Signal Intercepted | Process ID {pid} exiting safely.")
    finally:
        consumer.close()
