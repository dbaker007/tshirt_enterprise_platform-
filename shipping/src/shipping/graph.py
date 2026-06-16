import logging
from typing import Any, Dict, Literal

from langgraph.graph import END, START, StateGraph
from opentelemetry import trace
from typing_extensions import TypedDict

# Import your clean repository functions directly from your sibling database file
from shipping.db import stage_shipping_secured_event

logger = logging.getLogger("SHIPPING_SERVICE.GRAPH")
tracer = trace.get_tracer("shipping-fulfillment-service")


class ShippingState(TypedDict):
    """The data payload state bucket passed sequentially between nodes."""

    order_event: Dict[str, Any]
    action_type: str
    status: str


# =========================================================================
# GRAPH NODE ACTION FUNCTIONS (The Core Business Logic Hub)
# =========================================================================
def evaluate_geography_compliance(state: ShippingState) -> Dict[str, Any]:
    """BUSINESS LOGIC NODE: Inspects address records to determine regional legality."""
    with tracer.start_as_current_span(
        "langgraph_evaluate_geography_compliance"
    ) as span:
        action = state["action_type"]
        event = state["order_event"]

        span.set_attribute("order.correlation_id", event.get("order_id", "unknown"))
        span.set_attribute("saga.action_directive", action)

        if action == "CANCEL_TRANSACTION":
            return {"status": "TRIGGER_ROLLBACK", "order_event": event}

        address_info = event.get("shipping_address", {})
        target_state = address_info.get("state", "").upper().strip()
        span.set_attribute("destination.state", target_state)

        logger.info(
            f"LangGraph Evaluating Geography Compliance | Destination State: {target_state}"
        )

        # PLAYGROUND COMPLIANCE LAW: Michigan is flagged as a restricted zone!
        if target_state == "MI" or target_state == "MICHIGAN":
            logger.warning(
                f"Legal Restrictive Violation Intercepted | Order UUID: {event.get('order_id')} is blocked from Michigan shipping routes."
            )
            span.set_attribute("compliance.violation", True)
            return {"status": "TRIGGER_LEGAL_HOLD", "order_event": event}

        return {"status": "PASSED_COMPLIANCE", "order_event": event}


def execute_fulfillment(state: ShippingState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_fulfillment"):
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
        return {"status": "COMPLETED", "order_event": event}


def execute_legal_rejection(state: ShippingState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_legal_rejection") as span:
        event = state["order_event"]
        span.set_status(
            trace.Status(
                trace.StatusCode.ERROR,
                description="Michigan Legal Restriction Blocker Hit",
            )
        )
        logger.warning(
            f"Fulfillment Node Aborted | Staging Legal holds for Order: {event.get('order_id')}"
        )

        stage_shipping_secured_event(
            order_event=event,
            ledger_status="LEGAL_REJECTION_MI",
            status_msg="FAILED",
            reason_text="Fulfillment Aborted: Legal distribution constraint prohibits shirt logistics inside Michigan.",
        )
        return {"status": "COMPLETED", "order_event": event}


def execute_compensation_rollback(state: ShippingState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_compensation_rollback"):
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
        return {"status": "COMPLETED", "order_event": event}


# =========================================================================
# THE CONDITIONAL ROUTING EDGE
# =========================================================================
def route_fulfillment_decision(
    state: ShippingState,
) -> Literal[
    "execute_compensation_rollback", "execute_legal_rejection", "execute_fulfillment"
]:
    if state["status"] == "TRIGGER_ROLLBACK":
        return "execute_compensation_rollback"
    elif state["status"] == "TRIGGER_LEGAL_HOLD":
        return "execute_legal_rejection"
    return "execute_fulfillment"


# =========================================================================
# ASSEMBLING THE WORKFLOW MATRIX
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
