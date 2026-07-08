# shipping/src/shipping/graph.py

import logging
from typing import Any, Dict, Literal

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from opentelemetry import trace
from typing_extensions import TypedDict

from shipping.constants import (
    FREIGHT_ROUTE_RELEASED,
    LEGAL_REJECTION_MI,
    ROLLED_BACK,
    SHIPMENT_SECURED,
    SUCCESS,
)
from shipping.db import (
    persist_shipping_ledger_record,
    stage_shipping_saga_reply,
)

logger = logging.getLogger("SHIPPING_SERVICE.GRAPH")
tracer = trace.get_tracer("shipping-fulfillment-service")


class ShippingState(TypedDict):
    """The data payload state bucket passed sequentially between nodes."""

    order_event: Dict[str, Any]
    action_type: str
    status: str


# =========================================================================
# THE RESTRUCTURED GRAPH MATRIX ENTRYWAY ROUTER
# =========================================================================
def route_initial_ingress_directive(
    state: ShippingState,
) -> Literal["evaluate_geography_compliance", "execute_compensation_rollback"]:
    """GATEWAY ROUTER: Inspects the raw control action before any business nodes execute."""
    action = state.get("action_type") or state.get("action") or "NEW_SALE"

    if action == "CANCEL_TRANSACTION":
        return "execute_compensation_rollback"
    return "evaluate_geography_compliance"


# =========================================================================
# GRAPH NODE ACTION FUNCTIONS (The Core Business Logic Hub)
# =========================================================================
def evaluate_geography_compliance(state: ShippingState) -> Dict[str, Any]:
    """BUSINESS LOGIC NODE: Inspects address records strictly for forward compliance."""
    with tracer.start_as_current_span(
        "langgraph_evaluate_geography_compliance"
    ) as span:
        event = state["order_event"]

        address_info = event.get("shipping_address", {})
        target_state = address_info.get("state", "").upper().strip()
        span.set_attribute("destination.state", target_state)

        logger.info(
            f"LangGraph Evaluating Geography Compliance | Destination State: {target_state}"
        )

        if target_state in ["MI", "MICHIGAN"]:
            logger.warning(
                f"Legal Restrictive Violation Intercepted | Order UUID: {event.get('order_id')} is blocked from Michigan shipping routes."
            )
            span.set_attribute("compliance.violation", True)
            return {"status": "TRIGGER_LEGAL_HOLD", "order_event": event}

        return {"status": "PASSED_COMPLIANCE", "order_event": event}


def execute_fulfillment(state: ShippingState, config: RunnableConfig) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_fulfillment"):
        event = state["order_event"]
        order_id = event.get("order_id", "unknown-uuid")
        logger.info(
            f"Fulfillment Node Approved | Securing freight routes for Order: {order_id}"
        )

        # 🟢 EXTRACT ACTIVE DATABASE SESSION NATIVELY FROM RUNTIME SCOPE
        db = config.get("configurable", {}).get("db")
        if not db:
            raise RuntimeError(
                "Execution Boundary Violation: No active database session mapped in configuration context."
            )

        persist_shipping_ledger_record(db, order_id, ledger_status=SHIPMENT_SECURED)
        stage_shipping_saga_reply(
            db=db, order_id=order_id, wire_status=SUCCESS, ledger_status=SUCCESS
        )

        return {"status": "COMPLETED", "order_event": event}


def execute_legal_rejection(
    state: ShippingState, config: RunnableConfig
) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_legal_rejection") as span:
        event = state["order_event"]
        order_id = event.get("order_id", "unknown-uuid")
        span.set_status(
            trace.Status(
                trace.StatusCode.ERROR,
                description="Michigan Legal Restriction Blocker Hit",
            )
        )
        logger.warning(
            f"Fulfillment Node Aborted | Staging Legal holds for Order: {order_id}"
        )

        # 🟢 EXTRACT ACTIVE DATABASE SESSION NATIVELY FROM RUNTIME SCOPE
        db = config.get("configurable", {}).get("db")
        if not db:
            raise RuntimeError(
                "Execution Boundary Violation: No active database session mapped in configuration context."
            )

        persist_shipping_ledger_record(db, order_id, ledger_status=LEGAL_REJECTION_MI)
        stage_shipping_saga_reply(
            db=db,
            order_id=order_id,
            wire_status="FAILED",
            ledger_status=LEGAL_REJECTION_MI,
        )

        return {"status": "COMPLETED", "order_event": event}


def execute_compensation_rollback(
    state: ShippingState, config: RunnableConfig
) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_compensation_rollback"):
        event = state["order_event"]
        order_id = event.get("order_id", "unknown-uuid")
        logger.info(
            f"Compensation Node Fired | Releasing freight routes for Order: {order_id}"
        )

        # 🟢 EXTRACT ACTIVE DATABASE SESSION NATIVELY FROM RUNTIME SCOPE
        db = config.get("configurable", {}).get("db")
        if not db:
            raise RuntimeError(
                "Execution Boundary Violation: No active database session mapped in configuration context."
            )

        persist_shipping_ledger_record(
            db, order_id, ledger_status=FREIGHT_ROUTE_RELEASED
        )
        stage_shipping_saga_reply(
            db=db,
            order_id=order_id,
            wire_status="SUCCESS",
            ledger_status=ROLLED_BACK,
        )

        return {"status": "COMPLETED", "order_event": event}


# =========================================================================
# THE CONDITIONAL ROUTING EDGE
# =========================================================================
def route_fulfillment_decision(
    state: ShippingState,
) -> Literal["execute_legal_rejection", "execute_fulfillment"]:
    if state["status"] == "TRIGGER_LEGAL_HOLD":
        return "execute_legal_rejection"
    return "execute_fulfillment"


# =========================================================================
# ASSEMBLING THE WORKFLOW MATRIX (Symmetrical LangGraph)
# =========================================================================
builder = StateGraph(ShippingState)

builder.add_node("evaluate_geography_compliance", evaluate_geography_compliance)
builder.add_node("execute_fulfillment", execute_fulfillment)
builder.add_node("execute_legal_rejection", execute_legal_rejection)
builder.add_node("execute_compensation_rollback", execute_compensation_rollback)

builder.add_conditional_edges(START, route_initial_ingress_directive)
builder.add_conditional_edges(
    "evaluate_geography_compliance", route_fulfillment_decision
)

builder.add_edge("execute_fulfillment", END)
builder.add_edge("execute_legal_rejection", END)
builder.add_edge("execute_compensation_rollback", END)

# 🟢 SOLUTION: Graph engine compiled natively as-is without database intercept constraints
shipping_graph_engine = builder.compile()
