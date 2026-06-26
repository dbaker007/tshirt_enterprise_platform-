import logging
from typing import Any, Dict, Literal

from langgraph.graph import END, START, StateGraph
from opentelemetry import trace
from typing_extensions import TypedDict

# 🟢 STANDARDIZED IMPORTS: Pull your stateless database workers and connection factory
from shipping.db import (
    SessionLocal,
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
    # Permissively check for action or action_type from the framework envelope mapping
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

        # PLAYGROUND COMPLIANCE LAW: Michigan is flagged as a restricted zone!
        if target_state in ["MI", "MICHIGAN"]:
            logger.warning(
                f"Legal Restrictive Violation Intercepted | Order UUID: {event.get('order_id')} is blocked from Michigan shipping routes."
            )
            span.set_attribute("compliance.violation", True)
            return {"status": "TRIGGER_LEGAL_HOLD", "order_event": event}

        return {"status": "PASSED_COMPLIANCE", "order_event": event}


def execute_fulfillment(state: ShippingState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_fulfillment"):
        event = state["order_event"]
        order_id = event.get("order_id", "unknown-uuid")
        logger.info(
            f"Fulfillment Node Approved | Securing freight routes for Order: {order_id}"
        )

        # 🟢 TRANSACTION UNIT OF WORK: Explicit lifecycle block inside the execution node!
        db = SessionLocal()
        try:
            persist_shipping_ledger_record(
                db, order_id, ledger_status="SHIPMENT_SECURED"
            )
            stage_shipping_saga_reply(
                db=db,
                order_id=order_id,
                wire_status="SUCCESS",
                ledger_status="SHIPMENT_SECURED",
                reason_text="Fulfillment cleared: Shipping route successfully locked on carrier schedule.",
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(
                f"❌ Failed to commit shipping fulfillment transaction: {str(e)}"
            )
            raise e
        finally:
            db.close()

        return {"status": "COMPLETED", "order_event": event}


def execute_legal_rejection(state: ShippingState) -> Dict[str, Any]:
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

        # 🟢 TRANSACTION UNIT OF WORK: Symmetrical fail-safe logic mapping
        db = SessionLocal()
        try:
            persist_shipping_ledger_record(
                db, order_id, ledger_status="LEGAL_REJECTION_MI"
            )
            stage_shipping_saga_reply(
                db=db,
                order_id=order_id,
                wire_status="FAILED",
                ledger_status="LEGAL_REJECTION_MI",
                reason_text="Fulfillment Aborted: Legal distribution constraint prohibits shirt logistics inside Michigan.",
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(
                f"❌ Failed to commit shipping legal rejection transaction: {str(e)}"
            )
            raise e
        finally:
            db.close()

        return {"status": "COMPLETED", "order_event": event}


def execute_compensation_rollback(state: ShippingState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_compensation_rollback"):
        event = state["order_event"]
        order_id = event.get("order_id", "unknown-uuid")
        logger.info(
            f"Compensation Node Fired | Releasing freight routes for Order: {order_id}"
        )

        # 🟢 TRANSACTION UNIT OF WORK: Symmetrical compensation signature mapping
        db = SessionLocal()
        try:
            persist_shipping_ledger_record(
                db, order_id, ledger_status="FREIGHT_ROUTE_RELEASED"
            )
            stage_shipping_saga_reply(
                db=db,
                order_id=order_id,
                wire_status="ROLLED_BACK",
                ledger_status="FREIGHT_ROUTE_RELEASED",
                reason_text="Compensation rollback completed: Logistics inventory returned to open queue pools.",
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(
                f"❌ Failed to commit shipping compensation transaction: {str(e)}"
            )
            raise e
        finally:
            db.close()

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

# 🟢 FIX: Bind the entryway conditional router straight to the START node!
builder.add_conditional_edges(START, route_initial_ingress_directive)

# Map remaining forward path steps linearly
builder.add_conditional_edges(
    "evaluate_geography_compliance", route_fulfillment_decision
)

builder.add_edge("execute_fulfillment", END)
builder.add_edge("execute_legal_rejection", END)
builder.add_edge("execute_compensation_rollback", END)

shipping_graph_engine = builder.compile()
