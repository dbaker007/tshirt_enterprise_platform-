import logging
from typing import Any, Dict, Literal

from langgraph.graph import END, START, StateGraph
from opentelemetry import trace
from typing_extensions import TypedDict

# Import your clean repository functions directly from your sibling database file
from finance.db import execute_financial_clearance_and_stage_outbox

logger = logging.getLogger("FINANCE_SERVICE.GRAPH")
tracer = trace.get_tracer("finance-auditing-service")


class FinanceState(TypedDict):
    """The data payload state bucket passed sequentially between nodes."""

    order_event: Dict[str, Any]
    status: str


# =========================================================================
# GRAPH NODE ACTION FUNCTIONS (The Core Business Logic Hub)
# =========================================================================
def evaluate_financial_fraud_risk(state: FinanceState) -> Dict[str, Any]:
    """BUSINESS LOGIC NODE: Evaluates order size threshold margins to track fraud risk."""
    with tracer.start_as_current_span("langgraph_evaluate_fraud_risk") as span:
        event = state["order_event"]
        order_amount = float(event.get("amount", 0.0))

        span.set_attribute("order.correlation_id", event.get("order_id", "unknown"))
        span.set_attribute("evaluation.amount", order_amount)

        logger.info(
            f"LangGraph Evaluating Risk Node | Transaction Amount: ${order_amount}"
        )

        if order_amount > 200.0:
            logger.warning(
                f"Risk Threshold Exceeded! Flagging Potential Fraud for Order UUID: {event.get('order_id')}"
            )
            span.set_attribute("fraud.flagged", True)
            return {"status": "TRIGGER_FRAUD_REJECTION", "order_event": event}

        return {"status": "PASSED_RISK_CHECKS", "order_event": event}


def execute_approval(state: FinanceState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_finance_approval"):
        event = state["order_event"]
        logger.info(
            f"Financial Clearance Engine Approved | Order UUID: {event.get('order_id')}"
        )

        execute_financial_clearance_and_stage_outbox(
            order_event=event, status_msg="CREDIT_APPROVED"
        )
        return {"status": "COMPLETED", "order_event": event}


def execute_fraud_rejection(state: FinanceState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_fraud_rejection") as span:
        event = state["order_event"]
        span.set_status(
            trace.Status(trace.StatusCode.ERROR, description="Risk Threshold Exceeded")
        )
        logger.warning(
            f"Financial Clearance Engine Aborted | Order UUID: {event.get('order_id')}"
        )

        execute_financial_clearance_and_stage_outbox(
            order_event=event, status_msg="PAYMENT_REJECTED"
        )
        return {"status": "COMPLETED", "order_event": event}


# =========================================================================
# THE CONDITIONAL ROUTING EDGE
# =========================================================================
def route_risk_decision(
    state: FinanceState,
) -> Literal["execute_fraud_rejection", "execute_approval"]:
    if state["status"] == "TRIGGER_FRAUD_REJECTION":
        return "execute_fraud_rejection"
    return "execute_approval"


# =========================================================================
# ASSEMBLING THE WORKFLOW MATRIX
# =========================================================================
builder = StateGraph(FinanceState)

builder.add_node("evaluate_financial_fraud_risk", evaluate_financial_fraud_risk)
builder.add_node("execute_approval", execute_approval)
builder.add_node("execute_fraud_rejection", execute_fraud_rejection)

builder.add_edge(START, "evaluate_financial_fraud_risk")
builder.add_conditional_edges("evaluate_financial_fraud_risk", route_risk_decision)
builder.add_edge("execute_approval", END)
builder.add_edge("execute_fraud_rejection", END)

finance_graph_engine = builder.compile()
