import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

# =========================================================================
# 🛠️ GLOBAL OPENTELEMETRY TRACING CORE INITIALIZATION
# =========================================================================
from observability.tracing import initialize_tracer
from opentelemetry import trace

# Import your active relational data access layers and schema initializers
from sales.db import init_sales_db, persist_sale_and_stage_outbox

# CRITICAL BEST PRACTICE: Must execute globally at the file root level to hook into memory properly
tracer = initialize_tracer("sales-gateway-api")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SALES_GATEWAY")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing Sales Gateway Database tables...")
    init_sales_db()
    logger.info("Sales Gateway Tables Synchronized Flawlessly in PostgreSQL Container.")
    yield
    logger.info("Shutting down Sales Gateway...")


app = FastAPI(lifespan=lifespan)


@app.post("/sales/")
async def create_sale(transaction: dict):
    """Public gateway checkout checkpoint for handling consumer order payloads."""
    customer_info = transaction.get("customer", {})
    customer_email = customer_info.get("email", "unknown_user")

    logger.info(
        f"Checkout API Request Ingested | Customer Email: {customer_email} | Amount: ${transaction.get('amount')}"
    )

    # 🟢 START THE MASTER PARENT TRACING CONTEXT WINDOW
    with tracer.start_as_current_span("http_create_sale_request") as span:
        # Attach high-utility attributes to make your Jaeger UI console searchable
        span.set_attribute("http.method", "POST")
        span.set_attribute("customer.email", customer_email)
        span.set_attribute("order.amount", float(transaction.get("amount", 0.0)))
        span.set_attribute("item.id", transaction.get("item_id", "SHIRT_STANDARD_BLUE"))

        try:
            # Commit payload parameters inside our atomic transactional outbox layer
            order_id, invoice_id = persist_sale_and_stage_outbox(transaction)

            # Map tracking indexes directly to the visual trace metadata tree
            span.set_attribute("order.correlation_id", order_id)
            span.set_attribute("order.invoice_id", invoice_id)

            logger.info(
                f"API Transaction Handled Successfully | Dispatched to Outbox | Order UUID: {order_id}"
            )
            return {
                "status": "PROCESSED",
                "order_id": order_id,
                "invoice_id": invoice_id,
            }

        except Exception as e:
            # Automatically flag failures inside the graphical Jaeger timeline tree
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, description=str(e)))
            logger.error(f"HTTP Gateway Processing Exception Encountered: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Transaction processing failure: {str(e)}"
            )
