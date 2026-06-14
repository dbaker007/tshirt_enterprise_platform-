import logging
import sys

from fastapi import FastAPI, HTTPException

from sales.db import init_sales_db, persist_sale_and_stage_outbox

# =========================================================================
# 1. ENTERPRISE LOGGER SPECIFICATION
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SALES_GATEWAY")

app = FastAPI()


# Synchronize your local schemas with your background PostgreSQL container on boot
async def lifespan(app: FastAPI):
    """Manages the startup and shutdown lifecycle event loops for the application."""
    logger.info("Initializing Sales Gateway Database tables...")
    init_sales_db()
    logger.info("Sales Gateway Tables Synchronized Flawlessly in PostgreSQL Container.")
    yield
    logger.info("Shutting down Sales Gateway...")


# 4. Pass the lifespan handler straight into your FastAPI app constructor instance:
app = FastAPI(lifespan=lifespan)


@app.post("/sales/")
async def create_sale(transaction: dict):
    """Public gateway checkpoint endpoint for handling customer checkout payloads."""
    customer_info = transaction.get("customer", {})
    customer_email = customer_info.get("email", "unknown_user")

    logger.info(
        f"Checkout API Request Ingested | Customer Email: {customer_email} | Amount: ${transaction.get('amount')}"
    )

    try:
        # Invoke your encapsulated database data-access function out-of-band
        # This registers the sale and packages the 3 concurrent microservice commands atomically
        order_id, invoice_id = persist_sale_and_stage_outbox(transaction)

        logger.info(
            f"API Transaction Handled Successfully | Dispatched to Outbox | Order UUID: {order_id}"
        )
        return {"status": "PROCESSED", "order_id": order_id, "invoice_id": invoice_id}

    except Exception as e:
        logger.error(f"HTTP Gateway Gateway Processing Exception Encountered: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Transaction processing failure: {str(e)}"
        )
