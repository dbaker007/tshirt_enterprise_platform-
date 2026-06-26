import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

# =========================================================================
# 🛠️ GLOBAL OPENTELEMETRY TRACING CORE INITIALIZATION
# =========================================================================
from observability.tracing import initialize_tracer
from opentelemetry import trace

# 🟢 IMPORT THE FASTAPI MIDDLEWARE INSTRUMENTOR
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from sales.order_entry.db import init_sales_db

# 🟢 STANDARDIZED IMPORTS: Pull your stateless database workers and connection factory
from .db import (
    SessionLocal,
    initialize_saga_state_tracking,
    persist_invoice_record,
    resolve_or_create_customer,
    stage_saga_command_envelopes,
)

# CRITICAL BEST PRACTICE: Must execute globally at the file root level to hook into memory properly
tracer = initialize_tracer()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SALES_GATEWAY")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing Sales Gateway API Container Node...")
    init_sales_db()
    logger.info("Sales Gateway Web Tier Core initialized successfully inside cluster.")
    yield
    logger.info("Shutting down Sales Gateway...")


app = FastAPI(lifespan=lifespan)

# 🟢 FIX: Nailed to the mast! Instrument the FastAPI app instance to preserve async tracing states! [1.1]
FastAPIInstrumentor.instrument_app(app)


@app.post("/sales/")
async def create_sale(transaction: dict):
    """Public gateway checkout checkpoint for handling consumer order payloads."""
    customer_info = transaction.get("customer", {})
    customer_email = customer_info.get("email", "unknown_user")

    logger.info(
        f"🚀 [PIPELINE VERIFIED]: Ingesting Checkout Request | Customer Email: {customer_email} | Amount: ${transaction.get('amount')}"
    )

    # 🟢 START THE MASTER PARENT TRACING CONTEXT WINDOW
    with tracer.start_as_current_span("http_create_sale_request") as span:
        # Enforce strict float casting wall. Fail fast on bad data formats!
        try:
            order_amount = float(transaction.get("amount", 0.0))
        except (ValueError, TypeError):
            logger.warning(
                f"❌ HTTP 400 Rejection: Malformed amount parameter payload: {transaction.get('amount')}"
            )
            raise HTTPException(
                status_code=400,
                detail="Invalid payload amount format. Parameter must be a clean numeric float/integer value.",
            )

        # Attach high-utility attributes to make your Jaeger UI console searchable
        span.set_attribute("http.method", "POST")
        span.set_attribute("customer.email", customer_email)
        span.set_attribute("order.amount", order_amount)
        span.set_attribute("item.id", transaction.get("item_id", "SHIRT_STANDARD_BLUE"))

        # 🟢 TRANSACTION UNIT OF WORK: Explicit lifecycle block inside the entrypoint route!
        db = SessionLocal()
        generated_order_id = str(uuid.uuid4())

        try:
            # 1. Resolve customer profile and flush primary key
            customer_record = resolve_or_create_customer(db, customer_info)

            # 2. Record the local invoice document
            invoice_record = persist_invoice_record(
                db=db,
                order_id=generated_order_id,
                customer_id=customer_record.id,
                amount=order_amount,
            )

            # 3. Instantiate the tracking checklist row inside the orchestration table
            initialize_saga_state_tracking(db, generated_order_id)

            # 4. Construct the payload dictionary envelope contract
            raw_address_info = transaction.get("shipping_address", {})
            avro_compatible_payload = {
                "customer_name": customer_record.customer_name,
                "customer_email": customer_record.email,
                "amount": float(invoice_record.amount),
                "item_id": transaction.get("item_id", "SHIRT_STANDARD_BLUE"),
                "shipping_address": {
                    "street": raw_address_info.get("street", "123 Default Way"),
                    "city": raw_address_info.get("city", "Default Ville"),
                    "state": raw_address_info.get("state", "OH"),
                    "postal_code": raw_address_info.get("postal_code", "00000"),
                },
            }

            # 5. Stage individual downstream commands to the universal outbox table
            stage_saga_command_envelopes(
                db, generated_order_id, avro_compatible_payload
            )

            # Commit everything safely in a single atomic pass
            db.commit()

            # Map tracking indexes directly to the visual trace metadata tree
            span.set_attribute("order.correlation_id", generated_order_id)
            span.set_attribute("order.invoice_id", invoice_record.id)

            logger.info(
                f"API Transaction Handled Successfully | Dispatched to Outbox | Order UUID: {generated_order_id}"
            )
            return {
                "status": "PROCESSED",
                "order_id": generated_order_id,
                "invoice_id": invoice_record.id,
            }

        except Exception as e:
            db.rollback()
            # Automatically flag failures inside the graphical Jaeger timeline tree
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, description=str(e)))
            logger.error(f"HTTP Gateway Processing Exception Encountered: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Transaction processing failure: {str(e)}"
            )
        finally:
            db.close()
