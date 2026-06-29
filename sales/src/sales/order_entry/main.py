import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from observability.db import get_platform_database_url
from observability.outbox import stage_outbox_message
from observability.tracing import initialize_tracer
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from sales.order_entry.db import (
    init_sales_db,
    initialize_saga_state_tracking,
    persist_invoice_record,
    resolve_or_create_customer,
    stage_saga_command_envelopes,
)
from sales.shared_models import SagaState
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# CRITICAL BEST PRACTICE: Execute tracer hooking globally at the file root level
tracer = initialize_tracer()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SALES_GATEWAY")

# =========================================================================
# 📡 CENTRALIZED INFRASTRUCTURE DRIVER INITIALIZATION
# =========================================================================
LOCAL_PORT = os.environ.get("SALES_GATEWAY_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing Sales Gateway API Container Node...")
    # 🟢 SOLUTION: Pass your engine dependency cleanly to the refactored schema initializer!
    init_sales_db(engine)
    logger.info("Sales Gateway Web Tier Core initialized successfully inside cluster.")
    yield
    logger.info("Shutting down Sales Gateway...")


app = FastAPI(lifespan=lifespan)

# Instrument the FastAPI app instance to preserve async tracing states!
FastAPIInstrumentor.instrument_app(app)


# =========================================================================
# FORWARD CHECKOUT ROADWAY ROUTE
# =========================================================================
@app.post("/sales/")
@app.post("/sales/")
async def create_sale(transaction: dict):
    """Public gateway checkout checkpoint for handling consumer order payloads."""
    customer_info = transaction.get("customer", {})
    customer_email = customer_info.get("email", "unknown_user")

    logger.info(
        f"🚀 [PIPELINE VERIFIED]: Ingesting Checkout Request | Customer Email: {customer_email} | Amount: ${transaction.get('amount')}"
    )

    with tracer.start_as_current_span("http_create_sale_request") as span:
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

        span.set_attribute("http.method", "POST")
        span.set_attribute("customer.email", customer_email)
        span.set_attribute("order.amount", order_amount)
        span.set_attribute("item.id", transaction.get("item_id", "SHIRT_STANDARD_BLUE"))

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

            # 3. Construct the payload dictionary envelope contract first
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

            # 4. 🟢 SOLUTION: Pass the complete payload parameter dictionary straight into your worker!
            initialize_saga_state_tracking(
                db, generated_order_id, avro_compatible_payload
            )

            # 5. Stage individual downstream commands to the universal outbox table
            stage_saga_command_envelopes(
                db, generated_order_id, avro_compatible_payload
            )

            # Commit everything safely in a single atomic pass
            db.commit()

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
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, description=str(e)))
            logger.error(f"HTTP Gateway Processing Exception Encountered: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Transaction processing failure: {str(e)}"
            )
        finally:
            db.close()


# =========================================================================
# 🟢 EXTENSION: THE MISSING MANUAL RISK OVERRIDE ROUTE INTERFACE
# =========================================================================


@app.post("/sales/override")
async def override_sale(verdict_payload: dict):
    """Public gateway risk override checkpoint for manual human operator review holds."""
    order_id = verdict_payload.get("order_id")
    verdict = str(verdict_payload.get("verdict", "")).upper()

    if not order_id or verdict not in ["APPROVE", "REJECT"]:
        logger.warning(
            f"❌ HTTP 400 Rejection: Invalid override payload options received: {verdict_payload}"
        )
        raise HTTPException(
            status_code=400,
            detail="Malformed override parameters. Required arguments: 'order_id' (UUID string) and 'verdict' ('APPROVE' or 'REJECT').",
        )

    logger.info(
        f"🧑‍✈️ [MANUAL OVERRIDE ROUTER]: Processing operator verdict [{verdict}] for order [{order_id}]"
    )

    with tracer.start_as_current_span("http_override_sale_request") as span:
        span.set_attribute("http.method", "POST")
        span.set_attribute("order.correlation_id", str(order_id))
        span.set_attribute("operator.verdict", verdict)

        db = SessionLocal()
        try:
            # 🟢 SOLUTION: Rehydrate the original record fields from your database to pass Avro validation! [1.1]
            state_record = (
                db.query(SagaState).filter(SagaState.order_id == str(order_id)).first()
            )
            if not state_record:
                raise HTTPException(
                    status_code=44,
                    detail=f"Transaction context matching UUID {order_id} not found.",
                )

            customer_name = getattr(state_record, "customer_name", "Unknown Buyer")
            customer_email = getattr(
                state_record, "customer_email", "unknown@platform.internal"
            )
            order_amount = float(getattr(state_record, "amount", 0.0))
            item_id = getattr(state_record, "item_id", "SHIRT_STANDARD_BLUE")

            shipping_state = getattr(state_record, "shipping_state", "OH")
            street_address = getattr(
                state_record, "shipping_street", "123 Transaction Way"
            )
            city_name = getattr(state_record, "shipping_city", "Default Ville")
            postal_code = getattr(state_record, "shipping_postal", "00000")

            # Construct a fully compliant OrderPayloadRecord layout containing your custom verdict variable [1.1]
            avro_payload = {
                "customer_name": str(customer_name),
                "customer_email": str(customer_email),
                "amount": order_amount,
                "item_id": str(item_id),
                "shipping_address": {
                    "street": str(street_address),
                    "city": str(city_name),
                    "state": str(shipping_state),
                    "postal_code": str(postal_code),
                },
            }

            # 🟢 SOLUTION: Keep the authentic control string token completely un-faked! [1.1]
            envelope = {
                "command_id": str(uuid.uuid4()),
                "order_id": str(order_id),
                "action": "RESUME_REVIEW",  # Authentic control signal string passed through cleanly [1.1]
                "verdict": verdict,  # Intercepted inside your finance consumer layer natively
                "payload": avro_payload,
            }

            stage_outbox_message(
                db=db,
                topic="finance_commands",
                partition_key=str(order_id),
                payload=envelope,
            )

            db.commit()
            logger.info(
                f"✔ Manual override verdict successfully staged to platform outbox log for order [{order_id}]"
            )
            return {
                "status": "OVERRIDE_STAGED",
                "order_id": order_id,
                "verdict": verdict,
            }

        except HTTPException:
            raise
        except Exception as e:
            db.rollback()
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, description=str(e)))
            logger.error(f"HTTP Override Gateway Processing Exception: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Override transaction processing failure: {str(e)}",
            )
        finally:
            db.close()
