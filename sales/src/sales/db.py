import json
import logging
import uuid
from datetime import datetime

# 🏆 IMPORT THE UNIVERSAL W3C CONTEXT PROPAGATOR HOOK
from opentelemetry import propagate
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

logger = logging.getLogger("SALES_SERVICE.DATABASE")


class Base(DeclarativeBase):
    pass


DATABASE_URL = "postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    email = Column(String, unique=True, index=True)


class Invoice(Base):
    __tablename__ = "invoices"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    customer_id = Column(Integer)
    amount = Column(Float, index=True)


class Outbox(Base):
    """The transactional outbox table carrying the explicit W3C trace string context."""

    __tablename__ = "sales_outbox"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    key = Column(String, nullable=False)
    payload = Column(String, nullable=False)
    # 🛠️ THE EXACT W3C TELEMETRY CARRIER STORAGE FIELD
    trace_context = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class SagaState(Base):
    __tablename__ = "saga_states"
    order_id = Column(String, primary_key=True, index=True)
    status = Column(String, nullable=False)
    finance_status = Column(String, default="PENDING")
    shipping_status = Column(String, default="PENDING")
    notifications_status = Column(String, default="PENDING")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_sales_db():
    Base.metadata.create_all(bind=engine)


def persist_sale_and_stage_outbox(transaction: dict) -> tuple[str, int]:
    db = SessionLocal()
    generated_order_id = str(uuid.uuid4())

    try:
        # 🏆 EXPLICIT STATE CAPTURE PATTERN:
        # Pull the active W3C trace metadata out of the current execution thread,
        # serialize it to a clean string format, and store it in our outbox table.
        carrier = {}
        propagate.inject(carrier)
        w3c_traceparent_string = carrier.get("traceparent")

        customer_data = transaction.get("customer", {})
        new_customer = (
            db.query(Customer)
            .filter(Customer.email == customer_data.get("email"))
            .first()
        )
        if not new_customer:
            new_customer = Customer(
                name=customer_data.get("name", "Anonymous Buyer"),
                email=customer_data.get("email", "unknown@enterprise.io"),
            )
            db.add(new_customer)
            db.commit()
            db.refresh(new_customer)

        new_invoice = Invoice(
            order_id=generated_order_id,
            customer_id=new_customer.id,
            amount=transaction["amount"],
        )
        db.add(new_invoice)

        saga_tracking_log = SagaState(
            order_id=generated_order_id,
            status="STARTED",
            finance_status="PENDING",
            shipping_status="PENDING",
            notifications_status="PENDING",
        )
        db.add(saga_tracking_log)

        raw_address_info = transaction.get("shipping_address", {})
        avro_compatible_payload = {
            "customer_name": new_customer.name,
            "customer_email": new_customer.email,
            "amount": float(new_invoice.amount),
            "item_id": transaction.get("item_id", "SHIRT_STANDARD_BLUE"),
            "shipping_address": {
                "street": raw_address_info.get("street", "123 Default Way"),
                "city": raw_address_info.get("city", "Default Ville"),
                "state": raw_address_info.get("state", "OH"),
                "postal_code": raw_address_info.get("postal_code", "00000"),
            },
        }

        target_queues = [
            ("finance_commands", "finance_commands"),
            ("shipping_commands", "shipping_commands"),
            ("notifications_commands", "notifications_commands"),
        ]

        for queue_topic, queue_name in target_queues:
            envelope_data = {
                "command_id": str(uuid.uuid4()),
                "order_id": generated_order_id,
                "action": "NEW_SALE",
                "payload": avro_compatible_payload,
            }

            outbox_event = Outbox(
                topic=queue_topic,
                key=generated_order_id,
                payload=json.dumps(envelope_data),
                # Save the explicit W3C string context natively on the record
                trace_context=w3c_traceparent_string,
            )
            db.add(outbox_event)

        db.commit()
        db.refresh(new_invoice)

        logger.info(f"Saga Workflow Initialized | Order UUID: {generated_order_id}")
        return generated_order_id, new_invoice.id

    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()
