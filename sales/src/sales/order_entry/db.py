# sales/src/sales/order_entry/db.py

import logging
import os
import uuid
from datetime import datetime

from observability.outbox import stage_outbox_message

# 🟢 SOURCE OF TRUTH BASE MODELS MAPPING
from sales.shared_models import SagaState, SharedBase
from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Session

logger = logging.getLogger("SALES_SERVICE.DATABASE")


class Base(DeclarativeBase):
    pass


# =========================================================================
# 🗄️ RELEVANT DOMAIN LOGISTICS SCHEMAS
# =========================================================================
class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    customer_name = Column(String, index=True)
    email = Column(String)


class Invoice(Base):
    __tablename__ = "invoices"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    customer_id = Column(Integer)
    amount = Column(Float, index=True)


# =========================================================================
# 🟢 HYBRID SHARD SCHEMAS INITIALIZATION
# =========================================================================
def init_sales_db(engine) -> None:
    """Binds and maps the core sales order schemas directly onto the sales_domain logical shard."""
    # 🟢 SOLUTION: Wall off core sales business tables inside the isolated schema namespace [1.1]
    TARGET_SCHEMA = "sales_domain"
    Base.metadata.schema = TARGET_SCHEMA
    SharedBase.metadata.schema = TARGET_SCHEMA

    Base.metadata.create_all(bind=engine)
    SharedBase.metadata.create_all(bind=engine)


# =========================================================================
# 🟢 STATELESS DATA ACCESS WORKERS (Shared Unit-of-Work Targets)
# =========================================================================
def resolve_or_create_customer(db: Session, customer_data: dict) -> Customer:
    """Stateless worker. Fetches or creates a customer profile atomically."""
    email_address = customer_data.get("email", "unknown@enterprise.io")
    customer_record = db.query(Customer).filter(Customer.email == email_address).first()

    if not customer_record:
        customer_record = Customer(
            customer_name=customer_data.get("name", "Anonymous Buyer"),
            email=email_address,
        )
        db.add(customer_record)
        db.flush()  # Populates customer_record.id safely without committing early

    return customer_record


def persist_invoice_record(
    db: Session, order_id: str, customer_id: int, amount: float
) -> Invoice:
    """Stateless worker. Inserts a row into the local domain invoices table."""
    new_invoice = Invoice(
        order_id=str(order_id),
        customer_id=int(customer_id),
        amount=float(amount),
    )
    db.add(new_invoice)
    db.flush()  # Populates new_invoice.id safely without committing early
    return new_invoice


def initialize_saga_state_tracking(
    db: Session, order_id: str, avro_payload: dict
) -> None:
    """Stateless worker. Instantiates the tracking checklist row inside the orchestration table."""
    address_info = avro_payload.get("shipping_address", {})

    saga_tracking_log = SagaState(
        order_id=str(order_id),
        saga_status="STARTED",
        finance_status="PENDING",
        shipping_status="PENDING",
        notifications_status="PENDING",
        customer_name=str(avro_payload.get("customer_name", "Unknown Buyer")),
        customer_email=str(
            avro_payload.get("customer_email", "unknown@platform.internal")
        ),
        amount=float(avro_payload.get("amount", 0.0)),
        item_id=str(avro_payload.get("item_id", "SHIRT_STANDARD_BLUE")),
        shipping_street=str(address_info.get("street", "123 Transaction Way")),
        shipping_city=str(address_info.get("city", "Default Ville")),
        shipping_state=str(address_info.get("state", "OH")),
        shipping_postal=str(address_info.get("postal_code", "00000")),
    )
    db.add(saga_tracking_log)


def stage_saga_command_envelopes(
    db: Session, order_id: str, avro_payload: dict
) -> None:
    """Stateless worker. Stages individual downstream commands to the universal outbox table."""
    target_queues = [
        ("finance_commands", "finance_commands"),
        ("shipping_commands", "shipping_commands"),
        ("notifications_commands", "notifications_commands"),
    ]

    for queue_topic, queue_name in target_queues:
        envelope_data = {
            "command_id": str(uuid.uuid4()),
            "order_id": order_id,
            "action": "NEW_SALE",
            "payload": avro_payload,
        }
        # 🟢 SOLUTION: Dispatches straight to your pristine, un-translated public outbox logging framework [1.1]
        stage_outbox_message(
            db=db,
            topic=queue_topic,
            partition_key=order_id,
            payload=envelope_data,
        )
