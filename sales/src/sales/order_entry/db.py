import logging
import os
import uuid
from datetime import datetime

from observability.db import get_platform_database_url
from observability.outbox import stage_outbox_message
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

logger = logging.getLogger("SALES_SERVICE.DATABASE")


class Base(DeclarativeBase):
    pass


# =========================================================================
# 📡 COREDNS NETWORK ROUTING CONTROLS (Environment-Aware)
# =========================================================================
LOCAL_PORT = os.environ.get("SALES_API_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


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


class SagaState(Base):
    __tablename__ = "saga_states"
    order_id = Column(String, primary_key=True, index=True)
    saga_status = Column(String, nullable=False)
    finance_status = Column(String, default="PENDING")
    shipping_status = Column(String, default="PENDING")
    notifications_status = Column(String, default="PENDING")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_sales_db():
    Base.metadata.create_all(bind=engine)


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


def initialize_saga_state_tracking(db: Session, order_id: str) -> None:
    """Stateless worker. Instantiates the tracking checklist row inside the orchestration table."""
    saga_tracking_log = SagaState(
        order_id=str(order_id),
        saga_status="STARTED",
        finance_status="PENDING",
        shipping_status="PENDING",
        notifications_status="PENDING",
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
        stage_outbox_message(
            db=db,
            topic=queue_topic,
            partition_key=order_id,
            payload=envelope_data,
        )
