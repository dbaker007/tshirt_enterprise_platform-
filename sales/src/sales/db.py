import json
import logging
import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# =========================================================================
# 1. ENTERPRISE LOGGER SPECIFICATION
# =========================================================================
logger = logging.getLogger("SALES_SERVICE.DATABASE")


class Base(DeclarativeBase):
    pass


DATABASE_URL = "postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# =========================================================================
# 2. DATABASE SCHEMA MODELS
# =========================================================================
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
    """The master outbox table used by the conductor to dispatch directed commands."""

    __tablename__ = "outbox"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    key = Column(String, nullable=False)
    payload = Column(
        String, nullable=False
    )  # Will match the global CommandEnvelope contract
    created_at = Column(DateTime, default=datetime.utcnow)


class SagaState(Base):
    """The central checklist tracking table managing the multi-department state log."""

    __tablename__ = "saga_states"
    order_id = Column(String, primary_key=True, index=True)
    status = Column(
        String, nullable=False
    )  # STARTED, COMPLETED, ROLLING_BACK, REJECTED
    finance_status = Column(String, default="PENDING")
    shipping_status = Column(String, default="PENDING")
    notifications_status = Column(String, default="PENDING")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_sales_db():
    """Ensures relational tables and outbox metadata structures exist natively in Postgres."""
    Base.metadata.create_all(bind=engine)


def persist_sale_and_stage_outbox(transaction: dict) -> tuple[str, int]:
    """Atomically initializes the Saga Checklist State, registers the transaction data,

    and stages three independent worker commands natively inside a single SQL block.
    """
    db = SessionLocal()
    generated_order_id = str(uuid.uuid4())

    try:
        # 1. Store/Lookup Customer Profile State
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

        # 2. Stage Invoice Metadata Details
        new_invoice = Invoice(
            order_id=generated_order_id,
            customer_id=new_customer.id,
            amount=transaction["amount"],
        )
        db.add(new_invoice)

        # 3. Initialize the Conductor's Master Saga Checklist Record
        saga_tracking_log = SagaState(
            order_id=generated_order_id,
            status="STARTED",
            finance_status="PENDING",
            shipping_status="PENDING",
            notifications_status="PENDING",
        )
        db.add(saga_tracking_log)

        # 4. Construct strictly typed address payload data block to pass down tree
        raw_address_info = transaction.get("shipping_address", {})
        address_record = {
            "street": raw_address_info.get("street", "123 Default Way"),
            "city": raw_address_info.get("city", "Default Ville"),
            "state": raw_address_info.get(
                "state", "OH"
            ),  # ◄── THE CORE GEOGRAPHY ANCHOR FOR LAWS
            "postal_code": raw_address_info.get("postal_code", "00000"),
        }

        # Symmetrically assemble the precise payload matching 'OrderPayloadRecord' in Avro
        avro_compatible_payload = {
            "customer_name": new_customer.name,
            "customer_email": new_customer.email,
            "amount": float(new_invoice.amount),
            "item_id": transaction.get("item_id", "SHIRT_STANDARD_BLUE"),
            "shipping_address": address_record,  # ◄── NESTED AS A PROPER RECORD OBJECT TREE
        }

        # 5. HIGH-UTILITY MULTI-ROW OUTBOX PACKAGING
        target_queues = [
            ("finance_commands", "finance_commands"),
            ("shipping_commands", "shipping_commands"),
            ("notifications_commands", "notifications_commands"),
        ]

        for queue_topic, queue_name in target_queues:
            # Wrap everything natively inside the precise top-level 'CommandEnvelope' fields
            envelope_data = {
                "command_id": str(uuid.uuid4()),
                "order_id": generated_order_id,
                "action": "NEW_SALE",
                "payload": avro_compatible_payload,  # ◄── PASSED AS A DICTIONARY TREE, NOT STRING
            }

            outbox_event = Outbox(
                topic=queue_topic,
                key=generated_order_id,
                payload=json.dumps(
                    envelope_data
                ),  # Serialized to flat string row for database disk column storage only
            )
            db.add(outbox_event)

        # ATOMIC LOCAL TRANSACTION GATEWAY COMMIT
        db.commit()
        db.refresh(new_invoice)

        logger.info(
            f"Saga Workflow Initialized | Checklist Saved & 3 Commands Staged | Order UUID: {generated_order_id}"
        )
        return generated_order_id, new_invoice.id

    except Exception as e:
        db.rollback()
        logger.error(
            f"Saga Initialization Critical Failure | Issuing Rollback | Error: {str(e)}"
        )
        raise e
    finally:
        db.close()
