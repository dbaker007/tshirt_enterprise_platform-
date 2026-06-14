import json
import logging
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# =========================================================================
# 1. ENTERPRISE LOGGER SPECIFICATION
# =========================================================================
logger = logging.getLogger("FINANCE_SERVICE.DATABASE")

Base = declarative_base()

# Point to our shared multi-tenant PostgreSQL container engine port
DATABASE_URL = "postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class FinanceLedger(Base):
    __tablename__ = "finance_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    customer_name = Column(String)
    amount = Column(String)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class FinanceOutbox(Base):
    """The private outbox table for finance to report auditing states back to the conductor."""

    __tablename__ = "finance_outbox"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    key = Column(String, nullable=False)
    payload = Column(String, nullable=False)  # Will match the global SagaReply contract
    created_at = Column(DateTime, default=datetime.utcnow)


def init_finance_db():
    """Ensures the private finance ledger and auditing outbox tables exist in Postgres."""
    Base.metadata.create_all(bind=engine)


def execute_financial_clearance_and_stage_outbox(
    order_event: dict, status_msg: str
) -> str:
    """Atomically records finance audit parameters and stages an outbound saga reply token."""
    db = SessionLocal()
    order_id = order_event.get("order_id")
    timestamp_str = datetime.utcnow().isoformat()

    try:
        # 1. Update/Insert the local state tracking record into the ledger
        ledger_entry = (
            db.query(FinanceLedger).filter(FinanceLedger.order_id == order_id).first()
        )
        if not ledger_entry:
            ledger_entry = FinanceLedger(
                order_id=order_id,
                customer_name=order_event.get("customer_name"),
                amount=str(order_event.get("amount", "0.0")),
            )
            db.add(ledger_entry)

        ledger_entry.status = status_msg

        # 2. Construct payload matching our shared global 'SagaReply' Avro contract
        reply_payload = {
            "order_id": order_id,
            "department": "FINANCE",
            "status": status_msg,
            "reason": f"Financial clearance engine processed transaction state.",
            "timestamp": timestamp_str,
        }

        # 3. Stage outbound reply row inside our private outbox table
        outbox_entry = FinanceOutbox(
            topic="saga_replies",  # ◄── Points straight back to the central conductor
            key=order_id,
            payload=json.dumps(reply_payload),
        )
        db.add(outbox_entry)

        # ATOMIC TRANSACTION GATEWAY COMMIT
        db.commit()
        logger.info(
            f"Transaction Committed | Staged Outbox Reply -> topic: saga_replies | Order UUID: {order_id}"
        )
        return status_msg

    except Exception as e:
        db.rollback()
        # FIXED: Downgraded to a warning and stripped out the old legacy token phrase string!
        logger.warning(
            f"Database Transaction Fault | Issuing Rollback for Order UUID: {order_id} | Details: {str(e)}"
        )
        raise e
    finally:
        db.close()
