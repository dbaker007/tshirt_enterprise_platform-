import json
import os
import uuid
from datetime import datetime

# 🏆 IMPORT THE UNIVERSAL W3C CONTEXT PROPAGATOR HOOK
from opentelemetry import propagate
from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class Base(DeclarativeBase):
    pass


DATABASE_URL = "postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class CommunicationLedger(Base):
    __tablename__ = "communication_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, index=True)
    customer_name = Column(String)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class NotificationOutbox(Base):
    """The transactional outbox table carrying the explicit W3C trace string context for replies."""

    __tablename__ = "notification_outbox"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    key = Column(String, nullable=False)
    payload = Column(String, nullable=False)
    # 🛠️ THE EXACT W3C TELEMETRY CARRIER STORAGE FIELD
    trace_context = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_notifications_db():
    Base.metadata.create_all(bind=engine)


def execute_notification_task_and_stage_reply(
    order_id: str, customer_name: str, action: str
):
    """Executes atomic ledger persistence and dual-writes a tracking outbox reply with OTel metadata."""
    db = SessionLocal()

    try:
        # 🏆 EXPLICIT STATE CAPTURE PATTERN:
        # Pull the active W3C trace metadata out of the current execution thread,
        # serialize it to a clean string format, and store it in our outbox table.
        carrier = {}
        propagate.inject(carrier)
        w3c_traceparent_string = carrier.get("traceparent")

        # 1. Update your local communication ledger records
        status_msg = (
            "PENDING_FINANCIAL_CLEARANCE"
            if action == "NEW_SALE"
            else "DISPATCHED_FRAUD_ALERT"
        )
        new_entry = CommunicationLedger(
            order_id=order_id, customer_name=customer_name, status=status_msg
        )
        db.add(new_entry)

        # 2. Package the unified saga reply payload contract
        reply_envelope = {
            "order_id": str(order_id),
            "department": "NOTIFICATIONS",
            "status": "SUCCESS",
        }

        # 3. Double-write the event record straight to the transaction log table
        outbox_reply = NotificationOutbox(
            topic="saga_replies",
            key=str(order_id),
            payload=json.dumps(reply_envelope),
            # Save the explicit W3C string context natively on the record
            trace_context=w3c_traceparent_string,
        )
        db.add(outbox_reply)
        db.commit()

    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()
