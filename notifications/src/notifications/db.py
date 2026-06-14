import json
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

# Point to our shared multi-tenant PostgreSQL container engine port
DATABASE_URL = "postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class CommunicationLedger(Base):
    __tablename__ = "communication_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    customer_name = Column(String)
    alert_type = Column(String)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class NotificationOutbox(Base):
    """The private outbox table for notifications to reply back to the orchestrator."""

    __tablename__ = "notification_outbox"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    key = Column(String, nullable=False)
    payload = Column(String, nullable=False)  # Will match the global SagaReply contract
    created_at = Column(DateTime, default=datetime.utcnow)


def init_notifications_db():
    """Ensures the communications ledger and private outbox schemas exist in Postgres."""
    Base.metadata.create_all(bind=engine)


def execute_notification_task_and_stage_reply(
    order_id: str, customer_name: str, action_type: str
) -> str:
    """Atomically updates the communication state records and drops a SagaReply payload

    straight into the local outbox table inside a single SQL transaction loop.
    """
    db = SessionLocal()
    timestamp_str = datetime.utcnow().isoformat()

    try:
        # 1. Evaluate incoming commands and calculate our internal status mappings
        if action_type == "NEW_SALE":
            alert_type = "ORDER_RECEIPT_DRAFT"
            status_msg = "SUCCESS"
            ledger_status = "PENDING_FINANCIAL_CLEARANCE"
        elif action_type == "CANCEL_TRANSACTION":
            alert_type = "SECURITY_FRAUD_WARNING"
            status_msg = "SUCCESS"
            ledger_status = "DISPATCHED_FRAUD_ALERT"
        else:
            alert_type = "UNKNOWN_DIRECTIVE"
            status_msg = "FAILED"
            ledger_status = "REJECTED_UNKNOWN_ACTION"

        # 2. Persist local state to the Communication Ledger
        # If the row already exists (e.g. updating an active record on cancellation), update it
        record = (
            db.query(CommunicationLedger)
            .filter(CommunicationLedger.order_id == order_id)
            .first()
        )
        if not record:
            record = CommunicationLedger(order_id=order_id, customer_name=customer_name)
            db.add(record)

        record.alert_type = alert_type
        record.status = ledger_status

        # 3. Construct payload matching our shared global 'SagaReply' Avro contract
        reply_payload = {
            "order_id": order_id,
            "department": "NOTIFICATIONS",
            "status": status_msg,
            "reason": f"Executed action {action_type} successfully.",
            "timestamp": timestamp_str,
        }

        # 4. Stage outbound reply row inside our private outbox table
        outbox_entry = NotificationOutbox(
            topic="saga_replies",  # ◄── Points straight back to the central conductor
            key=order_id,
            payload=json.dumps(reply_payload),
        )
        db.add(outbox_entry)

        # ATOMIC SAVE GATEWAY COMMIT
        db.commit()
        print(
            f"   ✔ [NOTIFICATIONS DB]: Atomically saved ledger state & staged reply outbox row for Order: {order_id}"
        )
        return status_msg

    except Exception as e:
        db.rollback()
        print(
            f"   ❌ [NOTIFICATIONS DB CRITICAL]: Transaction failed, issuing complete rollback: {str(e)}"
        )
        raise e
    finally:
        db.close()
