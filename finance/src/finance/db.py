import json
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


class FinanceLedger(Base):
    __tablename__ = "finance_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    status = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class FinanceOutbox(Base):
    """The transactional outbox table carrying the explicit W3C trace string context for replies."""

    __tablename__ = "finance_outbox"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    key = Column(String, nullable=False)
    payload = Column(String, nullable=False)
    # 🛠️ THE EXACT W3C TELEMETRY CARRIER STORAGE FIELD
    trace_context = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_finance_db():
    Base.metadata.create_all(bind=engine)


def execute_financial_clearance_and_stage_outbox(order_event: dict, status_msg: str):
    """Executes atomic ledger persistence and dual-writes a tracking outbox reply with OTel metadata."""
    db = SessionLocal()
    order_id = order_event.get("order_id")

    try:
        # 🏆 EXPLICIT STATE CAPTURE PATTERN:
        # Pull the active W3C trace metadata out of the current execution thread,
        # serialize it to a clean string format, and store it in our outbox table.
        carrier = {}
        propagate.inject(carrier)
        w3c_traceparent_string = carrier.get("traceparent")

        # 1. Update your local ledger state
        new_ledger_entry = FinanceLedger(order_id=str(order_id), status=status_msg)
        db.add(new_ledger_entry)

        # 2. Package the unified saga reply payload contract
        # 🏆 FIXED: Explicitly provide mandatory contract schema variables natively!
        reply_envelope = {
            "order_id": str(order_id),
            "department": "FINANCE",
            "status": status_msg,
            "reason": f"Financial clearance status evaluated as: {status_msg}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # 3. Double-write the event record straight to the transaction log table
        outbox_reply = FinanceOutbox(
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
