import os
from datetime import datetime

from observability.db import get_platform_database_url

# IMPORT UNIVERSAL SYSTEM UTILITIES
from observability.outbox import stage_outbox_message
from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


# =========================================================================
# 📡 COREDNS INTERNAL CLUSTER NETWORK CHANNEL (Environment-Aware)
# =========================================================================
LOCAL_PORT = os.environ.get("SHIPPING_API_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class ShippingLedger(Base):
    __tablename__ = "shipping_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    execution_status = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_shipping_db():
    Base.metadata.create_all(bind=engine)


def persist_shipping_ledger_record(
    db: Session, order_id: str, ledger_status: str
) -> None:
    """Stateless data access worker.
    Compiles and executes a native PostgreSQL atomic UPSERT statement on the server.
    """
    # 1. Prepare a standard insert statement
    stmt = insert(ShippingLedger).values(
        order_id=str(order_id), execution_status=str(ledger_status)
    )

    # 2. Compile the "ON CONFLICT DO UPDATE" clause using your unique constraint target column
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[ShippingLedger.order_id],  # The unique constraint column target
        set_=dict(
            execution_status=str(ledger_status)
        ),  # What column field to alter on conflict
    )

    # 3. Stream the raw unified clause directly to the database hardware engine
    db.execute(upsert_stmt)


def stage_shipping_saga_reply(
    db: Session,
    order_id: str,
    wire_status: str,
    ledger_status: str,
    reason_text: str = None,
) -> None:
    """Stateless data access worker.

    Packages the standardized saga contract and routes it into the central platform outbox.
    """
    safe_reason = (
        reason_text if reason_text else f"Logistics event recorded as: {ledger_status}"
    )

    reply_envelope = {
        "order_id": str(order_id),
        "department": "SHIPPING",
        "status": str(wire_status),
        "reason": str(safe_reason),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    stage_outbox_message(
        db=db,
        topic="saga_replies",
        partition_key=str(order_id),
        payload=reply_envelope,
    )
