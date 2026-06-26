import os
from datetime import datetime

from observability.db import get_platform_database_url
from observability.outbox import stage_outbox_message
from sqlalchemy import Column, DateTime, Integer, String, create_engine

# 🟢 IMPORT THE NATIVE POSTGRESQL INSERT UTILITY
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


LOCAL_PORT = os.environ.get("NOTIFICATIONS_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class CommunicationLedger(Base):
    __tablename__ = "communication_ledger"
    id = Column(Integer, primary_key=True, index=True)
    # 🟢 FIX: Enforce strict table-level uniqueness matching your system standard!
    order_id = Column(String, unique=True, index=True)
    customer_name = Column(String)
    execution_status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_notifications_db():
    Base.metadata.create_all(bind=engine)


def persist_communication_ledger_record(
    db: Session, order_id: str, customer_name: str, ledger_status: str
) -> None:
    """Stateless data access worker.

    Compiles and executes a native PostgreSQL atomic UPSERT statement on the server. [1.1]
    """
    # 🟢 FIX: Compiles a native ON CONFLICT clause to cleanly update execution status on collisions! [1.1]
    stmt = insert(CommunicationLedger).values(
        order_id=str(order_id),
        customer_name=str(customer_name),
        execution_status=str(ledger_status),
    )

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[CommunicationLedger.order_id],
        set_=dict(
            customer_name=str(customer_name), execution_status=str(ledger_status)
        ),
    )

    db.execute(upsert_stmt)


def stage_notifications_saga_reply(
    db: Session, order_id: str, wire_status: str, ledger_status: str
) -> None:
    """Stateless data access worker.

    Packages the standardized saga contract and routes it into the central platform outbox.
    """
    reply_envelope = {
        "order_id": str(order_id),
        "department": "NOTIFICATIONS",
        "status": str(wire_status),
        "reason": f"Customer alert notification recorded as: {ledger_status}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    stage_outbox_message(
        db=db,
        topic="saga_replies",
        partition_key=str(order_id),
        payload=reply_envelope,
    )
