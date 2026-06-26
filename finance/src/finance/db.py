import os
from datetime import datetime

from observability.db import get_platform_database_url
from observability.outbox import stage_outbox_message
from sqlalchemy import Column, DateTime, Integer, String, create_engine

# 🟢 IMPORT THE NATIVE POSTGRESQL INSERT UTILITY [1.1]
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


LOCAL_PORT = os.environ.get("FINANCE_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class FinanceLedger(Base):
    __tablename__ = "finance_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    execution_status = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_finance_db():
    Base.metadata.create_all(bind=engine)


def persist_financial_ledger_record(
    db: Session, order_id: str, ledger_status: str
) -> None:
    """Stateless data access worker.

    Compiles and executes a native PostgreSQL atomic UPSERT statement on the server. [1.1]
    """
    # 🟢 FIX: Uses native Postgres atomic UPSERT to safely overwrite status counters on key collisions [1.1]
    stmt = insert(FinanceLedger).values(
        order_id=str(order_id), execution_status=str(ledger_status)
    )

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[FinanceLedger.order_id],
        set_=dict(execution_status=str(ledger_status)),
    )

    db.execute(upsert_stmt)


def stage_finance_saga_reply(
    db: Session, order_id: str, wire_status: str, ledger_status: str
) -> None:
    """Stateless data access worker.

    Packages the standardized saga contract and routes it into the central platform outbox.
    """
    reply_envelope = {
        "order_id": str(order_id),
        "department": "FINANCE",
        "status": str(wire_status),
        "reason": f"Financial clearance status evaluated as: {ledger_status}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    stage_outbox_message(
        db=db,
        topic="saga_replies",
        partition_key=str(order_id),
        payload=reply_envelope,
    )
