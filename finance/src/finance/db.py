import os
from datetime import datetime

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from observability.outbox import stage_outbox_message
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    pass


class FinanceLedger(Base):
    __tablename__ = "finance_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    execution_status = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_finance_db(engine) -> None:
    """Binds and maps the core relational schema definitions straight onto the provided engine runtime context."""
    Base.metadata.create_all(bind=engine)


async def get_finance_checkpointer() -> AsyncSqliteSaver:
    """Instantiates and returns the persistent non-blocking asynchronous SQLite checkpointer instance."""
    checkpoint_db_path = os.environ.get(
        "FINANCE_CHECKPOINT_DB_PATH", "finance_checkpoints.sqlite"
    )
    return AsyncSqliteSaver.from_conn_string(checkpoint_db_path)


def persist_financial_ledger_record(
    db: Session, order_id: str, ledger_status: str
) -> None:
    """Stateless data access worker.
    Uses a safe integrity-catch block to handle race conditions across dialects.
    """
    target_order_id = str(order_id)
    status_str = str(ledger_status)

    # 1. Attempt a fresh insert assumption first inside an independent savepoint nested transaction
    try:
        with db.begin_nested():
            new_record = FinanceLedger(
                order_id=target_order_id, execution_status=status_str
            )
            db.add(new_record)
            db.flush()
    except IntegrityError:
        # 2. Fallback Branch: The record was written concurrently! Intercept the collision and update safely.
        record = (
            db.query(FinanceLedger)
            .filter(FinanceLedger.order_id == target_order_id)
            .first()
        )
        if record:
            record.execution_status = status_str
            db.flush()


def stage_finance_saga_reply(
    db: Session, order_id: str, wire_status: str, ledger_status: str
) -> None:
    """Packages the standardized saga contract and routes it into the central platform outbox."""
    reply_envelope = {
        "order_id": str(order_id),
        "department": "FINANCE",
        "status": str(wire_status),
        "ledger_status": str(ledger_status),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    stage_outbox_message(
        db=db,
        topic="saga_replies",
        partition_key=str(order_id),
        payload=reply_envelope,
    )


def get_finance_ledger_by_order_id(db: Session, order_id: str) -> FinanceLedger | None:
    """Retrieves the local finance shard record for a specific order UUID."""
    return (
        db.query(FinanceLedger).filter(FinanceLedger.order_id == str(order_id)).first()
    )


def get_all_finance_ledgers_by_status(
    db: Session, execution_status: str
) -> list[FinanceLedger]:
    """Returns a list of all finance records matching a specific execution status."""
    return (
        db.query(FinanceLedger)
        .filter(FinanceLedger.execution_status == str(execution_status))
        .all()
    )
