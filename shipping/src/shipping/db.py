import json
import logging
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

logger = logging.getLogger("SHIPPING_SERVICE.DATABASE")

Base = declarative_base()

DATABASE_URL = "postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class ShippingLedger(Base):
    __tablename__ = "shipping_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    item_id = Column(String)
    customer_name = Column(String)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class ShippingOutbox(Base):
    """The private outbox table for shipping to report task outcomes back to the conductor."""

    __tablename__ = "shipping_outbox"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    key = Column(String, nullable=False)
    payload = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_shipping_db():
    Base.metadata.create_all(bind=engine)


def stage_shipping_secured_event(
    order_event: dict, ledger_status: str, status_msg: str, reason_text: str
) -> str:
    """HIGH-UTILITY CORE WRITER: Blindly records the calculated state to the database

    and stages the outbound saga reply envelope token inside a single ACID transaction block.
    """
    db = SessionLocal()
    order_id = order_event.get("order_id")
    timestamp_str = datetime.utcnow().isoformat()

    try:
        # 1. Update/Insert the local state tracking record into the ledger
        shipping_record = (
            db.query(ShippingLedger).filter(ShippingLedger.order_id == order_id).first()
        )
        if not shipping_record:
            shipping_record = ShippingLedger(
                order_id=order_id,
                item_id=order_event.get("item_id"),
                customer_name=order_event.get("customer_name"),
            )
            db.add(shipping_record)

        shipping_record.status = ledger_status

        # 2. Construct payload matching our shared global 'SagaReply' Avro contract
        reply_payload = {
            "order_id": order_id,
            "department": "SHIPPING",
            "status": status_msg,
            "reason": reason_text,
            "timestamp": timestamp_str,
        }

        # 3. Stage outbound reply row inside our private outbox table
        outbox_entry = ShippingOutbox(
            topic="saga_replies", key=order_id, payload=json.dumps(reply_payload)
        )
        db.add(outbox_entry)

        db.commit()
        logger.info(
            f"Database Transaction Committed | Local Status: {ledger_status} | Order UUID: {order_id}"
        )
        return status_msg

    except Exception as e:
        db.rollback()
        logger.error(
            f"Database Write Failure | Issuing Rollback for Order UUID: {order_id} | Error: {str(e)}"
        )
        raise e
    finally:
        db.close()
