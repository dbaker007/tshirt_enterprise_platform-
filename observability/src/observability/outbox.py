import json
import logging
from datetime import datetime

from opentelemetry import propagate
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Session

logger = logging.getLogger("OBSERVABILITY.OUTBOX_UTIL")


class Base(DeclarativeBase):
    pass


class PlatformOutboxRecord(Base):
    """The universal declarative mapping matching your cluster platform_outbox schema."""

    __tablename__ = "platform_outbox"

    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    partition_key = Column(String, nullable=False)
    payload = Column(Text, nullable=False)
    trace_context = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def stage_outbox_message(
    db: Session, topic: str, partition_key: str, payload: dict
) -> bool:
    """Universally injects an event payload straight into the single transactional outbox log.

    Automatically extracts active W3C distributed traceparent keys out of the thread context.
    """
    try:
        # 🏆 EXPLICIT W3C STATE CAPTURE: Extract trace headers on-the-fly [1.1]
        carrier = {}
        propagate.inject(carrier)
        w3c_traceparent_string = carrier.get("traceparent")

        # Instantiate your single, generic database data structure model
        outbox_entry = PlatformOutboxRecord(
            topic=topic,
            partition_key=str(partition_key),
            payload=json.dumps(payload),
            trace_context=w3c_traceparent_string,
        )

        db.add(outbox_entry)
        logger.info(
            f"✔ [OUTBOX STAGED]: Stored event payload targeting queue topic channel: [{topic}]"
        )
        return True

    except Exception as err:
        logger.error(
            f"❌ Failed to stage transaction event into universal outbox log: {str(err)}"
        )
        raise err
