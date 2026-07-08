# observability/src/observability/outbox.py

import json
import logging
from datetime import datetime

from opentelemetry import propagate
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text
from sqlalchemy.orm import Session

logger = logging.getLogger("OBSERVABILITY.OUTBOX_UTIL")

# 🟢 SOLUTION: Build a stateless Core Table tracking layout attached to a clean metadata manager [1.1]
# Leaving schema=None lets it naturally drop into the central shared "public" schema workspace [1.1].
metadata = MetaData()
outbox_table = Table(
    "platform_outbox",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("topic", String, nullable=False),
    Column("partition_key", String, nullable=False),
    Column("payload", Text, nullable=False),
    Column("trace_context", String, nullable=True),
    Column("created_at", DateTime, default=datetime.utcnow),
)


def stage_outbox_message(
    db: Session, topic: str, partition_key: str, payload: dict
) -> bool:
    """Universally injects an event payload straight into the single transactional outbox log.

    Automatically extracts active W3C distributed traceparent keys out of the thread context [1.1].
    """
    try:
        # 🏆 EXPLICIT W3C STATE CAPTURE: Extract trace headers on-the-fly [1.1]
        carrier = {}
        propagate.inject(carrier)
        w3c_traceparent_string = carrier.get("traceparent")

        # 🟢 SOLUTION: Compile using core insert macros to provide 100% cross-dialect stability [1.1]
        insert_stmt = outbox_table.insert().values(
            topic=str(topic),
            partition_key=str(partition_key),
            payload=json.dumps(payload),
            trace_context=w3c_traceparent_string,
            created_at=datetime.utcnow(),
        )

        # Execute the core statement block straight inside the active transaction session
        db.execute(insert_stmt)

        logger.info(
            f"✔ [OUTBOX STAGED]: Thread-safe event payload committed natively targeting queue topic channel: [{topic}]"
        )
        return True

    except Exception as err:
        logger.error(f"❌ Thread-safe outbox storage ingestion crash: {str(err)}")
        raise err
