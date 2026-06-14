import json
import logging
import os
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Import your private notifications data access tracking assets
from db import NotificationOutbox, SessionLocal, init_notifications_db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# =========================================================================
# 1. ENTERPRISE LOGGER SPECIFICATION
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("NOTIFICATIONS_DAEMON")

SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# DRY COMPLIANCE: Load the shared SagaReply data contract straight from our root schemas/ hub
schema_path = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "schemas", "saga_reply.avsc"
)

with open(schema_path, "r") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=schema_str,
    to_dict=lambda obj, ctx: obj,
)

KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "notifications_avro_outbox_daemon",
    "acks": "all",
}
producer = Producer(KAFKA_CONFIG)

POISON_PILL_TRACKER = {}
MAX_RETRY_THRESHOLD = 3


def process_notifications_outbox_ledger() -> int:
    """Polls private notification outbox table, serializes via Avro, and streams out-of-band."""
    db = SessionLocal()
    try:
        # Enforce structural integrity out-of-band using our data layer initializer
        init_notifications_db()

        pending_events = (
            db.query(NotificationOutbox)
            .order_by(NotificationOutbox.id.asc())
            .limit(20)
            .all()
        )

        if not pending_events:
            return 0

        logger.info(
            f"Discovered {len(pending_events)} pending replies inside outbox table. Commencing serialization..."
        )

        for event in pending_events:
            event_id = event.id
            delivery_status = {"success": False, "error": None}

            def delivery_report(err, msg):
                if err is not None:
                    delivery_status["error"] = err
                else:
                    delivery_status["success"] = True

            raw_payload_dict = json.loads(event.payload)

            try:
                from confluent_kafka.serialization import (
                    MessageField,
                    SerializationContext,
                )

                serialized_value = avro_serializer(
                    raw_payload_dict,
                    SerializationContext(event.topic, MessageField.VALUE),
                )
            except Exception as schema_err:
                POISON_PILL_TRACKER[event_id] = POISON_PILL_TRACKER.get(event_id, 0) + 1
                current_retries = POISON_PILL_TRACKER[event_id]

                logger.warning(
                    f"Contract Verification Failure (Attempt {current_retries}/{MAX_RETRY_THRESHOLD}) "
                    f"for Key: {event.key} | Details: {str(schema_err)}"
                )

                if current_retries >= MAX_RETRY_THRESHOLD:
                    logger.error(
                        f"Poison-Pill Block Isolated | Purging Row Key {event.key} to prevent system deadlock."
                    )
                    db.delete(event)
                    db.commit()
                    POISON_PILL_TRACKER.pop(event_id, None)
                    return 0

                backoff_time = min(2**current_retries, 10)
                return backoff_time

            # Stream the standardized Avro payload directly to the global saga_replies topic queue
            producer.produce(
                topic=event.topic,
                key=event.key,
                value=serialized_value,
                callback=delivery_report,
            )
            producer.flush()

            if delivery_status["success"]:
                db.delete(event)
                db.commit()
                POISON_PILL_TRACKER.pop(event_id, None)
                logger.info(
                    f"Schema Verified & Streamed Outcome Reply for Key: {event.key} -> Cleaned from DB."
                )
            else:
                logger.error(
                    f"Kafka Broker Ingestion Rejection for Key {event.key}: {delivery_status['error']}"
                )
                return 5

    except Exception as e:
        db.rollback()
        logger.error(
            f"Database/Stream Pipeline Critical Exception Encountered: {str(e)}"
        )
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    logger.info(
        "Notifications Background Outbox Streaming Worker Daemon Initialized Symmetrically."
    )
    try:
        while True:
            sleep_duration = process_notifications_outbox_ledger()
            if sleep_duration == 0:
                time.sleep(2)
            else:
                time.sleep(sleep_duration)
    except KeyboardInterrupt:
        logger.info(
            "Daemon Shutdown Command Received. Disengaging background processing safely."
        )
        sys.exit(0)
