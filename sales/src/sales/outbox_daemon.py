import json
import logging
import os
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Import your private sales data access tracking assets
from db import Outbox, SessionLocal, init_sales_db
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
logger = logging.getLogger("SALES_COMMAND_DAEMON")

SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# DRY COMPLIANCE: Load the shared CommandEnvelope data contract straight from our root schemas/ hub
schema_path = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "schemas", "command_envelope.avsc"
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
    "client.id": "sales_command_outbox_daemon",
    "acks": "all",
}
producer = Producer(KAFKA_CONFIG)

POISON_PILL_TRACKER = {}
MAX_RETRY_THRESHOLD = 3


def process_sales_command_outbox_ledger() -> int:
    """Polls private sales outbox table, serializes commands via Avro, and routes to worker queues."""
    db = SessionLocal()
    try:
        # Enforce structural integrity out-of-band using our data layer initializer
        init_sales_db()

        pending_commands = db.query(Outbox).order_by(Outbox.id.asc()).limit(20).all()

        if not pending_commands:
            return 0

        logger.info(
            f"Discovered {len(pending_commands)} pending commands inside outbox table. Commencing dispatch..."
        )

        for command in pending_commands:
            command_id = command.id
            delivery_status = {"success": False, "error": None}

            def delivery_report(err, msg):
                if err is not None:
                    delivery_status["error"] = err
                else:
                    delivery_status["success"] = True

            raw_payload_dict = json.loads(command.payload)

            try:
                from confluent_kafka.serialization import (
                    MessageField,
                    SerializationContext,
                )

                serialized_value = avro_serializer(
                    raw_payload_dict,
                    SerializationContext(command.topic, MessageField.VALUE),
                )
            except Exception as schema_err:
                POISON_PILL_TRACKER[command_id] = (
                    POISON_PILL_TRACKER.get(command_id, 0) + 1
                )
                current_retries = POISON_PILL_TRACKER[command_id]

                logger.warning(
                    f"Command Contract Verification Failure (Attempt {current_retries}/{MAX_RETRY_THRESHOLD}) "
                    f"for Key: {command.key} | Details: {str(schema_err)}"
                )

                if current_retries >= MAX_RETRY_THRESHOLD:
                    logger.error(
                        f"Poison-Pill Block Isolated | Purging Command Row Key {command.key} to prevent system deadlock."
                    )
                    db.delete(command)
                    db.commit()
                    POISON_PILL_TRACKER.pop(command_id, None)
                    return 0

                backoff_time = min(2**current_retries, 10)
                return backoff_time

            # Stream the standardized Avro payload directly to the explicit department queue topic channel
            producer.produce(
                topic=command.topic,
                key=command.key,
                value=serialized_value,
                callback=delivery_report,
            )
            producer.flush()

            if delivery_status["success"]:
                db.delete(command)
                db.commit()
                POISON_PILL_TRACKER.pop(command_id, None)
                logger.info(
                    f"Command Dispatched | Target Channel: {command.topic} | Key: {command.key} -> Cleaned from DB."
                )
            else:
                logger.error(
                    f"Kafka Broker Ingestion Rejection for Command Key {command.key}: {delivery_status['error']}"
                )
                return 5

    except Exception as e:
        db.rollback()
        logger.error(
            f"Database/Stream Command Pipeline Critical Exception Encountered: {str(e)}"
        )
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    logger.info(
        "Sales Conductor Command Outbox Background Streaming Worker Daemon Active and Polling..."
    )
    try:
        while True:
            sleep_duration = process_sales_command_outbox_ledger()
            if sleep_duration == 0:
                time.sleep(2)
            else:
                time.sleep(sleep_duration)
    except KeyboardInterrupt:
        logger.info(
            "Daemon Shutdown Command Received. Disengaging command dispatch processing safely."
        )
        sys.exit(0)
