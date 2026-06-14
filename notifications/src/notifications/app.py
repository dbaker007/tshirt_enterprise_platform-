import logging
import os
import sys

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

# Import your active production data repository assets
from notifications.db import (
    execute_notification_task_and_stage_reply,
    init_notifications_db,
)

# =========================================================================
# 1. ENTERPRISE LOGGER SPECIFICATION
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("NOTIFICATIONS_SERVICE")

# Platform Infrastructure Network Parameters
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# DRY COMPLIANCE: Load the shared CommandEnvelope schema straight from the root schemas/ hub
schema_path = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "schemas", "command_envelope.avsc"
)

with open(schema_path, "r") as f:
    schema_str = f.read()

avro_deserializer = AvroDeserializer(
    schema_registry_client, schema_str, lambda obj, ctx: obj
)

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "enterprise_notifications_processing_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(CONSUMER_CONFIG)
# Listen strictly to your private, dedicated department command queue mailbox
consumer.subscribe(["notifications_commands"])


def notifications_subgraph_node(command_envelope: dict):
    """The processing node that evaluates incoming conductor directives and routes state changes."""
    import json

    order_id = command_envelope.get("order_id")
    action = command_envelope.get("action")
    raw_payload_str = command_envelope.get("payload", "{}")

    # Safely unpack the embedded contextual variables passed down by the orchestrator
    payload_dict = json.loads(raw_payload_str)
    customer_name = payload_dict.get("customer_name", "Valued Customer")

    logger.info(f"Command Ingested | Action: {action} | Order UUID: {order_id}")

    # Execute the atomic dual-write repository function to log state and stage the outbox reply
    execute_notification_task_and_stage_reply(
        order_id=order_id, customer_name=customer_name, action_type=action
    )
    logger.info(f"Lifecycle State Processed Successfully | Order UUID: {order_id}")


if __name__ == "__main__":
    init_notifications_db()
    pid = os.getpid()
    logger.info(
        f"Service Booted | Process ID: {pid} | Polling 'notifications_commands' channel..."
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka Broker Stream Fault: {msg.error()}")
                    break

            try:
                # Deserialize the incoming Avro envelope bytes back into a native Python dictionary
                context = SerializationContext(msg.topic(), MessageField.VALUE)
                command_data = avro_deserializer(msg.value(), context)

                # Route the command data dictionary straight through your graph processing gateway
                notifications_subgraph_node(command_data)

                # Manual Position Commited Checkpoint Lock
                consumer.commit(msg, asynchronous=False)

            except Exception as stream_err:
                logger.error(
                    f"Data Pipeline Serialization/Processing Exception: {str(stream_err)}"
                )
                consumer.commit(msg, asynchronous=False)

    except KeyboardInterrupt:
        logger.info(f"Shutdown Signal Intercepted | Process ID {pid} exiting safely.")
    finally:
        consumer.close()
