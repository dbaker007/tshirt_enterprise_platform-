import os
from typing import Any, Dict

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"


def initialize_consumer_dependencies() -> AvroDeserializer:
    """Connects to the schema container and compiles the Avro CommandEnvelope contract."""
    from shipping.db import init_shipping_db

    init_shipping_db()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    # 3-hop relative path lookup to secure the contract file from root schemas/ hub
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "schemas", "command_envelope.avsc"
    )
    with open(schema_path, "r") as f:
        schema_str = f.read()
    return AvroDeserializer(schema_registry_client, schema_str, lambda obj, ctx: obj)


def extract_command_payload(msg, deserializer) -> tuple[Dict[str, Any], str]:
    """Unpacks the inner order payload metadata dictionary from raw message bytes."""
    context = SerializationContext(msg.topic(), MessageField.VALUE)
    command_envelope = deserializer(msg.value(), context)
    action = command_envelope.get("action")
    order_id = command_envelope.get("order_id")
    order_payload = command_envelope.get("payload", {})

    if "order_id" not in order_payload:
        order_payload["order_id"] = order_id

    return order_payload, action
