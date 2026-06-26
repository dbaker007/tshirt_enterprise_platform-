import json
import logging
import os
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from observability.tracing import initialize_tracer
from opentelemetry import context, trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Import the unified database context directly
from outbox_daemon.db import Outbox, SessionLocal, init_outbox_db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("OUTBOX_DAEMON.ENGINE")

# =========================================================================
# ⚙️ CENTRALIZED INFRASTRUCTURE BOOTSTRAP GATEWAYS
# =========================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2.0"))

KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "universal_platform_outbox_daemon",
    "acks": "all",
    "allow.auto.create.topics": True,
}
producer = Producer(KAFKA_CONFIG)

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

current_file_dir = os.path.dirname(os.path.abspath(__file__))
default_root = os.path.abspath(os.path.join(current_file_dir, "../../.."))

project_root = os.getenv("PROJECT_ROOT", default_root)

schemas_root = os.path.join(project_root, "schemas")

with open(os.path.join(schemas_root, "command_envelope.avsc"), "r") as f:
    command_schema_str = f.read()
with open(os.path.join(schemas_root, "saga_reply.avsc"), "r") as f:
    reply_schema_str = f.read()

command_serializer = AvroSerializer(
    schema_registry_client, command_schema_str, lambda obj, ctx: obj
)
reply_serializer = AvroSerializer(
    schema_registry_client, reply_schema_str, lambda obj, ctx: obj
)


tracer = initialize_tracer("outbox-daemon")

poison_pill_tracker = {}
MAX_RETRY_THRESHOLD = 3


# =========================================================================
# 🔬 CORE SINGLE ROW PIPELINE PROCESSING ENGINE
# =========================================================================
def process_single_row(db: SessionLocal, row: Outbox) -> bool:
    """Extracts data values from the log shard and pushes onto the Kafka network wire."""
    target_topic = str(row.topic)
    row_key = str(row.partition_key)
    raw_payload_string = row.payload
    stored_trace_context = row.trace_context

    logger.info(
        f"[OUTBOX AUDIT] Intercepted Row | Topic: [{target_topic}] | Key: [{row_key}]"
    )

    if raw_payload_string is None:
        logger.error(f"❌ Database outbox payload column is NULL for Key: {row_key}")
        raise ValueError(f"Payload column is NULL for Key: {row_key}")

    raw_payload_dict = json.loads(str(raw_payload_string))
    context = SerializationContext(target_topic, MessageField.VALUE)

    if target_topic == "saga_replies":
        serialized_value = reply_serializer(raw_payload_dict, context)
    else:
        serialized_value = command_serializer(raw_payload_dict, context)

    delivery_status = {"success": False, "error": None}

    def delivery_report(err, msg):
        if err is not None:
            delivery_status["error"] = err
        else:
            delivery_status["success"] = True

    kafka_headers = []
    if stored_trace_context:
        kafka_headers.append(("traceparent", str(stored_trace_context).encode("utf-8")))

    try:
        # 🚀 PERFORMANCE OPTIMIZATION: Produce asynchronously without inline flushing! [1.1]
        producer.produce(
            topic=target_topic,
            key=str(row_key).encode("utf-8"),
            value=serialized_value,
            headers=kafka_headers,
            callback=delivery_report,
        )

        # Trigger internal queue callbacks briefly to flush network buffers
        producer.poll(0)
        return True

    except Exception as transport_err:
        logger.error(
            f"❌ Fatal Outbound Transmission Pipeline Error: {str(transport_err)}"
        )
        return False


# =========================================================================
# 🎛️ BATCH SWEEP INTERFACE SCANNING ENGINE
# =========================================================================
def run_single_iteration() -> float:
    """Polls the platform_outbox table, joining parent traces and executing batch commits."""
    db = SessionLocal()
    propagator = TraceContextTextMapPropagator()
    try:
        pending_records = db.query(Outbox).order_by(Outbox.id.asc()).limit(20).all()
        if not pending_records:
            return 0.0

        for row in pending_records:
            row_id = row.id
            row_key = row.partition_key
            stored_trace_context = row.trace_context

            # 🟢 OTel FIX: Reconstruct the remote parent span context from the row value! [1.1]
            parent_context = context.get_current()
            if stored_trace_context:
                carrier = {"traceparent": str(stored_trace_context)}
                parent_context = propagator.extract(carrier=carrier)

            with tracer.start_as_current_span(
                "outbox_dispatch_platform_outbox",
                context=parent_context,
                kind=trace.SpanKind.PRODUCER,
            ) as span:
                span.set_attribute("outbox.row_id", str(row_id))
                span.set_attribute("order.correlation_id", str(row_key))

                try:
                    success = process_single_row(db, row)
                    if success:
                        db.delete(row)
                        db.commit()
                        poison_pill_tracker.pop(row_id, None)
                    else:
                        span.set_status(
                            trace.Status(
                                trace.StatusCode.ERROR, "Stream Ingestion Failure"
                            )
                        )
                        return 5.0

                except Exception as execution_err:
                    db.rollback()
                    span.record_exception(execution_err)
                    span.set_status(
                        trace.Status(
                            trace.StatusCode.ERROR, description=str(execution_err)
                        )
                    )

                    poison_pill_tracker[row_id] = poison_pill_tracker.get(row_id, 0) + 1
                    attempts = poison_pill_tracker[row_id]

                    if attempts >= MAX_RETRY_THRESHOLD:
                        db.delete(row)
                        db.commit()
                        poison_pill_tracker.pop(row_id, None)
                        return 0.0
                    return 0.0

        # 🚀 BATCH FLUSH: Drain the entire message registry buffer onto the wire in one single packet pass! [1.1]
        producer.flush(timeout=5.0)

    except Exception as system_fault:
        db.rollback()
        logger.error(f"❌ System Loop Exception Fault: {str(system_fault)}")
    finally:
        db.close()
    return 0.0


if __name__ == "__main__":
    logger.info("🚀 Universal Outbox Daemon Active | Polling platform_outbox table...")
    init_outbox_db()

    try:
        while True:
            backoff_sleep = run_single_iteration()
            if backoff_sleep == 0.0:
                time.sleep(POLL_INTERVAL)
            else:
                time.sleep(backoff_sleep)
    except KeyboardInterrupt:
        logger.info(
            "📡 Graceful shutdown signal intercepted. Exiting daemon process core loop."
        )
        sys.exit(0)
