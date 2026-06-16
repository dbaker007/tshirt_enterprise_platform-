import json
import logging
import os
import sys
import time
from typing import Callable, Generic, List, Tuple, TypeVar

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from opentelemetry import trace
from sqlalchemy.orm import Session

logger = logging.getLogger("DAEMONS.MASTER_ENGINE")

T_Model = TypeVar("T_Model")


class OutboxDaemonEngine(Generic[T_Model]):
    """Enterprise-grade concrete framework engine that handles database transaction

    polling, dynamic multi-schema serialization, and auto-instrumented Kafka streaming
    out-of-band, completely eliminating department-specific boilerplate code.
    """

    def __init__(
        self,
        service_name: str,
        daemon_name: str,
        client_id: str,
        session_factory: Callable[[], Session],
        outbox_model: T_Model,
        poll_interval: float = 2.0,
    ):
        self.service_name = service_name
        self.daemon_name = daemon_name
        self.session_factory = session_factory
        self.outbox_model = outbox_model
        self.poll_interval = poll_interval

        # 1. Initialize Universal Kafka Network Coordinates
        KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
        SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"

        KAFKA_CONFIG = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": client_id,
            "acks": "all",
            "allow.auto.create.topics": True,
        }
        self.producer = Producer(KAFKA_CONFIG)

        # 2. Universal Schema Registry Compilation
        self.schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        # 🛠️ FIXED: Symmetrical schema path resolution relative to its new nested home!
        schemas_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "schemas")
        )

        with open(os.path.join(schemas_root, "command_envelope.avsc"), "r") as f:
            command_schema_str = f.read()
        with open(os.path.join(schemas_root, "saga_reply.avsc"), "r") as f:
            reply_schema_str = f.read()

        self.command_serializer = AvroSerializer(
            self.schema_registry_client, command_schema_str, lambda obj, ctx: obj
        )
        self.reply_serializer = AvroSerializer(
            self.schema_registry_client, reply_schema_str, lambda obj, ctx: obj
        )

        # 3. OpenTelemetry Process Initialization
        from observability.tracing import initialize_tracer

        self.tracer = initialize_tracer(service_name)

        self.poison_pill_tracker = {}
        self.max_retry_threshold = 3

    def get_db_session(self) -> Session:
        return self.session_factory()

    def fetch_pending_rows(self, db: Session) -> List[T_Model]:
        return (
            db.query(self.outbox_model)
            .order_by(self.outbox_model.id.asc())
            .limit(20)
            .all()
        )

    def process_single_row(self, db: Session, row: T_Model) -> bool:
        """Processes a single outbox record row by verifying its contents, packing
        W3C trace metadata headers, and routing it cleanly to the Kafka producer.
        """
        target_topic = str(getattr(row, "topic", "unknown_topic"))
        row_key = str(getattr(row, "partition_key", "unknown_key"))
        raw_payload_string = getattr(row, "payload", None)
        stored_trace_context = getattr(row, "trace_context", None)

        # 🔬 FIXED: Routed directly through the true module-level 'logger' object
        logger.info(
            f"[OUTBOX AUDIT] Intercepted Row | Topic: [{target_topic}] | Key: [{row_key}]"
        )
        logger.info(
            f"[OUTBOX AUDIT] trace_context payload: {repr(stored_trace_context)}"
        )

        if raw_payload_string is None:
            logger.error(
                f"❌ Database outbox payload column is NULL or unreadable for Key: {row_key}"
            )
            raise ValueError(f"Payload column is NULL for Key: {row_key}")

        raw_payload_dict = json.loads(str(raw_payload_string))
        context = SerializationContext(target_topic, MessageField.VALUE)

        if target_topic == "saga_replies":
            serialized_value = self.reply_serializer(raw_payload_dict, context)
        else:
            serialized_value = self.command_serializer(raw_payload_dict, context)

        delivery_status = {"success": False, "error": None}

        def delivery_report(err, msg):
            if err is not None:
                delivery_status["error"] = err
            else:
                delivery_status["success"] = True

        kafka_headers = []
        if stored_trace_context:
            kafka_headers.append(
                ("traceparent", str(stored_trace_context).encode("utf-8"))
            )
            logger.info(
                f"[OUTBOX AUDIT] ✔ [HEADER PACKED]: Injected traceparent metadata onto Kafka wire array."
            )
        else:
            logger.warning(
                f"[OUTBOX AUDIT] ⚠️ [HEADER EMPTY]: trace_context is missing or unpopulated."
            )

        # 🛠️ FIXED: Re-injected the core transmission block that was missing from the refactor!
        try:
            self.producer.produce(
                topic=target_topic,
                key=str(row_key).encode("utf-8"),
                value=serialized_value,
                headers=kafka_headers,
                callback=delivery_report,
            )
            self.producer.flush()

            if delivery_status["error"]:
                logger.error(
                    f"❌ Broker Network Rejected Delivery: {delivery_status['error']}"
                )
                return False

            return True

        except Exception as transport_err:
            logger.error(
                f"❌ Fatal Outbound Transmission Pipeline Error: {str(transport_err)}"
            )
            return False

    def run_single_iteration(self) -> float:
        db = self.get_db_session()
        try:
            pending_records = self.fetch_pending_rows(db)
            if not pending_records:
                return 0.0

            for row in pending_records:
                row_id = getattr(row, "id", None)
                row_key = getattr(row, "key", "unknown_key")

                with self.tracer.start_as_current_span(
                    f"outbox_dispatch_{self.daemon_name.lower()}"
                ) as span:
                    span.set_attribute("outbox.row_id", str(row_id))
                    span.set_attribute("order.correlation_id", str(row_key))

                    try:
                        success = self.process_single_row(db, row)
                        if success:
                            db.delete(row)
                            db.commit()
                            self.poison_pill_tracker.pop(row_id, None)
                        else:
                            span.set_status(
                                trace.Status(
                                    trace.StatusCode.ERROR,
                                    description="Stream Dispatch Ingestion Failure",
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

                        self.poison_pill_tracker[row_id] = (
                            self.poison_pill_tracker.get(row_id, 0) + 1
                        )
                        attempts = self.poison_pill_tracker[row_id]

                        if attempts >= self.max_retry_threshold:
                            db.delete(row)
                            db.commit()
                            self.poison_pill_tracker.pop(row_id, None)
                            return 0.0
                        return min(2**attempts, 10.0)
        except Exception as system_fault:
            db.rollback()
        finally:
            db.close()
        return 0.0

    def start_polling_loop(self):
        logger.info(
            f"🚀 OutboxDaemonEngine Active | Service: [{self.service_name}] Node: [{self.daemon_name}] Polling channels..."
        )
        try:
            while True:
                backoff_sleep = self.run_single_iteration()
                if backoff_sleep == 0.0:
                    time.sleep(self.poll_interval)
                else:
                    time.sleep(backoff_sleep)
        except KeyboardInterrupt:
            sys.exit(0)
