import logging
import os
import sys
import time
from abc import ABC, abstractmethod

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from observability.tracing import initialize_tracer, trace_kafka_message


class MicroserviceConsumerApp(ABC):
    """Abstract Parent Base Class that encapsulates message consumption boilerplate,

    Avro contract mapping, and thread-safe telemetry context tracing out-of-band.
    """

    def __init__(
        self,
        service_name: str,
        group_base_id: str,
        topic_channel: str,
        schema_filename: str,
    ):
        self.service_name = service_name
        self.topic_channel = topic_channel

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )
        self.logger = logging.getLogger(f"{service_name.upper()}.APP")
        self.tracer = initialize_tracer(service_name)

        KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

        registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        # Extract the environment override variable cleanly
        env_root = os.getenv("PROJECT_ROOT")

        # 🟢 FIX: Verify if the env path exists and contains schemas; if not, fall back to getcwd()!
        if env_root and os.path.exists(os.path.join(env_root, "schemas")):
            project_root = env_root
        else:
            project_root = os.getcwd()

        schemas_root = os.path.join(project_root, "schemas")
        schema_path = os.path.join(schemas_root, schema_filename)

        self.logger.info(
            f"📡 [FRAMEWORK INIT]: Resolving Avro Contract Schema Path -> {schema_path}"
        )

        with open(schema_path, "r") as f:
            schema_str = f.read()

        self.deserializer = AvroDeserializer(
            registry_client, schema_str, lambda obj, ctx: obj
        )

        runtime_suffix = int(time.time())
        self.consumer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"{group_base_id}_{runtime_suffix}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([topic_channel])

    @abstractmethod
    def execute_business_logic(self, order_payload: dict, action: str):
        pass

    def start_polling_loop(self):
        """Maintains the continuous, high-speed inbound consumer execution loop,

        explicitly guarding against empty poll timeouts and initialization errors.
        """
        pid = os.getpid()
        self.logger.info(
            f"Saga Node Booted | Process ID: {pid} | Subscribed to Channel: [{self.topic_channel}]"
        )

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        # Log the broker error and continue polling
                        self.logger.warning(
                            f"⚠️ Broker Subscription Sync Code: {msg.error()}"
                        )
                        time.sleep(1.0)
                        continue

                try:
                    context = SerializationContext(msg.topic(), MessageField.VALUE)
                    envelope = self.deserializer(msg.value(), context)

                    action = envelope.get("action", "NEW_SALE")
                    order_payload = envelope.get("payload", envelope)

                    if "order_id" not in order_payload:
                        order_payload["order_id"] = envelope.get("order_id")

                    # Extract the embedded W3C string out of the Avro envelope payload!
                    extracted_carrier = {}
                    w3c_string = envelope.get("trace_context")
                    if w3c_string:
                        extracted_carrier["traceparent"] = str(w3c_string)

                    # Pass the extracted carrier dictionary cleanly into your tracing middleware utility
                    with trace_kafka_message(
                        self.tracer,
                        f"kafka_receive_{self.topic_channel}",
                        msg,
                        extracted_carrier=extracted_carrier,
                    ):
                        self.execute_business_logic(order_payload, action)

                    self.consumer.commit(msg, asynchronous=False)

                except Exception as loop_err:
                    self.logger.error(
                        f"Data Pipeline Processing Exception: {str(loop_err)}"
                    )
                    self.consumer.commit(msg, asynchronous=False)

        except KeyboardInterrupt:
            self.logger.info(
                f"Shutdown Signal Intercepted | Process ID {pid} exiting safely."
            )
        finally:
            self.consumer.close()
