import atexit
import logging
import os
from contextlib import contextmanager

from opentelemetry import context, trace  # ◄── 1. IMPORT CORE CONTEXT INTERFACE
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, set_global_textmap
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

logger = logging.getLogger("OBSERVABILITY.TRACING")
_PROVIDER_INITIALIZED = False


class KafkaHeaderGetter:
    """Natively extracts W3C binary parameters out-of-band from confluent-kafka's
    list-of-tuples wire metadata headers using precise case-insensitive matching.
    """

    def get(self, carrier: list, key: str) -> list[str] | None:
        if not carrier:
            return None
        target_key = key.lower()

        for header_key, header_val in carrier:
            current_key = (
                header_key.decode("utf-8").lower()
                if isinstance(header_key, bytes)
                else str(header_key).lower()
            )
            if current_key == target_key:
                # 🛠️ FIXED: Return the raw bytes directly to OpenTelemetry's extractor!
                # This prevents the 'str object has no attribute decode' runtime exception.
                if isinstance(header_val, bytes):
                    return [header_val.decode("utf-8")]
                return [str(header_val)]
        return None

    def keys(self, carrier: list) -> list[str]:
        if not carrier:
            return []
        return [
            header_key.decode("utf-8")
            if isinstance(header_key, bytes)
            else str(header_key)
            for header_key, _ in carrier
        ]


kafka_getter = KafkaHeaderGetter()


def initialize_tracer(service_name: str) -> trace.Tracer:
    global _PROVIDER_INITIALIZED

    if _PROVIDER_INITIALIZED or isinstance(trace.get_tracer_provider(), TracerProvider):
        _PROVIDER_INITIALIZED = True
        return trace.get_tracer(service_name)

    resource = Resource.create(
        attributes={"service.name": service_name, "environment": "development"}
    )

    provider = TracerProvider(resource=resource)
    jaeger_endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces"
    )
    exporter = OTLPSpanExporter(endpoint=jaeger_endpoint)

    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    try:
        trace.set_tracer_provider(provider)
        _PROVIDER_INITIALIZED = True
    except ValueError:
        pass

    set_global_textmap(TraceContextTextMapPropagator())
    atexit.register(flush_telemetry_traces)

    return trace.get_tracer(service_name)


def flush_telemetry_traces():
    """Forces the active process global OpenTelemetry Tracer Provider to instantly dump
    its in-memory cache arrays straight over HTTP to the Jaeger container.
    """
    # 🏆 PRODUCTION-GRADE LOOKUP: Natively query the live active process provider instance
    # directly out of OpenTelemetry's central SDK engine cache out-of-band!
    active_provider = trace.get_tracer_provider()

    if active_provider and hasattr(active_provider, "force_flush"):
        try:
            active_provider.force_flush()
            logger.info(
                "✔ [OBSERVABILITY SUCCESS]: OpenTelemetry buffer completely flushed to Jaeger container."
            )
        except Exception as flush_err:
            logger.warning(f"Telemetry buffer flush intercept fault: {str(flush_err)}")


# =========================================================================
# 🏆 AUTOMATIC FLUSHING ASYNC KAFKA MIDDLEWARE INTERCEPTOR
# =========================================================================
@contextmanager
def trace_kafka_message(service_tracer: trace.Tracer, span_name: str, kafka_msg):
    """Context manager that manually extracts remote W3C wire headers, binds
    asynchronous thread contexts, and triggers real-time telemetry cache flushes.
    """
    raw_headers = kafka_msg.headers() or []

    # 1. Extract the active W3C parent trace context dictionary from the wire list
    extracted_context = extract(raw_headers, getter=kafka_getter)

    remote_span = trace.get_current_span(extracted_context)
    remote_span_context = remote_span.get_span_context()

    # Build an active runtime context wrapper that explicitly inherits your remote parent tracking metadata
    if remote_span_context.is_valid:
        runtime_context = trace.set_span_in_context(
            trace.NonRecordingSpan(remote_span_context)
        )
    else:
        runtime_context = context.get_current()

    # Hard-attach the synchronized runtime context straight to the active process thread frame
    token = context.attach(runtime_context)

    try:
        # Start the child span directly bound to the synchronized runtime context layer
        with service_tracer.start_as_current_span(
            span_name, context=runtime_context, kind=trace.SpanKind.SERVER
        ) as span:
            message_key = (
                kafka_msg.key().decode("utf-8")
                if isinstance(kafka_msg.key(), bytes)
                else str(kafka_msg.key())
            )
            span.set_attribute("order.correlation_id", message_key)
            span.set_attribute("kafka.topic", kafka_msg.topic())
            span.set_attribute("kafka.partition", kafka_msg.partition())
            yield span
    finally:
        # Tear down memory references safely
        context.detach(token)

        # Force an instant flush of the telemetry buffer to the Jaeger container database index
        flush_telemetry_traces()
