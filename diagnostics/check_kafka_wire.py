import logging
import sys

from confluent_kafka import Consumer, TopicPartition

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("WIRE_SNIFFER")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def sniff_shipping_topic_directly():
    """Independent read-only sniffer that uses explicit partition assignment

    and an explicit earliest reset flag to read raw wire bytes instantly.
    """
    logger.info("📡 Connecting directly to Kafka broker container...")

    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "direct_wire_debugger_group",
        # 🛠️ FIXED: Tell the broker to automatically jump to the earliest offset
        # on direct partition assignments, clearing out the seek state fault!
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    try:
        consumer = Consumer(config)

        # Manually assign the consumer to Partition 0 of shipping_commands.
        tp = TopicPartition("shipping_commands", 0)
        consumer.assign([tp])

        logger.info("📡 Reading raw wire bytes directly from partition zero...")
        # Give the poll loop plenty of time to fetch the cached backlog bytes
        msg = consumer.poll(4.0)
        consumer.close()

        if msg is None:
            print(
                "\n🚨 [AUDIT RESULT]: Partition wire is empty. The messages were deleted or expired."
            )
            return

        if msg.error():
            print(f"\n💥 Broker Network Fault: {msg.error()}")
            return

        # Success: Message exists on the wire!
        print(
            "\n🟢 [AUDIT RESULT]: Raw message data discovered sitting on the live wire partition!"
        )
        print(
            f"   ├── Message Key: {msg.key().decode('utf-8') if msg.key() else 'None'}"
        )

        # EXTRACT THE RAW HEADERS TO SEE THE TELEMETRY TRACE ID
        raw_headers = msg.headers() or []
        print(f"   └── Wire Metadata Headers Discovered: {len(raw_headers)} entries.")

        for key, val in raw_headers:
            decoded_val = val.decode("utf-8") if isinstance(val, bytes) else str(val)
            print(f"       ├── Header Key: [{key}] ──► Value: [{decoded_val}]")

    except Exception as e:
        logger.error(f"💥 Failed to execute independent wire sniff: {str(e)}")


if __name__ == "__main__":
    sniff_shipping_topic_directly()
