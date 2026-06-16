import json
import logging
import sys

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("PLATFORM_AUDITOR")

DB_URL = "postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def check_sales_db_history():
    """PHASE 2 DIRECT CHECK: Connects straight to the PostgreSQL container

    ledger to verify if checkout rows possess well-formed W3C trace contexts.
    """
    logger.info(
        "📡 Scanning PostgreSQL container table 'outbox' for fresh checkout rows..."
    )
    engine = create_engine(DB_URL)

    query = text("""
        SELECT id, topic, key, trace_context 
        FROM outbox 
        ORDER BY id DESC 
        LIMIT 3;
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(query).fetchall()
            if not rows:
                print(
                    "\n❓ [DATABASE EMPTY]: The 'outbox' table exists, but it contains 0 rows."
                )
                print(
                    "💡 Tip: Ensure you executed 'uv run simulate_order.py' first while daemons are OFF!"
                )
                return False

            print(
                "\n================================================================================"
            )
            print("📋 PHASE 2 AUDIT: RAW DATABASE OUTBOX COLUMN DISK CHECK")
            print(
                "================================================================================"
            )
            for row in rows:
                print(f"🆔 Row ID:         {row.id}")
                print(f"📬 Target Topic:   {row.topic}")
                print(f"🔑 Reference Key:  {row.key}")
                if row.trace_context:
                    print(
                        f"🟢 [VALID]: Trace Context String Stored: {row.trace_context}"
                    )
                else:
                    print(
                        "🚨 [CRITICAL TELEMETRY FAULT]: Trace Context field is empty or NULL!"
                    )
                print(
                    "--------------------------------------------------------------------------------"
                )
            return True

    except Exception as db_err:
        logger.error(f"💥 Failed to execute database history scan: {str(db_err)}")
        return False


def sniff_kafka_wire_directly(topic_name: str):
    """PHASE 3 DIRECT CHECK: Uses manual partition assignment to read raw wire bytes

    instantly from Kafka, extraction metadata headers out-of-band to check for the trace ID.
    """
    print(
        "\n--------------------------------------------------------------------------------"
    )
    logger.info(f"📡 Sniffing live Kafka broker partition for topic: [{topic_name}]...")

    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "direct_wire_debugger_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    try:
        consumer = Consumer(config)

        # Explicitly bind to partition zero to bypass group assignment rebalance loops
        tp = TopicPartition(topic_name, 0)
        consumer.assign([tp])

        # Pull the message off the network card partition
        msg = consumer.poll(4.0)
        consumer.close()

        if msg is None:
            print(f"\n🚨 [KAFKA WIRE AUDIT]: Topic '{topic_name}' is completely EMPTY.")
            print(
                "   └── Conclusion: The outbox daemon failed to commit the bytes to the broker network."
            )
            return

        if msg.error():
            print(f"\n💥 Broker Network Fault: {msg.error()}")
            return

        print(
            f"\n🟢 [KAFKA WIRE PASS]: Raw message data discovered sitting on topic partition: [{topic_name}]!"
        )
        print(
            f"   ├── Message Routing Key: {msg.key().decode('utf-8') if msg.key() else 'None'}"
        )

        # Extract the raw wire header tuples list
        raw_headers = msg.headers() or []
        print(f"   └── Wire Metadata Headers Discovered: {len(raw_headers)} entries.")

        has_traceparent = False
        for key, val in raw_headers:
            decoded_val = val.decode("utf-8") if isinstance(val, bytes) else str(val)
            print(f"       ├── Header Key: [{key}] ──► Value: [{decoded_val}]")
            if key.lower() == "traceparent":
                has_traceparent = True

        if has_traceparent:
            print(
                "\n🟢 [TELEMETRY PASS]: The W3C 'traceparent' header IS traveling across the live wire!"
            )
        else:
            print(
                "\n🚨 [TELEMETRY CRITICAL FAULT]: The message on Kafka has NO telemetry headers. It is naked."
            )

    except Exception as kafka_err:
        logger.error(f"💥 Failed to execute raw wire packet sniffer: {str(kafka_err)}")


def check_department_outbox(table_name: str):
    """WORKER CHECK: Queries a specific department's outbox table directly

    to verify if the worker's LangGraph node successfully saved its reply with trace info.
    """
    print(
        "\n--------------------------------------------------------------------------------"
    )
    logger.info(
        f"📡 Querying PostgreSQL container table '{table_name}' for worker reply data..."
    )
    engine = create_engine(DB_URL)

    try:
        # Check if the relation table actually exists in the catalog first
        with engine.connect() as conn:
            table_check = conn.execute(
                text(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');"
                )
            ).scalar()

            if not table_check:
                print(
                    f"\n🚨 [DATABASE FAULT]: Relation table '{table_name}' DOES NOT EXIST on disk."
                )
                print(
                    "   └── Conclusion: The worker crashed, encountered a schema exception, or never executed its nodes."
                )
                return

            # Table exists, query the newest row
            row = conn.execute(
                text(
                    f"SELECT id, topic, key, trace_context FROM {table_name} ORDER BY id DESC LIMIT 1;"
                )
            ).fetchone()

            if not row:
                print(
                    f"\n❓ [DATABASE EMPTY]: Table '{table_name}' exists, but it contains 0 records."
                )
                print(
                    "   └── Conclusion: The worker rolled back its transaction or skipped writing outbox data."
                )
                return

            print(
                f"\n================================================================================"
            )
            print(f"📋 WORKER DATA AUDIT: {table_name.upper()} DISK COLUMNS CHECK")
            print(
                f"================================================================================"
            )
            print(f"🆔 Record Row ID:     {row.id}")
            print(f"📬 Destination Topic: {row.topic}")
            print(f"🔑 Transaction Key:   {row.key}")
            if row.trace_context:
                print(f"🟢 [PASS]: Trace Context String Stored: {row.trace_context}")
            else:
                print(
                    "🚨 [CRITICAL TELEMETRY FAULT]: Row exists, but 'trace_context' column is empty!"
                )

    except Exception as err:
        logger.error(f"💥 Failed to complete worker data audit lookup: {str(err)}")


if __name__ == "__main__":
    print(
        "================================================================================"
    )
    print("🏆 UNIFIED PLATFORM PIPELINE ISOLATION LAYER SNAPSHOT AUDITOR")
    print(
        "================================================================================"
    )

    # Executing arguments router interface mapping
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode == "kafka" and len(sys.argv) > 2:
            sniff_kafka_wire_directly(sys.argv[2])
        elif mode == "worker" and len(sys.argv) > 2:
            check_department_outbox(sys.argv[2])
        else:
            check_sales_db_history()
    else:
        # Standard default strategy pass executes the Phase 2 Database scan
        check_sales_db_history()

    print(
        "================================================================================"
    )
