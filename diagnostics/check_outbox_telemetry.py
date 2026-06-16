import logging
import sys

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("TELEMETRY_AUDITOR")

# Direct database connection string pointing to your Docker ledger container
DATABASE_URL = "postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger"


def audit_raw_outbox_bytes():
    """Bypasses all OTel wrappers and queries the Postgres database disk directly

    to check if the trace_context string is actually being persisted at checkout.
    """
    logger.info("🔍 Connecting directly to PostgreSQL container ledger...")
    engine = create_engine(DATABASE_URL)

    query = text("""
        SELECT id, topic, key, substring(payload from 1 for 60) as payload_preview, trace_context 
        FROM outbox 
        ORDER BY id DESC 
        LIMIT 5;
    """)

    try:
        with engine.connect() as conn:
            logger.info("📡 Executing raw SQL inspection scan on table 'outbox'...")
            results = conn.execute(query).fetchall()

            if not results:
                logger.warning(
                    "❓ [EMPTY]: No rows discovered in the sales outbox table."
                )
                logger.info(
                    "💡 Tip: Fire 'uv run simulate_order.py' first, but STOP your daemons so the rows stay in the table!"
                )
                return

            logger.info(
                f"✔ Discovered {len(results)} rows. Printing raw database bytes:"
            )
            logger.info(
                "=========================================================================================="
            )

            for row in results:
                print(f"🆔 Row ID:         {row.id}")
                print(f"📬 Target Topic:   {row.topic}")
                print(f"🔑 Routing Key:    {row.key}")
                print(f"📝 Payload Snap:   {row.payload_preview}...")

                # 🔍 THE CRITICAL TELEMETRY AUDIT
                if row.trace_context:
                    print(f"🟢 [VALID] Trace Context: {row.trace_context}")
                else:
                    print(
                        "🚨 [CRITICAL FAULT] Trace Context: None (Database field is empty or NULL!)"
                    )
                print(
                    "------------------------------------------------------------------------------------------"
                )

    except Exception as db_err:
        logger.error(f"💥 Failed to execute direct database inspection: {str(db_err)}")
        logger.info(
            "💡 Tip: Make sure your postgres container is running by executing 'make services' first!"
        )


if __name__ == "__main__":
    audit_raw_outbox_bytes()
