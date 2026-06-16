import logging

# 🏆 IMPORT THE CONCRETE MASTER ENGINE DIRECTLY
from observability.framework.base import OutboxDaemonEngine

# Import your private local data tracking assets
from sales.db import Outbox, SessionLocal, init_sales_db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == "__main__":
    # 1. Initialize local tables out-of-band
    init_sales_db()

    # 2. Instantiate the concrete engine inline, passing only your structural parameters!
    daemon = OutboxDaemonEngine(
        service_name="sales-outbox-daemon",
        daemon_name="Sales_Outbox",
        client_id="sales_command_outbox_daemon",
        session_factory=SessionLocal,
        outbox_model=Outbox,
        poll_interval=2.0,
    )

    # 3. Fire the infinite polling loop
    daemon.start_polling_loop()
