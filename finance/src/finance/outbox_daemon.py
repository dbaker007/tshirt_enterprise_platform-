import logging

# 🏆 IMPORT THE UNIVERSAL MASTER ENGINE DIRECTLY
from observability.framework.base import OutboxDaemonEngine

# Import your private local finance data tracking assets
from finance.db import FinanceOutbox, SessionLocal, init_finance_db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == "__main__":
    # 1. Initialize local tables out-of-band
    init_finance_db()

    # 2. Instantiate the concrete engine inline, passing only your structural parameters!
    daemon = OutboxDaemonEngine(
        service_name="finance-auditing-service",
        daemon_name="Finance_Outbox",
        client_id="finance_reply_outbox_daemon",
        session_factory=SessionLocal,
        outbox_model=FinanceOutbox,
        poll_interval=2.0,
    )

    # 3. Fire the infinite polling loop
    daemon.start_polling_loop()
