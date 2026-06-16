import logging

# 🏆 IMPORT THE UNIVERSAL MASTER ENGINE DIRECTLY
from observability.framework.base import OutboxDaemonEngine

# Import your private local notifications data tracking assets
from notifications.db import NotificationOutbox, SessionLocal, init_notifications_db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == "__main__":
    # 1. Initialize local tables out-of-band
    init_notifications_db()

    # 2. Instantiate the concrete engine inline, passing only your structural parameters!
    daemon = OutboxDaemonEngine(
        service_name="notifications-alert-service",
        daemon_name="Notifications_Outbox",
        client_id="notifications_reply_outbox_daemon",
        session_factory=SessionLocal,
        outbox_model=NotificationOutbox,
        poll_interval=2.0,
    )

    # 3. Fire the infinite polling loop
    daemon.start_polling_loop()
