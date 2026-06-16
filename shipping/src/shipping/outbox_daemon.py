import logging

# 🏆 IMPORT THE UNIVERSAL MASTER ENGINE DIRECTLY
from observability.framework.base import OutboxDaemonEngine

# Import your private local shipping data tracking assets
from shipping.db import SessionLocal, ShippingOutbox, init_shipping_db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == "__main__":
    # 1. Initialize local tables out-of-band
    init_shipping_db()

    # 2. Instantiate the concrete engine inline, passing only your structural parameters!
    daemon = OutboxDaemonEngine(
        service_name="shipping-fulfillment-service",
        daemon_name="Shipping_Outbox",
        client_id="shipping_reply_outbox_daemon",
        session_factory=SessionLocal,
        outbox_model=ShippingOutbox,
        poll_interval=2.0,
    )

    # 3. Fire the infinite polling loop
    daemon.start_polling_loop()
