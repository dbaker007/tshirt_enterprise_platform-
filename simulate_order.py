import logging
import sys
import time
import uuid

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("PLATFORM_SIMULATOR")

GATEWAY_URL = "http://localhost:8000/sales/"


def run_live_checkout_simulation():
    """Fires a live HTTP payload into the active FastAPI server port

    to watch the transaction propagate natively across the Kafka mesh.
    """
    logger.info("🏁 Initializing Live Corporate Checkout Simulation...")
    logger.info("=====================================================")

    # 1. Construct a clean, compliant corporate checkout payload matching our schemas
    address_record = {
        "street": "123 Enterprise Blvd",
        "city": "Austin",
        "state": "TX",  # ◄── Clean state path (will approve shipping)
        "postal_code": "78701",
    }

    checkout_payload = {
        "customer": {
            "name": "Alex Platform Architect",
            "email": f"alex.trace.{uuid.uuid4().hex[:6]}@enterprise.io",
        },
        "amount": 149.99,  # ◄── Under $200 (will clear fraud checks)
        "item_id": "SHIRT_GOLD_LIMITED_XL",
        "shipping_address": address_record,
    }

    try:
        logger.info(f"📤 Posting transaction request to Sales Gateway: {GATEWAY_URL}")
        response = requests.post(GATEWAY_URL, json=checkout_payload, timeout=5.0)

        if response.status_code == 200:
            data = response.json()
            order_id = data.get("order_id")
            logger.info(
                "✔ [SUCCESS]: Transaction ingested smoothly by Sales Gateway API."
            )
            logger.info(f"🆔 Generated Correlation ID (Master Trace Key): {order_id}")
            logger.info("=====================================================")
            logger.info(
                "⏳ Data is now cascading asynchronously across the microservice mesh..."
            )
            logger.info(
                "📝 Check your terminal logs or open Jaeger to watch the bars move live!"
            )
        else:
            logger.error(
                f"❌ Gateway Rejected Transaction! HTTP Status: {response.status_code} | Details: {response.text}"
            )

    except Exception as e:
        logger.critical(
            f"💥 Failed to communicate with live Sales API container: {str(e)}"
        )
        logger.info(
            "💡 Tip: Make sure your services are running by executing 'make services' and 'make daemons' first!"
        )


if __name__ == "__main__":
    run_live_checkout_simulation()
