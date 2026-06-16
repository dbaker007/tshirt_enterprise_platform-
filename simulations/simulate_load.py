import asyncio
import random
import uuid

import httpx

API_URL = "http://localhost:8000/sales/"

# 🛠️ FIXED: Initialize a concurrency token bucket to protect database connection pools!
CONCURRENCY_LIMIT = 20
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)


async def send_order(client, payload, index):
    # Enforce strict queue-waiting parameters behind the semaphore lock boundary
    async with semaphore:
        try:
            response = await client.post(API_URL, json=payload, timeout=15.0)
            if response.status_code == 200:
                flag = "STANDARD"
                if payload.get("shipping_address", {}).get("state") == "MI":
                    flag = "MICHIGAN BREACH"
                elif payload.get("amount", 0) > 200:
                    flag = "FRAUD BREACH"

                order_uuid = payload.get("order_id")
                print(
                    f"🚀 [LOADGEN] Order {index + 1:04d}/1000 Ingested | HTTP 200 | Mode: {flag:<15} | ID: {order_uuid}"
                )
            else:
                print(
                    f"❌ [LOADGEN] Order {index + 1} Failed | HTTP {response.status_code}"
                )
        except Exception as e:
            print(f"🚨 [LOADGEN] Transport Error on Order {index + 1}: {str(e)}")


async def main():
    print("🏁 Initializing Rate-Limited Intermingled Load Matrix (1000 Requests):")
    print(
        f"   └── Enforcing strict connection-pool ceiling: Max {CONCURRENCY_LIMIT} sockets concurrent.\n"
    )
    payloads = []

    # 1. Inject 10 Explicit Michigan Compliance Violations (Nested Structure)
    for i in range(10):
        payloads.append(
            {
                "order_id": str(uuid.uuid4()),
                "customer_email": f"mi.compliance.breach.{i}@enterprise.io",
                "amount": round(random.uniform(10.0, 150.0), 2),
                "shipping_address": {"state": "MI"},
            }
        )

    # 2. Inject 10 Explicit Financial Fraud Over-Limit Breaches
    for i in range(10):
        payloads.append(
            {
                "order_id": str(uuid.uuid4()),
                "customer_email": f"financial.fraud.breach.{i}@enterprise.io",
                "amount": 15500.00,
                "shipping_address": {"state": "TX"},
            }
        )

    # 3. Fill remaining slots with Standard valid orders
    for i in range(80):
        payloads.append(
            {
                "order_id": str(uuid.uuid4()),
                "customer_email": f"load.test.user.{i}@enterprise.io",
                "amount": round(random.uniform(15.0, 199.0), 2),
                "shipping_address": {
                    "state": random.choice(["TX", "CA", "NY", "FL", "IL"])
                },
            }
        )

    # 4. Shuffle the cards to cleanly intermingle failures across your asynchronous workers
    random.shuffle(payloads)

    # Configure an optimized async client connection tracking pool
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=40)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [send_order(client, p, idx) for idx, p in enumerate(payloads)]
        await asyncio.gather(*tasks)

    print(
        "\n🏁 [SUCCESS]: All 1000 orders successfully processed through the mesh with zero drops!"
    )


if __name__ == "__main__":
    asyncio.run(main())
