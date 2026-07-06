import logging
import sys

from fastapi import FastAPI

# 🟢 SOLUTION: Import the standard FastAPI cross-origin resource isolation middleware
from fastapi.middleware.cors import CORSMiddleware
from observability.tracing import initialize_tracer
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from ops_agent.endpoints.agent_routes import agent_router

# Execute global OpenTelemetry trace hooking at the absolute application root layer
tracer = initialize_tracer()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("OPS_AGENT_CORE")

app = FastAPI(
    title="Enterprise Ops Agent Pod",
    description="Synchronous Natural Language Reasoning and Tool Execution Engine Shard",
)

# 🟢 SOLUTION: Configure the allowed origin whitelist contract natively
origins = [
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]

# 🟢 SOLUTION: Bind the security layer directly into the global application runtime thread frame
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=[
        "*"
    ],  # Permits critical OPTIONS/POST browser preflight requests seamlessly
    allow_headers=["*"],
)

# Preserve trace contexts across asynchronous task boundaries natively
FastAPIInstrumentor.instrument_app(app)

# Mount our endpoint routes cleanly into the global application context
app.include_router(agent_router)


@app.get("/health")
async def execution_plane_health_check():
    """Kubernetes liveness and readiness probe handshake checkpoint."""
    return {"status": "HEALTHY", "domain": "OPS_AGENT"}
