# T-Shirt Enterprise Platform (Orchestrated Saga Mesh)

A reference architecture demonstrating an asynchronous, event-driven platform utilizing the Orchestrated Saga Pattern. This system guarantees eventual consistency across decoupled microservices using strictly-typed data contracts, transactional outboxes, and state-machine business graphs.

## Core Architectural Guardrails

*   **Strict Data Governance**: All microservice communication hops are validated via Apache Avro against the centralized schema registry using explicit, deeply nested record types.
*   **Transactional Outbox Pattern**: Application nodes execute dual-writes to local state and private outbox tables inside a single, atomic SQL transaction block. Out-of-band background daemons stream messages onto Kafka.
*   **Decoupled Worker Grid**: Microservices are completely sandboxed. They possess unique relational schemas, separate database connections, and consume from private, dedicated command queues.
*   **Automated Compensation Rollbacks**: The central conductor initiates targeted cancellation routines across the mesh to restore system-wide state parity out-of-band upon downstream business or compliance failures.

## Repository Topology

The workspace follows an enterprise `src/` directory layout to isolate production source code from configuration and verification boundaries.

```text
tshirt_enterprise_platform/
├── platform_infra/         # Docker Compose configurations (Kafka, Postgres, Apicurio)
├── schemas/                # Global Single Source of Truth Avro Contracts
│   ├── command_envelope.avsc
│   └── saga_reply.avsc
├── sales/                  # The Saga Conductor Domain
│   ├── src/sales/          # Isolated package modules (app, db, orchestrator)
│   └── tests/              # Independent, asynchronous integration test matrix
├── shipping/               # Fulfillment & Logistics Domain
│   ├── src/shipping/       # Enforces LangGraph compliance state nodes
│   └── tests/
├── finance/                # Financial Auditing & Fraud Domain
│   ├── src/finance/        # LangGraph risk profile graphs
│   └── tests/
├── notifications/          # Customer Communications Domain
│   ├── src/notifications/
│   └── tests/
├── pyproject.toml          # Shared uv Workspace umbrella module linkages
└── Makefile                # Master global platform automation control panel
```

## Local Development Setup

The project leverages `uv` workspaces to register sub-projects as editable local developer packages, preventing namespace cross-pollution.

### Initialization & Verification
Run the master bootstrap target from the parent repository root folder. This spins up the backend infrastructure, synchronizes the workspace environment, and executes the entire global integration testing matrix:

```bash
make bootstrap
```

### Global Testing Matrix
Every integration test case spawns an independent, non-blocking asynchronous background listener thread running the real production application loops. To execute the global aggregated integration test suite and view a single unified summary pass row, run:

```bash
make test-all
```

## Infrastructure Configuration
*   **Kafka Broker**: `localhost:9092`
*   **Apicurio Schema Registry**: `http://localhost:8081` (ccompat v7 API layer)
*   **PostgreSQL Engine DB**: `postgresql://platform_admin:admin_secure_password@localhost:5432/platform_shared_ledger`
