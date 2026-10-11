# Multi-Agent Smart Factory — System Architecture & Viva Presentation Reference

This document provides a concise 1-page architectural breakdown of the **Multi-Agent Smart Factory System**, mapped directly to the five key criteria required for academic evaluation and viva presentation defense.

---

## 1. Communication Architecture
- **In-Process Pub/Sub Message Bus**: Built using Python's `asyncio.Queue` primitives. Provides asynchronous, non-blocking message distribution without requiring external middleware heavy infrastructure (like Kafka or RabbitMQ), allowing local execution.
- **Standardized JSON Schema**: All agents adhere to a strict 7-field JSON contract:
  $$\text{Message} = \{ \text{id}, \text{sender}, \text{receiver}, \text{type}, \text{payload}, \text{priority}, \text{timestamp}, \text{correlation\_id} \}$$
- **Routing & RPC Capabilities**:
  - **Broadcast**: Messages with `receiver: "broadcast"` or `"all"` are fan-out distributed to all active agent queues.
  - **Direct RPC**: Inter-agent request-response pairs use `correlation_id` matching backed by `asyncio.Future` promises with exponential retry backoff ($1\text{s}, 2\text{s}, 4\text{s}$).

---

## 2. Coordination & Contract-Net Protocol (CNP)
- **Central Coordinator Arbiter**: All cross-agent decisions with potential resource conflicts pass through the Coordinator Agent.
- **Bidding & Negotiation Workflow**:
  1. **Call for Bids (RFP)**: Coordinator broadcasts `CALL_FOR_BIDS` with requested resource (e.g. `LINE_01`) and event context.
  2. **Bid Generation**: Agents compute bids containing confidence score, urgency score, economic value, and textual rationale.
  3. **Score Evaluation**: Bids are scored using a weighted algorithm:
     $$\text{Score} = (\text{BidValue} \times \text{RuleMultiplier}) + (\text{Urgency} \times 0.4) + (\text{Confidence} \times 20)$$
  4. **Contract Award**: Coordinator selects the highest-scoring bid and broadcasts `DECISION_ANNOUNCEMENT`.

---

## 3. Priority Policy Matrix
- **Configurable Rules (`config/priority_policy.json`)**: Conflict resolution is deterministically driven by a external JSON configuration policy:
  - **Level 1 (CRITICAL - Score 100)**: Quality Control & Safety Holds (`QCA` Precedence over output).
  - **Level 2 (HIGH - Score 80)**: Critical Emergency Maintenance (`MA` Lockout before bearing shear).
  - **Level 3 (MEDIUM - Score 60)**: Production Batch Output Targets (`PPA` Schedule).
  - **Level 4 (LOW - Score 40)**: Logistics & Dock Scheduling (`LA` Shipping alignment).
- **Explainability**: Every decision records the exact `rule_id`, matched criteria, winning bid, and human-readable explanation in the audit log.

---

## 4. Failure Recovery & Health Monitoring
- **Heartbeat Protocol**: Each agent emits a `HEARTBEAT` message every 3.0 seconds to the Coordinator.
- **Timeout Detection & Safe Default Mode**:
  - If an agent misses heartbeats for $>9.0\text{s}$, the Coordinator flags the agent as `CRASHED`.
  - The factory immediately transitions to **Safe Default Mode** (pausing new job commitments).
  - An automated recovery task terminates the hung worker task, re-instantiates the agent class, and restores normal status automatically.

---

## 5. Human Oversight & Auditability
- **Human-in-the-Loop (HITL) Triggers**: High-stakes decisions (e.g. `QUALITY_HALT`, `EMERGENCY_SHUTDOWN`) are configured with `requires_human_approval: true`.
- **Interactive Action Center**: When triggered, the Coordinator pauses execution and pushes a real-time banner to the Web Dashboard. Human operators can click **Approve**, **Reject**, or **Override**.
- **Traceable Audit Trail**: Every decision is stored in SQLite (`audit_trail` table) with origin tags (`AUTONOMOUS`, `COORDINATOR_ARBITRATED`, `HUMAN_APPROVED`, `HUMAN_OVERRIDDEN`), searchable, and exportable via CSV and JSON endpoints.
