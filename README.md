# Multi-Agent Smart Factory System 🏭

A simplified, fully runnable, production-quality implementation of an autonomous **Multi-Agent Smart Factory System** designed for college case study presentation and viva defense.

The system demonstrates all **5 core concepts**:
1. **Communication**: Asynchronous pub/sub messaging bus and point-to-point RPC (`broker.py`).
2. **Coordination**: Contract-Net Protocol (CNP) bidding (`coordinator.py`).
3. **Priorities**: Configurable priority engine matrix (`config/priority_policy.json`).
4. **Failure Recovery**: 3s heartbeats, timeout crash detection, and auto-restart (`coordinator.py`).
5. **Human Oversight**: Real-time Human-in-the-Loop (HITL) approval modal and SQLite audit trail (`db.py`, `dashboard/`).

---

## 📁 Simplified Core File Overview (Easy to Memorize!)

```
AIML NEXAS/
├── broker.py            # Async Pub/Sub Message Bus & RPC correlation routing
├── db.py                # SQLite database (machines, batches, event logs, audit trail)
├── agents.py            # All 4 domain agents (PPA, QCA, MA, LA) & BaseAgent class
├── coordinator.py       # Central Coordinator, Priority Engine & Health Monitor
├── app.py               # FastAPI server, REST routes, WebSockets & Dashboard host
├── simulate.py          # Terminal-based 4-scenario live presentation runner
├── test_system.py       # Single-file automated verification test suite
├── run.sh               # Single setup & launch command
├── config/              # priority_policy.json & factory_config.json
└── dashboard/           # Glassmorphic dark-mode web dashboard (HTML, CSS, JS)
```

### Quick Reference for Viva Presentation:
- **`broker.py`**: Handles message schema and pub/sub routing between agents.
- **`db.py`**: Manages SQLite storage and audit log exports (`CSV`/`JSON`).
- **`agents.py`**: Contains `ProductionPlanningAgent` (PPA), `QualityControlAgent` (QCA), `MaintenanceAgent` (MA), and `LogisticsAgent` (LA).
- **`coordinator.py`**: Contains `PriorityEngine` and `CoordinatorAgent` to resolve conflicts via bidding and handle heartbeat timeouts.
- **`app.py`**: Starts the FastAPI web server, WebSocket live stream, and dashboard at `http://localhost:8000`.

---

## 🚀 How to Run

### Option 1: Live Web Dashboard Launcher (Recommended)
```bash
./run.sh
```
*Automatically sets up `venv`, installs dependencies, runs verification tests, and opens the dashboard at [http://localhost:8000](http://localhost:8000).*

### Option 2: Terminal Scripted Simulator
```bash
python simulate.py
```
*Executes all 4 viva demo scenarios in colored terminal output.*

### Option 3: Automated Test Verification
```bash
pytest -v test_system.py
```

---

## 🎭 The 4 Viva Demo Scenarios

1. **Scenario 1: Normal Operation**: Routine batch scheduling and material stock RPC check.
2. **Scenario 2: Resource Conflict**: Line access clash (Production vs Maintenance) resolved via Contract-Net bidding.
3. **Scenario 3: Quality Failure**: Defect rate spike (14.5%) generating a live **Human-in-the-Loop approval modal**.
4. **Scenario 4: Agent Crash & Recovery**: Kills Maintenance Agent process; Coordinator detects missing heartbeats and auto-restarts the agent.
