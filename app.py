import asyncio
import json
import os
import logging
from contextlib import asynccontextmanager
from typing import Set, Dict, Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db import state_db
from broker import global_bus, Message
from agents import ProductionPlanningAgent, QualityControlAgent, MaintenanceAgent, LogisticsAgent
from coordinator import CoordinatorAgent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SmartFactoryApp")

# Load factory initial config
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "factory_config.json")
factory_config = {}
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        factory_config = json.load(f)

state_db.seed_initial_state(factory_config)

# Instantiate Agents
ppa = ProductionPlanningAgent()
qca = QualityControlAgent()
ma = MaintenanceAgent()
la = LogisticsAgent()

agents = {
    "PPA": ppa,
    "QCA": qca,
    "MA": ma,
    "LA": la,
}

coordinator = CoordinatorAgent(agent_instances=agents)

# Connected WebSocket clients
ws_clients: Set[WebSocket] = set()


async def broadcast_ws_event(event_type: str, data: dict):
    """Broadcast JSON payload to all connected WebSockets."""
    dead_clients = set()
    payload = json.dumps({"event": event_type, "data": data})
    for client in list(ws_clients):
        try:
            await client.send_text(payload)
        except Exception:
            dead_clients.add(client)
    for dead in dead_clients:
        ws_clients.discard(dead)


def bus_message_listener(msg: Message):
    """Callback triggered on every published bus message."""
    state_db.log_event(
        msg_id=msg.id,
        sender=msg.sender,
        receiver=msg.receiver,
        msg_type=msg.type,
        priority=msg.priority,
        payload=msg.payload,
        timestamp=msg.timestamp,
    )
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(broadcast_ws_event("MESSAGE_LOG", msg.model_dump()))
    except Exception:
        pass


global_bus.add_listener(bus_message_listener)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing Smart Factory Agent Runtime...")
    await ppa.start()
    await qca.start()
    await ma.start()
    await la.start()
    await coordinator.start()
    logger.info("All 5 Factory Agents are ONLINE and communicating via Pub/Sub Message Bus.")

    yield

    logger.info("Shutting down Smart Factory Agents...")
    await coordinator.stop()
    await ppa.stop()
    await qca.stop()
    await ma.stop()
    await la.stop()


app = FastAPI(title="Multi-Agent Smart Factory API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Request Models ---

class HITLResponseModel(BaseModel):
    decision_id: str
    action: str  # APPROVE, REJECT, OVERRIDE
    notes: Optional[str] = ""


class AgentControlModel(BaseModel):
    action: str  # PAUSE, RESUME, KILL, RESTART


# --- API Endpoints ---

@app.get("/api/status")
def get_status():
    agent_summary = {}
    for agent_id, agent in agents.items():
        coord_status = coordinator.agent_statuses.get(agent_id, {})
        agent_summary[agent_id] = {
            "agent_id": agent_id,
            "role": agent.role,
            "status": agent.status,
            "active_task": agent.active_task_description,
            "last_seen": coord_status.get("last_seen", "N/A"),
        }
    
    agent_summary["Coordinator"] = {
        "agent_id": "Coordinator",
        "role": coordinator.role,
        "status": coordinator.status,
        "active_task": coordinator.active_task_description,
        "last_seen": "Active",
    }

    return {
        "safe_mode": coordinator.safe_mode,
        "agents": agent_summary,
        "pending_decisions_count": len(coordinator.pending_decisions),
        "pending_decisions": list(coordinator.pending_decisions.values()),
        "active_conflicts": coordinator.active_conflicts[-10:],
    }


@app.get("/api/messages")
def get_messages(limit: int = 50):
    return state_db.get_event_logs(limit=limit)


@app.get("/api/audit")
def get_audit(limit: int = 100):
    return state_db.get_audit_trail(limit=limit)


@app.get("/api/audit/export")
def export_audit(format: str = Query("json", regex="^(json|csv)$")):
    if format == "csv":
        csv_data = state_db.export_audit_csv()
        return Response(
            content=csv_data,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=smart_factory_audit_trail.csv"}
        )
    else:
        json_data = state_db.export_audit_json()
        return Response(
            content=json_data,
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=smart_factory_audit_trail.json"}
        )


@app.post("/api/hitl/respond")
async def respond_hitl(body: HITLResponseModel):
    try:
        res = await coordinator.resolve_hitl_decision(body.decision_id, body.action, body.notes or "")
        return {"status": "success", "resolved_decision": res}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/agents/{agent_id}/control")
async def control_agent(agent_id: str, body: AgentControlModel):
    if agent_id not in agents and agent_id != "Coordinator":
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found.")

    agent = agents.get(agent_id)
    act = body.action.upper()

    if act == "PAUSE":
        if agent:
            agent.pause()
        return {"status": "success", "message": f"Agent '{agent_id}' paused."}

    elif act == "RESUME":
        if agent:
            agent.resume()
        return {"status": "success", "message": f"Agent '{agent_id}' resumed."}

    elif act == "KILL":
        if agent:
            await agent.stop()
            coordinator.agent_statuses[agent_id] = {"status": "CRASHED", "active_task": "Manually killed by human operator"}
        return {"status": "success", "message": f"Agent '{agent_id}' process terminated."}

    elif act == "RESTART":
        if agent:
            await agent.stop()
            await agent.start()
            coordinator.agent_statuses[agent_id] = {"status": "ONLINE", "active_task": "Manually restarted"}
        return {"status": "success", "message": f"Agent '{agent_id}' restarted."}

    else:
        raise HTTPException(status_code=400, detail=f"Invalid control action '{body.action}'.")


@app.post("/api/scenario/{scenario_id}")
async def trigger_scenario(scenario_id: int):
    if scenario_id == 1:
        # Scenario 1: Normal Operation
        ppa_agent = agents["PPA"]
        await ppa_agent.process_message(Message(sender="User", receiver="PPA", type="SCHEDULE_PRODUCTION_TRIGGER", payload={}))
        return {"status": "success", "scenario": 1, "description": "Triggered Normal Operation sequence."}

    elif scenario_id == 2:
        # Scenario 2: Resource Conflict (PPA vs MA)
        ppa_agent = agents["PPA"]
        ma_agent = agents["MA"]
        asyncio.create_task(ppa_agent.process_message(Message(sender="User", receiver="PPA", type="SCHEDULE_PRODUCTION_TRIGGER", payload={})))
        asyncio.create_task(ma_agent.process_message(Message(sender="User", receiver="MA", type="TRIGGER_MAINTENANCE_ALERT", payload={"vibration": 7.2})))
        return {"status": "success", "scenario": 2, "description": "Triggered Production vs Maintenance Conflict (CNP Bidding)."}

    elif scenario_id == 3:
        # Scenario 3: Quality Failure -> HITL Prompt
        qca_agent = agents["QCA"]
        await qca_agent.process_message(Message(sender="User", receiver="QCA", type="TRIGGER_DEFECT_SPIKE", payload={"defect_rate": 14.2}))
        return {"status": "success", "scenario": 3, "description": "Triggered Quality Failure defect spike (HITL prompt generated)."}

    elif scenario_id == 4:
        # Scenario 4: Agent Crash & Auto-Recovery
        ma_agent = agents["MA"]
        await ma_agent.stop()
        coordinator.agent_statuses["MA"] = {"status": "CRASHED", "active_task": "Process terminated (simulated crash)"}
        return {"status": "success", "scenario": 4, "description": "Killed Maintenance Agent process. Coordinator health monitor will detect timeout and auto-restart."}

    else:
        raise HTTPException(status_code=400, detail=f"Scenario {scenario_id} unknown. Choose 1 to 4.")


# WebSocket Endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    try:
        status_data = {
            "safe_mode": coordinator.safe_mode,
            "agents": {
                "PPA": ppa.to_dict(),
                "QCA": qca.to_dict(),
                "MA": ma.to_dict(),
                "LA": la.to_dict(),
                "Coordinator": coordinator.to_dict(),
            },
            "pending_decisions": list(coordinator.pending_decisions.values()),
            "recent_messages": state_db.get_event_logs(limit=25),
        }
        await websocket.send_text(json.dumps({"event": "INIT_STATE", "data": status_data}))

        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        ws_clients.discard(websocket)
    except Exception:
        ws_clients.discard(websocket)


# Mount static Dashboard files at root '/'
DASHBOARD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard")
if os.path.exists(DASHBOARD_DIR):
    app.mount("/", StaticFiles(directory=DASHBOARD_DIR, html=True), name="dashboard")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
