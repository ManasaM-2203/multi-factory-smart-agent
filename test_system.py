import pytest
import asyncio
from broker import MessageBus, Message
from db import state_db
from agents import ProductionPlanningAgent, QualityControlAgent, MaintenanceAgent, LogisticsAgent
from coordinator import CoordinatorAgent, PriorityEngine


@pytest.mark.asyncio
async def test_message_bus_rpc():
    bus = MessageBus()
    queue = bus.register_agent("LA")

    async def responder():
        msg = await queue.get()
        assert msg.type == "CHECK_MATERIAL_STOCK"
        reply = Message(sender="LA", receiver="PPA", type="STOCK_OK", correlation_id=msg.correlation_id)
        await bus.publish(reply)

    asyncio.create_task(responder())
    req = Message(sender="PPA", receiver="LA", type="CHECK_MATERIAL_STOCK")
    res = await bus.request(req, timeout=2.0)
    assert res.sender == "LA"
    assert res.type == "STOCK_OK"


@pytest.mark.asyncio
async def test_priority_engine_evaluation():
    engine = PriorityEngine()
    bids = [
        {"agent_id": "PPA", "bid_value": 75.0, "urgency_score": 60, "confidence_score": 0.88, "reasoning": "High demand"},
        {"agent_id": "MA", "bid_value": 90.0, "urgency_score": 90, "confidence_score": 0.95, "reasoning": "Vibration breach"},
    ]
    res = engine.evaluate_conflict("MAINTENANCE_REQUIRED", "MA", bids)
    assert res["winning_agent"] == "MA"
    assert res["rule_id"] == "RULE_EMERGENCY_MAINTENANCE"


@pytest.mark.asyncio
async def test_agent_lifecycle_and_hitl():
    ppa = ProductionPlanningAgent()
    qca = QualityControlAgent()
    ma = MaintenanceAgent()
    la = LogisticsAgent()

    agents = {"PPA": ppa, "QCA": qca, "MA": ma, "LA": la}
    coordinator = CoordinatorAgent(agent_instances=agents)

    await ppa.start()
    await qca.start()
    await ma.start()
    await la.start()
    await coordinator.start()

    # Trigger Quality Halt (HITL)
    await qca.process_message(Message(sender="Test", receiver="QCA", type="TRIGGER_DEFECT_SPIKE", payload={"defect_rate": 12.0}))
    await asyncio.sleep(2.0)

    assert len(coordinator.pending_decisions) == 1
    decision_id = list(coordinator.pending_decisions.keys())[0]

    resolved = await coordinator.resolve_hitl_decision(decision_id, "APPROVE", notes="Test Approval")
    assert resolved["status"] == "APPROVE"

    # Clean up
    await coordinator.stop()
    await ppa.stop()
    await qca.stop()
    await ma.stop()
    await la.stop()


@pytest.mark.asyncio
async def test_agent_crash_recovery():
    ma = MaintenanceAgent()
    agents = {"MA": ma}
    coordinator = CoordinatorAgent(agent_instances=agents)

    await ma.start()
    await coordinator.start()

    # Simulate crash
    await ma.stop()
    coordinator.agent_statuses["MA"] = {"status": "CRASHED", "active_task": "Simulated crash"}

    await coordinator._handle_agent_crash("MA", elapsed_seconds=10.0)

    assert ma.status == "ONLINE"
    assert coordinator.safe_mode is False

    await coordinator.stop()
    await ma.stop()
