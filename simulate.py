#!/usr/bin/env python3
"""
Scripted Demo Simulator for Multi-Agent Smart Factory System.
Executes 4 required viva demonstration scenarios sequentially with colored logs.
"""

import asyncio
import sys
import logging
from datetime import datetime, timezone

from broker import global_bus, Message
from db import state_db
from agents import ProductionPlanningAgent, QualityControlAgent, MaintenanceAgent, LogisticsAgent
from coordinator import CoordinatorAgent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


class TerminalColor:
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def print_banner(title: str):
    print("\n" + "=" * 80)
    print(f"{TerminalColor.BOLD}{TerminalColor.CYAN}  {title}{TerminalColor.RESET}")
    print("=" * 80 + "\n")


async def run_simulation():
    print_banner("INITIALIZING MULTI-AGENT SMART FACTORY SYSTEM")

    # Instantiate agents
    ppa = ProductionPlanningAgent()
    qca = QualityControlAgent()
    ma = MaintenanceAgent()
    la = LogisticsAgent()

    agents = {"PPA": ppa, "QCA": qca, "MA": ma, "LA": la}
    coordinator = CoordinatorAgent(agent_instances=agents)

    # Start agents
    await ppa.start()
    await qca.start()
    await ma.start()
    await la.start()
    await coordinator.start()

    print(f"{TerminalColor.GREEN}✔ All 5 agents (PPA, QCA, MA, LA, Coordinator) ONLINE.{TerminalColor.RESET}\n")
    await asyncio.sleep(2)

    # ---------------------------------------------------------
    # SCENARIO 1: Normal Operation
    # ---------------------------------------------------------
    print_banner("SCENARIO 1: NORMAL OPERATION")
    print(f"{TerminalColor.BOLD}1. PPA checks raw material availability with LA via RPC...{TerminalColor.RESET}")
    stock_reply = await ppa.request_rpc("LA", "CHECK_MATERIAL_STOCK", {"required_units": 500})
    print(f"   ➔ LA Stock Reply: Available={stock_reply.payload.get('available')}, Current Stock={stock_reply.payload.get('current_stock')} sheets\n")

    print(f"{TerminalColor.BOLD}2. PPA triggers standard production batch run...{TerminalColor.RESET}")
    await ppa.process_message(Message(sender="Simulator", receiver="PPA", type="SCHEDULE_PRODUCTION_TRIGGER", payload={}))
    await asyncio.sleep(3)

    # ---------------------------------------------------------
    # SCENARIO 2: Resource Conflict (Production vs Maintenance)
    # ---------------------------------------------------------
    print_banner("SCENARIO 2: RESOURCE CONFLICT (PRODUCTION VS MAINTENANCE)")
    print(f"{TerminalColor.YELLOW}PPA requests Line 1 max output while MA flags vibration breach (6.8 mm/s)...{TerminalColor.RESET}")
    
    asyncio.create_task(ppa.process_message(Message(sender="Simulator", receiver="PPA", type="SCHEDULE_PRODUCTION_TRIGGER", payload={})))
    asyncio.create_task(ma.process_message(Message(sender="Simulator", receiver="MA", type="TRIGGER_MAINTENANCE_ALERT", payload={"vibration": 6.8})))

    await asyncio.sleep(3)

    if coordinator.active_conflicts:
        latest_conflict = coordinator.active_conflicts[-1]
        eval_res = latest_conflict.get("evaluation", {})
        print(f"{TerminalColor.GREEN}✔ Coordinator Arbitrated Conflict via Priority Matrix:{TerminalColor.RESET}")
        print(f"   ➔ Rule Matched: {eval_res.get('rule_id')}")
        print(f"   ➔ Winner: {eval_res.get('winning_agent')}")
        print(f"   ➔ Rationale: {eval_res.get('rationale')}\n")

    # ---------------------------------------------------------
    # SCENARIO 3: Quality Failure & Human Oversight (HITL)
    # ---------------------------------------------------------
    print_banner("SCENARIO 3: QUALITY FAILURE & HUMAN OVERSIGHT (HITL)")
    print(f"{TerminalColor.RED}QCA detects 14.5% defect rate on Line 1 -> Issues QUALITY_HALT alert...{TerminalColor.RESET}")
    await qca.process_message(Message(sender="Simulator", receiver="QCA", type="TRIGGER_DEFECT_SPIKE", payload={"defect_rate": 14.5}))

    await asyncio.sleep(2)

    if coordinator.pending_decisions:
        decision_id = list(coordinator.pending_decisions.keys())[0]
        pending_item = coordinator.pending_decisions[decision_id]
        print(f"{TerminalColor.YELLOW}⚠️ PENDING HITL DECISION GENERATED:{TerminalColor.RESET}")
        print(f"   ➔ Decision ID: {decision_id}")
        print(f"   ➔ Event Type: {pending_item['event_type']}")
        print(f"   ➔ Rationale: {pending_item['rationale']}")
        print(f"\n{TerminalColor.BOLD}Simulating Human Operator APPROVAL live...{TerminalColor.RESET}")
        
        resolved = await coordinator.resolve_hitl_decision(decision_id, "APPROVE", notes="Approved line halt via simulator")
        print(f"{TerminalColor.GREEN}✔ Decision {resolved.get('status', 'RESOLVED')}: Audit trail updated.{TerminalColor.RESET}\n")

    # ---------------------------------------------------------
    # SCENARIO 4: Agent Crash & Auto-Recovery
    # ---------------------------------------------------------
    print_banner("SCENARIO 4: AGENT CRASH & AUTO-RECOVERY")
    print(f"{TerminalColor.RED}Simulating sudden crash of Maintenance Agent (MA process killed)...{TerminalColor.RESET}")
    await ma.stop()
    coordinator.agent_statuses["MA"] = {"status": "CRASHED", "active_task": "Process terminated (simulated crash)"}
    
    print(f"{TerminalColor.YELLOW}Triggering Coordinator heartbeat recovery check...{TerminalColor.RESET}")
    await coordinator._handle_agent_crash("MA", elapsed_seconds=9.5)

    await asyncio.sleep(2)
    print(f"{TerminalColor.GREEN}✔ MA Agent status after recovery: {ma.status} ({ma.active_task_description}){TerminalColor.RESET}\n")

    # ---------------------------------------------------------
    # AUDIT TRAIL SUMMARY
    # ---------------------------------------------------------
    print_banner("DEMO SIMULATION COMPLETE — AUDIT TRAIL SUMMARY")
    audits = state_db.get_audit_trail(limit=10)
    for audit in audits:
        print(f" [{audit['timestamp'][:19]}] {audit['origin']} | Rule: {audit['rule_id']} | Winner: {audit['winning_agent']}")
        print(f"   ➔ {audit['decision_summary']}")

    # Cleanup
    await coordinator.stop()
    await ppa.stop()
    await qca.stop()
    await ma.stop()
    await la.stop()


if __name__ == "__main__":
    asyncio.run(run_simulation())
