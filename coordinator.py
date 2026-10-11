import asyncio
import json
import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from agents import BaseAgent
from broker import global_bus, Message
from db import state_db

logger = logging.getLogger("Coordinator")

POLICY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "priority_policy.json")


class PriorityEngine:
    def __init__(self, policy_path: str = POLICY_PATH):
        self.policy_path = policy_path
        self.policy = self._load_policy()

    def _load_policy(self) -> Dict[str, Any]:
        if os.path.exists(self.policy_path):
            with open(self.policy_path, "r") as f:
                return json.load(f)
        return {
            "rules": [],
            "human_in_the_loop": {"enabled": True, "mandatory_approval_events": ["QUALITY_HALT"]}
        }

    def reload_policy(self):
        self.policy = self._load_policy()

    def evaluate_conflict(self, event_type: str, source_agent: str, bids: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Evaluate a conflict between competing bids using the configured priority matrix policy."""
        rules = self.policy.get("rules", [])
        
        matched_rule = None
        for rule in rules:
            if rule.get("target_event") == event_type or rule.get("source_agent") == source_agent:
                matched_rule = rule
                break

        scored_bids = []
        for bid in bids:
            agent_id = bid.get("agent_id")
            urgency = bid.get("urgency_score", 50)
            confidence = bid.get("confidence_score", 0.8)
            bid_val = bid.get("bid_value", 50.0)

            rule_multiplier = 1.0
            if matched_rule and matched_rule.get("source_agent") == agent_id:
                rule_multiplier = 1.8 if matched_rule.get("priority_level") == "CRITICAL" else 1.4

            final_score = (bid_val * rule_multiplier) + (urgency * 0.4) + (confidence * 20.0)
            scored_bids.append({
                "bid": bid,
                "agent_id": agent_id,
                "final_score": round(final_score, 2),
                "urgency": urgency,
                "reasoning": bid.get("reasoning", "")
            })

        scored_bids.sort(key=lambda x: x["final_score"], reverse=True)
        winner = scored_bids[0] if scored_bids else None

        rule_id = matched_rule.get("id") if matched_rule else "RULE_DEFAULT_HIGHEST_SCORE"
        rule_desc = matched_rule.get("description") if matched_rule else "Resolved based on weighted bid scores"

        rationale = (
            f"Rule [{rule_id}] applied: {rule_desc}. "
            f"Winner '{winner['agent_id']}' scored {winner['final_score']} "
            f"(Urgency: {winner['urgency']}, Reason: {winner['reasoning']})."
        )

        requires_hitl = False
        if matched_rule and matched_rule.get("requires_human_approval"):
            requires_hitl = True
        elif event_type in self.policy.get("human_in_the_loop", {}).get("mandatory_approval_events", []):
            requires_hitl = True

        return {
            "winning_agent": winner["agent_id"] if winner else "UNKNOWN",
            "rule_id": rule_id,
            "rule_description": rule_desc,
            "rationale": rationale,
            "winning_bid": winner["bid"] if winner else {},
            "all_scored_bids": scored_bids,
            "requires_human_approval": requires_hitl,
        }


class CoordinatorAgent(BaseAgent):
    def __init__(self, agent_instances: Optional[Dict[str, BaseAgent]] = None):
        super().__init__(agent_id="Coordinator", role="Central Orchestrator & Arbiter", heartbeat_interval=3.0)
        self.priority_engine = PriorityEngine()
        self.agent_instances = agent_instances or {}
        
        self.agent_heartbeats: Dict[str, datetime] = {}
        self.agent_statuses: Dict[str, Dict[str, Any]] = {}
        self.safe_mode: bool = False
        
        self.pending_decisions: Dict[str, Dict[str, Any]] = {}
        self.active_conflicts: List[Dict[str, Any]] = []
        
        self._active_bids: Dict[str, List[Dict[str, Any]]] = {}
        self._health_check_task: Optional[asyncio.Task] = None

    async def start(self):
        await super().start()
        self._health_check_task = asyncio.create_task(self._monitor_agent_health_loop())
        logger.info("[Coordinator] Started health monitor and orchestrator.")

    async def stop(self):
        if self._health_check_task:
            self._health_check_task.cancel()
        await super().stop()

    async def _monitor_agent_health_loop(self):
        """Monitor agent heartbeats every 2 seconds. Detect timeouts > 9 seconds."""
        expected_agents = ["PPA", "QCA", "MA", "LA"]
        while self._running:
            try:
                now = datetime.now(timezone.utc)
                for agent_id in expected_agents:
                    last_hb = self.agent_heartbeats.get(agent_id)
                    if last_hb:
                        elapsed = (now - last_hb).total_seconds()
                        if elapsed > 9.0 and self.agent_statuses.get(agent_id, {}).get("status") != "CRASHED":
                            await self._handle_agent_crash(agent_id, elapsed)
                await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Coordinator] Health monitor error: {e}")

    async def _handle_agent_crash(self, agent_id: str, elapsed_seconds: float):
        """Trigger Safe Default Mode and Auto-Restart for crashed agent."""
        logger.warning(f"[Coordinator] ALERT: Agent {agent_id} missed heartbeats for {elapsed_seconds:.1f}s!")
        self.safe_mode = True
        self.agent_statuses[agent_id] = {
            "status": "CRASHED",
            "active_task": f"CRASHED: Missing heartbeats ({elapsed_seconds:.1f}s)",
            "last_seen": self.agent_heartbeats.get(agent_id, datetime.now(timezone.utc)).isoformat()
        }
        
        state_db.record_audit(
            audit_id=f"audit_crash_{uuid.uuid4().hex[:6]}",
            timestamp=datetime.now(timezone.utc).isoformat(),
            origin="AUTONOMOUS",
            rule_id="RULE_FAILURE_RECOVERY",
            winning_agent="Coordinator",
            summary=f"Detected crash of agent {agent_id} after {elapsed_seconds:.1f}s timeout. Factory set to Safe Default Mode.",
            details={"crashed_agent": agent_id, "elapsed_seconds": elapsed_seconds, "safe_mode": True}
        )

        logger.info(f"[Coordinator] Attempting auto-restart of agent {agent_id}...")
        if agent_id in self.agent_instances:
            agent = self.agent_instances[agent_id]
            await agent.stop()
            await asyncio.sleep(1.0)
            await agent.start()
            self.agent_heartbeats[agent_id] = datetime.now(timezone.utc)
            self.agent_statuses[agent_id]["status"] = "ONLINE"
            self.agent_statuses[agent_id]["active_task"] = "Recovered via Auto-Restart"
            self.safe_mode = False
            logger.info(f"[Coordinator] Agent {agent_id} successfully auto-restarted.")

    async def process_message(self, message: Message):
        if message.type == "HEARTBEAT":
            self.agent_heartbeats[message.sender] = datetime.now(timezone.utc)
            self.agent_statuses[message.sender] = {
                "status": message.payload.get("status", "ONLINE"),
                "active_task": message.payload.get("active_task", "Active"),
                "role": message.payload.get("role", message.sender),
                "last_seen": message.timestamp,
            }

        elif message.type == "BID_SUBMISSION":
            proposal_id = message.payload.get("proposal_id")
            if proposal_id and proposal_id in self._active_bids:
                self._active_bids[proposal_id].append(message.payload)

        elif message.type in ["RESOURCE_REQUEST", "MAINTENANCE_REQUIRED", "QUALITY_HALT", "LOGISTICS_DISPATCH"]:
            await self.handle_incoming_conflict(message)

    async def handle_incoming_conflict(self, message: Message):
        """Initiate Contract-Net Protocol bidding for incoming resource request or alert."""
        proposal_id = f"prop_{uuid.uuid4().hex[:8]}"
        resource = message.payload.get("resource", message.payload.get("line_id", "LINE_01"))
        
        self._active_bids[proposal_id] = []

        rfp_msg = Message(
            sender=self.agent_id,
            receiver="broadcast",
            type="CALL_FOR_BIDS",
            payload={
                "proposal_id": proposal_id,
                "resource": resource,
                "event_type": message.type,
                "source_agent": message.sender,
                "details": message.payload,
            },
            priority="HIGH",
        )
        await global_bus.publish(rfp_msg)

        await asyncio.sleep(1.2)
        bids = self._active_bids.pop(proposal_id, [])

        if not any(b.get("agent_id") == message.sender for b in bids):
            bids.append({
                "agent_id": message.sender,
                "proposal_id": proposal_id,
                "resource": resource,
                "confidence_score": 0.85,
                "urgency_score": 75 if message.priority in ["HIGH", "CRITICAL"] else 40,
                "bid_value": 70.0,
                "reasoning": message.payload.get("reason", "Initiated request"),
            })

        eval_result = self.priority_engine.evaluate_conflict(message.type, message.sender, bids)
        
        conflict_entry = {
            "conflict_id": proposal_id,
            "timestamp": message.timestamp,
            "event_type": message.type,
            "source_agent": message.sender,
            "resource": resource,
            "bids_received": bids,
            "evaluation": eval_result,
        }
        self.active_conflicts.append(conflict_entry)
        if len(self.active_conflicts) > 20:
            self.active_conflicts.pop(0)

        if eval_result.get("requires_human_approval"):
            decision_id = f"hitl_{uuid.uuid4().hex[:8]}"
            pending_item = {
                "decision_id": decision_id,
                "proposal_id": proposal_id,
                "event_type": message.type,
                "source_agent": message.sender,
                "resource": resource,
                "winning_agent": eval_result["winning_agent"],
                "rule_id": eval_result["rule_id"],
                "rationale": eval_result["rationale"],
                "status": "PENDING_APPROVAL",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": message.payload,
            }
            self.pending_decisions[decision_id] = pending_item
            
            await global_bus.publish(Message(
                sender=self.agent_id,
                receiver="broadcast",
                type="HITL_REQUIRED_ALERT",
                payload=pending_item,
                priority="CRITICAL"
            ))
            logger.info(f"[Coordinator] HITL decision created: {decision_id} ({message.type})")
        else:
            await self._execute_decision(
                origin="COORDINATOR_ARBITRATED",
                rule_id=eval_result["rule_id"],
                winning_agent=eval_result["winning_agent"],
                summary=eval_result["rationale"],
                details={"proposal_id": proposal_id, "resource": resource, "event_type": message.type}
            )

    async def _execute_decision(self, origin: str, rule_id: str, winning_agent: str, summary: str, details: Dict[str, Any]):
        audit_id = f"audit_{uuid.uuid4().hex[:8]}"
        state_db.record_audit(
            audit_id=audit_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            origin=origin,
            rule_id=rule_id,
            winning_agent=winning_agent,
            summary=summary,
            details=details
        )
        
        await global_bus.publish(Message(
            sender=self.agent_id,
            receiver="broadcast",
            type="DECISION_ANNOUNCEMENT",
            payload={
                "audit_id": audit_id,
                "origin": origin,
                "winning_agent": winning_agent,
                "rule_id": rule_id,
                "summary": summary,
                "details": details
            },
            priority="HIGH"
        ))

    async def resolve_hitl_decision(self, decision_id: str, action: str, notes: str = "") -> Dict[str, Any]:
        """Process Human-In-The-Loop response from Dashboard operator."""
        if decision_id not in self.pending_decisions:
            raise KeyError(f"Decision ID '{decision_id}' not found.")

        item = self.pending_decisions.pop(decision_id)
        origin = "HUMAN_APPROVED" if action == "APPROVE" else "HUMAN_OVERRIDDEN"
        summary = f"Human Operator [{action}]: {item['rationale']} (Notes: '{notes}')"
        
        item["status"] = action
        item["resolved_at"] = datetime.now(timezone.utc).isoformat()

        if action in ["APPROVE", "OVERRIDE"]:
            await self._execute_decision(
                origin=origin,
                rule_id=item["rule_id"],
                winning_agent=item["winning_agent"],
                summary=summary,
                details={"decision_id": decision_id, "action": action, "notes": notes, "original_payload": item["details"]}
            )
            if item["event_type"] == "QUALITY_HALT":
                if "PPA" in self.agent_instances:
                    await self.agent_instances["PPA"].process_message(
                        Message(sender="Coordinator", receiver="PPA", type="PRODUCTION_PAUSE", payload={"line_id": item["resource"]})
                    )
        else:
            state_db.record_audit(
                audit_id=f"audit_rej_{uuid.uuid4().hex[:6]}",
                timestamp=datetime.now(timezone.utc).isoformat(),
                origin="HUMAN_OVERRIDDEN",
                rule_id=item["rule_id"],
                winning_agent="Operator",
                summary=f"Human Operator REJECTED decision for {item['event_type']} on {item['resource']}.",
                details={"decision_id": decision_id, "notes": notes}
            )

        return item
