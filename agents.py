import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from broker import global_bus, Message

logger = logging.getLogger("Agents")


class BaseAgent:
    def __init__(self, agent_id: str, role: str, heartbeat_interval: float = 3.0):
        self.agent_id = agent_id
        self.role = role
        self.heartbeat_interval = heartbeat_interval
        self.status = "OFFLINE"  # ONLINE, BUSY, PAUSED, OFFLINE, CRASHED
        self._queue: Optional[asyncio.Queue] = None
        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        self.active_task_description: str = "Idle"

    async def start(self):
        """Start agent background loops."""
        if self._running:
            return
        self._running = True
        self.status = "ONLINE"
        self._queue = global_bus.register_agent(self.agent_id)
        self._main_task = asyncio.create_task(self._listen_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"[{self.agent_id}] Agent started.")

    async def stop(self):
        """Stop agent background loops."""
        self._running = False
        self.status = "OFFLINE"
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._main_task:
            self._main_task.cancel()
        global_bus.unregister_agent(self.agent_id)
        logger.info(f"[{self.agent_id}] Agent stopped.")

    def pause(self):
        self.status = "PAUSED"
        self.active_task_description = "Paused by operator"

    def resume(self):
        self.status = "ONLINE"
        self.active_task_description = "Active"

    async def _heartbeat_loop(self):
        while self._running:
            try:
                if self.status != "OFFLINE":
                    hb_msg = Message(
                        sender=self.agent_id,
                        receiver="Coordinator",
                        type="HEARTBEAT",
                        payload={
                            "role": self.role,
                            "status": self.status,
                            "active_task": self.active_task_description,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                        priority="LOW",
                    )
                    await global_bus.publish(hb_msg)
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.agent_id}] Heartbeat error: {e}")
                await asyncio.sleep(self.heartbeat_interval)

    async def _listen_loop(self):
        while self._running:
            try:
                if not self._queue:
                    await asyncio.sleep(0.5)
                    continue
                msg: Message = await self._queue.get()
                if self.status == "PAUSED":
                    if msg.type == "CONTROL_RESUME":
                        self.resume()
                    self._queue.task_done()
                    continue

                await self.process_message(msg)
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.agent_id}] Error processing message: {e}")

    async def process_message(self, message: Message):
        """Override in subclasses to handle specific message types."""
        pass

    async def send_message(self, receiver: str, msg_type: str, payload: Dict[str, Any], priority: str = "MEDIUM", correlation_id: Optional[str] = None):
        msg = Message(
            sender=self.agent_id,
            receiver=receiver,
            type=msg_type,
            payload=payload,
            priority=priority,
            correlation_id=correlation_id,
        )
        await global_bus.publish(msg)

    async def request_rpc(self, receiver: str, msg_type: str, payload: Dict[str, Any], timeout: float = 5.0, max_retries: int = 3) -> Message:
        msg = Message(
            sender=self.agent_id,
            receiver=receiver,
            type=msg_type,
            payload=payload,
            priority="HIGH",
        )
        return await global_bus.request(msg, timeout=timeout, max_retries=max_retries)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "role": self.role,
            "status": self.status,
            "active_task": self.active_task_description,
        }


class ProductionPlanningAgent(BaseAgent):
    def __init__(self, heartbeat_interval: float = 3.0):
        super().__init__(agent_id="PPA", role="Production Planning Agent", heartbeat_interval=heartbeat_interval)
        self.target_batch = "BATCH_101"
        self.target_units = 1000
        self.produced_units = 350

    async def process_message(self, message: Message):
        if message.type == "CALL_FOR_BIDS":
            payload = message.payload
            resource = payload.get("resource", "LINE_01")
            bid_payload = {
                "agent_id": self.agent_id,
                "proposal_id": payload.get("proposal_id"),
                "resource": resource,
                "confidence_score": 0.88,
                "urgency_score": 60,
                "estimated_benefit": "Produce 650 units of high-demand BATCH_101",
                "bid_value": 75.0,
                "reasoning": "High commercial priority order with pending delivery schedule",
            }
            await self.send_message(
                receiver="Coordinator",
                msg_type="BID_SUBMISSION",
                payload=bid_payload,
                correlation_id=message.correlation_id,
            )

        elif message.type == "SCHEDULE_PRODUCTION_TRIGGER":
            self.status = "BUSY"
            self.active_task_description = f"Scheduling high-volume run for {self.target_batch} on Line 1"
            await self.send_message(
                receiver="Coordinator",
                msg_type="RESOURCE_REQUEST",
                payload={
                    "request_type": "LINE_ACCESS",
                    "resource": "LINE_01",
                    "batch_id": self.target_batch,
                    "speed": "MAX_SPEED",
                    "units_requested": 650,
                },
                priority="MEDIUM",
            )
            await asyncio.sleep(2)
            self.status = "ONLINE"
            self.active_task_description = f"Monitoring batch {self.target_batch} (350/1000 completed)"

        elif message.type == "PRODUCTION_PAUSE":
            self.pause()
            self.active_task_description = f"Production paused on {message.payload.get('line_id', 'LINE_01')} due to directive"


class QualityControlAgent(BaseAgent):
    def __init__(self, heartbeat_interval: float = 3.0):
        super().__init__(agent_id="QCA", role="Quality Control Agent", heartbeat_interval=heartbeat_interval)
        self.defect_threshold_percent = 5.0
        self.current_defect_rate = 0.5

    async def process_message(self, message: Message):
        if message.type == "CALL_FOR_BIDS":
            payload = message.payload
            resource = payload.get("resource", "LINE_01")
            urgency = 100 if self.current_defect_rate > self.defect_threshold_percent else 30
            bid_payload = {
                "agent_id": self.agent_id,
                "proposal_id": payload.get("proposal_id"),
                "resource": resource,
                "confidence_score": 0.99,
                "urgency_score": urgency,
                "estimated_benefit": "Ensure zero defective shipments & line compliance",
                "bid_value": 100.0 if urgency == 100 else 40.0,
                "reasoning": f"Quality inspection report (Defect rate: {self.current_defect_rate:.1f}%)",
            }
            await self.send_message(
                receiver="Coordinator",
                msg_type="BID_SUBMISSION",
                payload=bid_payload,
                correlation_id=message.correlation_id,
            )

        elif message.type == "TRIGGER_DEFECT_SPIKE":
            self.current_defect_rate = message.payload.get("defect_rate", 12.5)
            self.status = "BUSY"
            self.active_task_description = f"CRITICAL: Defect spike detected ({self.current_defect_rate}%) on Line 1"
            
            await self.send_message(
                receiver="Coordinator",
                msg_type="QUALITY_HALT",
                payload={
                    "line_id": message.payload.get("line_id", "LINE_01"),
                    "defect_rate": self.current_defect_rate,
                    "batch_id": "BATCH_101",
                    "action_required": "HALT_LINE",
                    "reason": f"Defect rate ({self.current_defect_rate}%) exceeded safety threshold ({self.defect_threshold_percent}%)",
                },
                priority="CRITICAL",
            )

        elif message.type == "RESET_QUALITY_BASELINE":
            self.current_defect_rate = 0.4
            self.status = "ONLINE"
            self.active_task_description = "Monitoring sensor quality metrics (Normal: 0.4%)"


class MaintenanceAgent(BaseAgent):
    def __init__(self, heartbeat_interval: float = 3.0):
        super().__init__(agent_id="MA", role="Maintenance Agent", heartbeat_interval=heartbeat_interval)
        self.vibration_level = 2.1
        self.temp_c = 58.5
        self.critical_vibration_threshold = 5.0

    async def process_message(self, message: Message):
        if message.type == "CALL_FOR_BIDS":
            payload = message.payload
            resource = payload.get("resource", "LINE_01")
            urgency = 90 if self.vibration_level > self.critical_vibration_threshold else 45
            bid_payload = {
                "agent_id": self.agent_id,
                "proposal_id": payload.get("proposal_id"),
                "resource": resource,
                "confidence_score": 0.95,
                "urgency_score": urgency,
                "estimated_benefit": "Prevent $45k spindle bearing destruction & 12hr downtime",
                "bid_value": 90.0 if urgency == 90 else 55.0,
                "reasoning": f"Telemetry vibration status ({self.vibration_level} mm/s, Temp: {self.temp_c}°C)",
            }
            await self.send_message(
                receiver="Coordinator",
                msg_type="BID_SUBMISSION",
                payload=bid_payload,
                correlation_id=message.correlation_id,
            )

        elif message.type == "TRIGGER_MAINTENANCE_ALERT":
            self.vibration_level = message.payload.get("vibration", 6.8)
            self.temp_c = message.payload.get("temperature", 85.0)
            self.status = "BUSY"
            self.active_task_description = f"HIGH ALERT: Vibration high ({self.vibration_level} mm/s) on Robotic Spindle MCH_02"
            
            await self.send_message(
                receiver="Coordinator",
                msg_type="MAINTENANCE_REQUIRED",
                payload={
                    "line_id": message.payload.get("line_id", "LINE_01"),
                    "machine_id": "MCH_02",
                    "vibration": self.vibration_level,
                    "temperature": self.temp_c,
                    "urgency": "HIGH",
                    "requested_downtime_minutes": 30,
                    "reason": "Preventative replacement of bearing assembly before structural shear",
                },
                priority="HIGH",
            )

        elif message.type == "RESET_MAINTENANCE_BASELINE":
            self.vibration_level = 2.0
            self.temp_c = 55.0
            self.status = "ONLINE"
            self.active_task_description = "Monitoring machine health telemetry (Green status)"


class LogisticsAgent(BaseAgent):
    def __init__(self, heartbeat_interval: float = 3.0):
        super().__init__(agent_id="LA", role="Logistics Agent", heartbeat_interval=heartbeat_interval)
        self.steel_stock = 1200
        self.truck_scheduled = False

    async def process_message(self, message: Message):
        if message.type == "CALL_FOR_BIDS":
            payload = message.payload
            resource = payload.get("resource", "LINE_01")
            bid_payload = {
                "agent_id": self.agent_id,
                "proposal_id": payload.get("proposal_id"),
                "resource": resource,
                "confidence_score": 0.90,
                "urgency_score": 50 if self.truck_scheduled else 20,
                "estimated_benefit": "Optimize dock throughput & prevent carrier waiting penalty",
                "bid_value": 50.0 if self.truck_scheduled else 30.0,
                "reasoning": f"Dock status (Truck scheduled: {self.truck_scheduled}, Stock: {self.steel_stock} sheets)",
            }
            await self.send_message(
                receiver="Coordinator",
                msg_type="BID_SUBMISSION",
                payload=bid_payload,
                correlation_id=message.correlation_id,
            )

        elif message.type == "SCHEDULE_PICKUP":
            self.truck_scheduled = True
            self.status = "BUSY"
            self.active_task_description = "Transport truck arriving at Dock 3 for BATCH_100 pickup"
            await self.send_message(
                receiver="Coordinator",
                msg_type="LOGISTICS_DISPATCH",
                payload={
                    "dock_id": "DOCK_03",
                    "batch_id": "BATCH_100",
                    "scheduled_time": "IMMEDIATE",
                    "truck_carrier": "Express Freight #402",
                },
                priority="LOW",
            )
            await asyncio.sleep(2)
            self.status = "ONLINE"
            self.active_task_description = "Managing inventory & dock assignments"

        elif message.type == "CHECK_MATERIAL_STOCK":
            required_sheets = message.payload.get("required_units", 500)
            has_stock = self.steel_stock >= required_sheets
            await self.send_message(
                receiver=message.sender,
                msg_type="MATERIAL_STOCK_RESPONSE",
                payload={
                    "available": has_stock,
                    "current_stock": self.steel_stock,
                    "required": required_sheets,
                },
                correlation_id=message.correlation_id,
            )
