import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Callable, Optional, Any, Tuple
from pydantic import BaseModel, Field


class Message(BaseModel):
    id: str = Field(default_factory=lambda: f"msg_{uuid.uuid4().hex[:8]}")
    sender: str
    receiver: str  # Agent ID, "Coordinator", "broadcast", or "all"
    type: str  # e.g., HEARTBEAT, RESOURCE_REQUEST, BID_SUBMISSION, CONFLICT_ALERT, QUALITY_HALT
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: str = "MEDIUM"  # CRITICAL, HIGH, MEDIUM, LOW
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: Optional[str] = None


class MessageBus:
    def __init__(self):
        self._queues: Dict[str, asyncio.Queue] = {}
        self._listeners: List[Callable[[Message], Any]] = []
        self._pending_rpc: Dict[str, Tuple[asyncio.Future, str]] = {}
        self._message_history: List[Message] = []
        self._max_history = 200

    def register_agent(self, agent_id: str) -> asyncio.Queue:
        """Register an agent and return its dedicated asyncio Queue."""
        if agent_id not in self._queues:
            self._queues[agent_id] = asyncio.Queue()
        return self._queues[agent_id]

    def unregister_agent(self, agent_id: str):
        """Remove agent queue on crash or shutdown."""
        if agent_id in self._queues:
            del self._queues[agent_id]

    def add_listener(self, callback: Callable[[Message], Any]):
        """Add WebSocket/logger callback to observe all traffic."""
        if callback not in self._listeners:
            self._listeners.append(callback)

    def remove_listener(self, callback: Callable[[Message], Any]):
        if callback in self._listeners:
            self._listeners.remove(callback)

    async def publish(self, message: Message):
        """Publish a message to target agent queue(s) and resolve matching RPC response futures."""
        self._message_history.append(message)
        if len(self._message_history) > self._max_history:
            self._message_history.pop(0)

        # Notify global listeners (WebSockets, DB logger)
        for listener in list(self._listeners):
            try:
                res = listener(message)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                print(f"[MessageBus] Listener error: {e}")

        # Check RPC future resolution
        if message.correlation_id and message.correlation_id in self._pending_rpc:
            fut, req_sender = self._pending_rpc[message.correlation_id]
            if message.sender != req_sender:
                if not fut.done():
                    fut.set_result(message)

        # Route message
        if message.receiver.lower() in ["broadcast", "all"]:
            for agent_id, queue in self._queues.items():
                if agent_id != message.sender:
                    await queue.put(message)
        elif message.receiver in self._queues:
            await self._queues[message.receiver].put(message)

    async def request(self, message: Message, timeout: float = 5.0, max_retries: int = 3) -> Message:
        """Point-to-point RPC request with correlation ID and exponential backoff retry."""
        if not message.correlation_id:
            message.correlation_id = f"rpc_{uuid.uuid4().hex[:8]}"

        for attempt in range(1, max_retries + 1):
            fut = asyncio.get_event_loop().create_future()
            self._pending_rpc[message.correlation_id] = (fut, message.sender)
            try:
                await self.publish(message)
                response = await asyncio.wait_for(fut, timeout=timeout)
                return response
            except asyncio.TimeoutError:
                print(f"[MessageBus] RPC Timeout (Attempt {attempt}/{max_retries}) for correlation_id={message.correlation_id}")
                if attempt == max_retries:
                    raise TimeoutError(f"RPC Request timed out after {max_retries} attempts.")
                await asyncio.sleep(1.0 * (2 ** (attempt - 1)))
            finally:
                self._pending_rpc.pop(message.correlation_id, None)

    def get_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        return [msg.model_dump() for msg in self._message_history[-limit:]]


# Global shared message bus singleton instance
global_bus = MessageBus()
