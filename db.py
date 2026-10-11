import sqlite3
import json
import csv
import io
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "factory_state.db")


class StateManager:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS machines (
                    id TEXT PRIMARY KEY,
                    line_id TEXT,
                    type TEXT,
                    health_score REAL,
                    maintenance_due INTEGER,
                    status TEXT,
                    updated_at TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS production_batches (
                    id TEXT PRIMARY KEY,
                    line_id TEXT,
                    target_units INTEGER,
                    produced_units INTEGER,
                    defect_count INTEGER,
                    status TEXT,
                    scheduled_start TEXT,
                    completed_at TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS event_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg_id TEXT,
                    sender TEXT,
                    receiver TEXT,
                    type TEXT,
                    priority TEXT,
                    payload TEXT,
                    timestamp TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_trail (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT,
                    origin TEXT,
                    rule_id TEXT,
                    winning_agent TEXT,
                    decision_summary TEXT,
                    details_json TEXT
                )
            """)
            conn.commit()

    def seed_initial_state(self, factory_config: Dict[str, Any]):
        """Seed machine and initial batch state from config if empty."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM machines")
            if cursor.fetchone()[0] == 0:
                now = datetime.now(timezone.utc).isoformat()
                for mch in factory_config.get("machines", []):
                    cursor.execute(
                        "INSERT INTO machines VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            mch["id"],
                            mch["line_id"],
                            mch["type"],
                            mch["health_score"],
                            1 if mch.get("maintenance_due") else 0,
                            "ONLINE",
                            now,
                        ),
                    )
                cursor.execute(
                    "INSERT INTO production_batches VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "BATCH_101",
                        "LINE_01",
                        1000,
                        350,
                        2,
                        "IN_PROGRESS",
                        now,
                        None,
                    ),
                )
                conn.commit()

    def log_event(self, msg_id: str, sender: str, receiver: str, msg_type: str, priority: str, payload: Dict[str, Any], timestamp: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO event_logs (msg_id, sender, receiver, type, priority, payload, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (msg_id, sender, receiver, msg_type, priority, json.dumps(payload), timestamp),
            )
            conn.commit()

    def record_audit(
        self,
        audit_id: str,
        timestamp: str,
        origin: str,
        rule_id: Optional[str],
        winning_agent: str,
        summary: str,
        details: Dict[str, Any],
    ):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO audit_trail VALUES (?, ?, ?, ?, ?, ?, ?)",
                (audit_id, timestamp, origin, rule_id or "N/A", winning_agent, summary, json.dumps(details)),
            )
            conn.commit()

    def get_audit_trail(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM audit_trail ORDER BY timestamp DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                result.append({
                    "id": row["id"],
                    "timestamp": row["timestamp"],
                    "origin": row["origin"],
                    "rule_id": row["rule_id"],
                    "winning_agent": row["winning_agent"],
                    "decision_summary": row["decision_summary"],
                    "details": json.loads(row["details_json"]) if row["details_json"] else {},
                })
            return result

    def get_event_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM event_logs ORDER BY id DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                result.append({
                    "id": row["id"],
                    "msg_id": row["msg_id"],
                    "sender": row["sender"],
                    "receiver": row["receiver"],
                    "type": row["type"],
                    "priority": row["priority"],
                    "payload": json.loads(row["payload"]) if row["payload"] else {},
                    "timestamp": row["timestamp"],
                })
            return result

    def export_audit_csv(self) -> str:
        logs = self.get_audit_trail(limit=500)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Audit ID", "Timestamp", "Origin", "Rule ID", "Winning Agent", "Summary", "Details"])
        for row in logs:
            writer.writerow([
                row["id"],
                row["timestamp"],
                row["origin"],
                row["rule_id"],
                row["winning_agent"],
                row["decision_summary"],
                json.dumps(row["details"]),
            ])
        return output.getvalue()

    def export_audit_json(self) -> str:
        logs = self.get_audit_trail(limit=500)
        return json.dumps(logs, indent=2)


# Global state manager instance
state_db = StateManager()
