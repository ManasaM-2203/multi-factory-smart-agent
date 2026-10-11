// Ultra-Clean Smart Factory Live Control Dashboard Application

let socket = null;
let currentPendingHITL = null;

document.addEventListener("DOMContentLoaded", () => {
  initWebSocket();
  fetchStatus();
  fetchAuditLogs();

  document.getElementById("export-csv-btn").addEventListener("click", () => {
    window.open("/api/audit/export?format=csv", "_blank");
  });
  document.getElementById("export-json-btn").addEventListener("click", () => {
    window.open("/api/audit/export?format=json", "_blank");
  });
});

function initWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${protocol}//${window.location.host}/ws`;

  socket = new WebSocket(wsUrl);

  socket.onopen = () => {
    console.log("[Dashboard WS] Connected to Smart Factory Agent Bus.");
  };

  socket.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleWSEvent(msg);
    } catch (e) {
      console.error("[Dashboard WS] Parse error:", e);
    }
  };

  socket.onclose = () => {
    setTimeout(initWebSocket, 3000);
  };
}

function handleWSEvent(msg) {
  if (msg.event === "INIT_STATE") {
    renderSystemState(msg.data);
  } else if (msg.event === "MESSAGE_LOG") {
    appendMessageLog(msg.data);
    fetchStatus();
  } else if (msg.event === "HITL_REQUIRED_ALERT") {
    showHITLBanner(msg.data);
  } else if (msg.event === "DECISION_ANNOUNCEMENT") {
    fetchAuditLogs();
    fetchStatus();
  }
}

async function fetchStatus() {
  try {
    const res = await fetch("/api/status");
    if (!res.ok) return;
    const data = await res.json();
    renderSystemState(data);
  } catch (e) {
    console.error("Error fetching status:", e);
  }
}

async function fetchAuditLogs() {
  try {
    const res = await fetch("/api/audit?limit=30");
    if (!res.ok) return;
    const logs = await res.json();
    renderAuditTable(logs);
  } catch (e) {
    console.error("Error fetching audit logs:", e);
  }
}

function renderSystemState(data) {
  // 1. System Status Badge
  const badge = document.getElementById("system-status-badge");
  if (data.safe_mode) {
    badge.className = "badge badge-warning";
    badge.innerHTML = `<span class="status-dot orange"></span> Safe Default Mode Active`;
  } else {
    badge.className = "badge badge-success";
    badge.innerHTML = `<span class="status-dot green"></span> System Nominal`;
  }

  // 2. Render Agents Grid (5 Cards)
  const agentsGrid = document.getElementById("agents-grid");
  if (data.agents) {
    agentsGrid.innerHTML = "";
    Object.values(data.agents).forEach((agent) => {
      agentsGrid.appendChild(createAgentCard(agent));
    });
  }

  // 3. Render HITL Pending Decision Banner
  if (data.pending_decisions && data.pending_decisions.length > 0) {
    showHITLBanner(data.pending_decisions[0]);
  } else {
    hideHITLBanner();
  }

  // 4. Render Hero Decision Sentence
  if (data.active_conflicts && data.active_conflicts.length > 0) {
    const latest = data.active_conflicts[data.active_conflicts.length - 1];
    document.getElementById("latest-decision-text").innerText = toPlainEnglishSentence(latest);
  }
}

function createAgentCard(agent) {
  const card = document.createElement("div");
  const isCoord = agent.agent_id === "Coordinator";
  card.className = `agent-card ${isCoord ? "coordinator" : ""}`;

  let dotClass = "green";
  if (agent.status === "BUSY") dotClass = "yellow";
  else if (agent.status === "PAUSED") dotClass = "orange";
  else if (agent.status === "CRASHED" || agent.status === "OFFLINE") dotClass = "red";

  const controlsHtml = !isCoord
    ? `<details class="agent-controls-toggle">
         <summary>⚙ Controls</summary>
         <div class="agent-btn-row">
           <button class="btn btn-secondary btn-xs" onclick="controlAgent('${agent.agent_id}', 'PAUSE')">Pause</button>
           <button class="btn btn-secondary btn-xs" onclick="controlAgent('${agent.agent_id}', 'RESUME')">Resume</button>
           <button class="btn btn-danger btn-xs" onclick="controlAgent('${agent.agent_id}', 'KILL')">Crash</button>
           <button class="btn btn-secondary btn-xs" onclick="controlAgent('${agent.agent_id}', 'RESTART')">Restart</button>
         </div>
       </details>`
    : `<div style="font-size:0.75rem; color:var(--text-dim); margin-top:0.4rem;">👑 Orchestrator</div>`;

  card.innerHTML = `
    <div>
      <div class="agent-card-top">
        <span class="agent-name">${isCoord ? "Coordinator" : agent.agent_id}</span>
        <span class="badge" style="padding: 0.2rem 0.6rem; font-size:0.75rem;">
          <span class="status-dot ${dotClass}"></span> ${agent.status}
        </span>
      </div>
      <div class="agent-role">${agent.role || agent.agent_id}</div>
      <div class="agent-task" title="${agent.active_task}">${agent.active_task || "Idle"}</div>
    </div>
    ${controlsHtml}
  `;
  return card;
}

function toPlainEnglishSentence(conflict) {
  if (!conflict) return "System nominal. Select a scenario above to observe multi-agent decision making.";
  const evalRes = conflict.evaluation || {};
  const winner = evalRes.winning_agent || "Coordinator";
  const eventType = conflict.event_type;

  if (eventType === "QUALITY_HALT") {
    return "Quality Control Agent halted Line 1 due to a defect surge — waiting for human approval.";
  }
  if (eventType === "MAINTENANCE_REQUIRED" || winner === "MA") {
    return "Coordinator awarded Line 1 access to Maintenance Agent to prevent machine damage.";
  }
  if (eventType === "RESOURCE_REQUEST" || winner === "PPA") {
    return "Production Planning Agent scheduled high-volume batch BATCH_101 run on Line 1.";
  }
  if (eventType === "LOGISTICS_DISPATCH" || winner === "LA") {
    return "Logistics Agent dispatched truck for finished goods transport at Dock 3.";
  }

  return `Coordinator granted priority to ${winner} for resource ${conflict.resource || "LINE_01"}.`;
}

function showHITLBanner(item) {
  currentPendingHITL = item;
  const banner = document.getElementById("hitl-banner");
  document.getElementById("hitl-title").innerText = `⚠️ Human Oversight Required: Quality Halt`;
  document.getElementById("hitl-desc").innerText = `Critical defect surge detected on Line 1. Approve emergency line halt?`;
  banner.classList.remove("hidden");
}

function hideHITLBanner() {
  currentPendingHITL = null;
  const banner = document.getElementById("hitl-banner");
  banner.classList.add("hidden");
}

async function respondHITL(action) {
  if (!currentPendingHITL) return;
  try {
    const res = await fetch("/api/hitl/respond", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        decision_id: currentPendingHITL.decision_id,
        action: action,
        notes: "Operator action via control dashboard",
      }),
    });
    if (res.ok) {
      hideHITLBanner();
      fetchStatus();
      fetchAuditLogs();
    }
  } catch (e) {
    console.error("Error responding to HITL:", e);
  }
}

function appendMessageLog(msg) {
  const feed = document.getElementById("message-feed");
  const item = document.createElement("div");
  item.className = "msg-line";

  const timeStr = msg.timestamp ? msg.timestamp.split("T")[1].slice(0, 8) : "";
  item.innerText = `[${timeStr}] ${msg.sender} ➔ ${msg.receiver} | ${msg.type}`;

  feed.insertBefore(item, feed.firstChild);
  if (feed.children.length > 20) {
    feed.removeChild(feed.lastChild);
  }
}

function renderAuditTable(logs) {
  const tbody = document.getElementById("audit-table-body");
  if (!logs || logs.length === 0) {
    tbody.innerHTML = `<tr><td colspan="2" class="placeholder-text">No audit history recorded yet.</td></tr>`;
    return;
  }

  tbody.innerHTML = "";
  logs.forEach((log) => {
    const tr = document.createElement("tr");
    const plainText = auditToPlainSentence(log);
    const tagType = formatAuditTypeTag(log.origin);

    tr.innerHTML = `
      <td>${plainText}</td>
      <td style="text-align: right;"><span class="type-tag type-${tagType.class}">${tagType.label}</span></td>
    `;
    tbody.appendChild(tr);
  });
}

function auditToPlainSentence(log) {
  const origin = log.origin;
  const summary = log.decision_summary || "";
  const winner = log.winning_agent || "";

  if (summary.includes("crash") || summary.includes("Safe Default Mode")) {
    return `Coordinator detected agent timeout and automatically restored process health.`;
  }
  if (origin === "HUMAN_APPROVED") {
    return `Human operator approved safety line halt on Line 1.`;
  }
  if (origin === "HUMAN_OVERRIDDEN") {
    return `Human operator rejected line halt directive on Line 1.`;
  }
  if (winner === "MA" || summary.includes("MA")) {
    return `Coordinator awarded Line 1 to Maintenance Agent for preventative repair.`;
  }
  if (winner === "PPA" || summary.includes("PPA")) {
    return `Production Planning Agent scheduled batch production run on Line 1.`;
  }
  if (winner === "QCA" || summary.includes("QCA")) {
    return `Quality Control Agent flagged defect surge on Line 1.`;
  }

  return `Decision completed autonomously by ${winner || "Coordinator"}.`;
}

function formatAuditTypeTag(origin) {
  if (origin === "HUMAN_APPROVED" || origin === "HUMAN_OVERRIDDEN") return { label: "⚡ Human", class: "Human" };
  if (origin === "COORDINATOR_ARBITRATED") return { label: "⚖️ Arbitrated", class: "Arbitrated" };
  return { label: "⚡ Auto", class: "Auto" };
}

async function controlAgent(agentId, action) {
  try {
    await fetch(`/api/agents/${agentId}/control`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: action }),
    });
    fetchStatus();
  } catch (e) {
    console.error(`Error controlling agent ${agentId}:`, e);
  }
}

async function runScenario(id) {
  try {
    const res = await fetch(`/api/scenario/${id}`, { method: "POST" });
    if (res.ok) {
      fetchStatus();
      fetchAuditLogs();
    }
  } catch (e) {
    console.error(`Error running scenario ${id}:`, e);
  }
}
