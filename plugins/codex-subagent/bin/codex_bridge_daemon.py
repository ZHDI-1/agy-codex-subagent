#!/usr/bin/env python3
"""
Unified Codex & Gemini Subagent Mission Control Bridge Daemon.

Responsibilities:
1. Maintains a persistent WebSocket-over-AF_UNIX connection to `codex app-server` daemon
   (~/.codex/app-server-control/app-server-control.sock).
2. Maps Antigravity (`agy`) subagent conversations (`conversationId`) to Codex `threadId`
   and `activeTurnId`.
3. Translates `invoke_subagent(TypeName="codex")` and `send_message(Recipient=<id>)` into:
   - `thread/start` + `turn/start` (when thread is new or idle)
   - `turn/steer` (when thread is actively executing a turn — zero-interruption mid-turn steering!)
   - `thread/queue/add`, `turn/interrupt`, `thread/revert`, `thread/fork`, `turn/settings/update`
4. Mirrors Codex real-time `ServerNotification` stream (`reasoning`, `commandExecution`,
   `fileChange`, `agentMessage`, `turn/diff/updated`, `turn/plan/updated`) directly into
   `<appDataDir>/brain/<conversationId>/.system_generated/logs/transcript.jsonl` and
   `conversation_summaries.db` so native `agy` UI, `conversation://<id>`, and Swarm Board
   render Codex subagents identically to native Gemini subagents.
5. Automatically wakes up the parent `agy` conversation via `LanguageServerService/SendAgentMessage`
   (or `agy agentapi send-message`) when a Codex turn completes or requests guidance.
6. Provides a Unified Control Plane HTTP API (127.0.0.1:18765) for both the MCP Server and
   the Sidecar UI, taking over BOTH Codex subagents AND native Gemini subagents (`self`, `research`, etc.).
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from codex_ws_client import CodexAppServerClient, CodexRpcError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("codex_bridge")

BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = int(os.environ.get("AGY_CODEX_BRIDGE_PORT", "18765"))

MESSAGE_FROM_RE = re.compile(
    r"^Message from ([0-9a-fA-F-]{8,})(?:\s*\([^)]*\))?\s*:\s*\n?(.*)$",
    re.DOTALL,
)
SYSTEM_MSG_TAG_RE = re.compile(
    r"\[Message\]\s+timestamp=\S+\s+sender=([0-9a-fA-F-]{8,})\s+priority=\S+\s+content=(.*?)\s*(?:</SYSTEM_MESSAGE>|$)",
    re.DOTALL,
)
PREFIX_CMD_RE = re.compile(
    r"^\s*\[(STEER|QUEUE|INTERRUPT|REVERT(?::\d+)?|COMPACT|APPROVE|DECLINE|MODEL:[^\]]+|EFFORT:[^\]]+)\]\s*(.*)$",
    re.IGNORECASE | re.DOTALL,
)


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def find_app_data_dirs() -> List[Path]:
    dirs: List[Path] = []
    env_data = os.environ.get("ANTIGRAVITY_EXECUTABLE_DATA_DIR", "")
    if env_data:
        p = Path(env_data).resolve()
        for parent in [p] + list(p.parents):
            if parent.name == "sidecar_data":
                candidate = parent.parent
                if (candidate / "brain").exists():
                    dirs.append(candidate)
    gemini_root = Path.home() / ".gemini"
    for name in ("antigravity-cli", "antigravity", "antigravity-ide"):
        candidate = gemini_root / name
        if (candidate / "brain").exists() and candidate not in dirs:
            dirs.append(candidate)
    if gemini_root.exists():
        for child in sorted(gemini_root.iterdir()):
            if child.is_dir() and (child / "brain").exists() and child not in dirs:
                dirs.append(child)
    return dirs or [gemini_root / "antigravity-cli"]


def primary_app_data_dir() -> Path:
    return find_app_data_dirs()[0]


def unquote_arg(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        try:
            return str(json.loads(s))
        except Exception:
            return s[1:-1]
    return s


def summarize_transcript_file(transcript_path: Path, max_steps: int = 40) -> Dict[str, Any]:
    """Parses an agy transcript.jsonl file (works for both Gemini and Codex subagents)."""
    if not transcript_path.exists():
        return {
            "exists": False,
            "steps": 0,
            "activity": "",
            "activityKind": "",
            "lastThinking": "",
            "lastResponse": "",
            "lastError": "",
            "recentSteps": [],
            "mtime": 0,
        }
    try:
        st = transcript_path.stat()
        mtime = int(st.st_mtime * 1000)
        raw_text = transcript_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return {
            "exists": False,
            "steps": 0,
            "activity": "",
            "activityKind": "",
            "lastThinking": "",
            "lastResponse": "",
            "lastError": "",
            "recentSteps": [],
            "mtime": 0,
        }

    lines = [ln for ln in raw_text.splitlines() if ln.strip()]
    total_steps = len(lines)
    activity = ""
    activity_kind = ""
    last_thinking = ""
    last_response = ""
    last_error = ""
    recent_steps: List[Dict[str, Any]] = []

    for line in lines[-max_steps:]:
        try:
            step = json.loads(line)
        except Exception:
            continue
        if not isinstance(step, dict):
            continue
        s_idx = step.get("step_index", len(recent_steps))
        if isinstance(s_idx, int) and s_idx + 1 > total_steps:
            total_steps = s_idx + 1
        s_type = str(step.get("type", ""))
        s_status = str(step.get("status", ""))
        created_at = str(step.get("created_at", ""))

        # Support both flat transcript.jsonl format and nested planner_response format
        pr = step.get("planner_response") if isinstance(step.get("planner_response"), dict) else step
        thinking = str(pr.get("thinking") or step.get("thinking") or "")
        response_text = str(pr.get("response") or step.get("response") or "")
        content_text = str(step.get("content") or "")
        tool_calls = pr.get("tool_calls") or step.get("tool_calls") or []

        if thinking:
            last_thinking = thinking
        if response_text:
            last_response = response_text
        if step.get("error"):
            last_error = str(step.get("error"))

        tool_labels: List[str] = []
        if isinstance(tool_calls, list):
            for tc in tool_calls:
                if not isinstance(tc, dict):
                    continue
                tname = str(tc.get("name", ""))
                targs = tc.get("args") if isinstance(tc.get("args"), dict) else {}
                tsum = unquote_arg(targs.get("toolSummary") or targs.get("toolAction"))
                tpath = unquote_arg(targs.get("Path") or targs.get("AbsolutePath") or targs.get("TargetFile"))
                tcmd = unquote_arg(targs.get("CommandLine"))
                if tsum:
                    label = tsum
                elif tpath:
                    verb = {"read_file": "Read", "write_file": "Write", "edit_file": "Edit"}.get(tname, tname)
                    label = f"{verb} {Path(tpath).name}"
                elif tcmd:
                    label = f"Run {tcmd[:60]}"
                else:
                    label = tname
                if label:
                    tool_labels.append(label)

        if s_type == "PLANNER_RESPONSE":
            if tool_labels:
                activity = tool_labels[-1]
                activity_kind = "tool"
            elif thinking:
                first_line = thinking.strip().splitlines()[-1][:80]
                activity = first_line.replace("**", "").strip()
                activity_kind = "thinking"
            elif response_text:
                activity = "Responded"
                activity_kind = "response"

        recent_steps.append(
            {
                "stepIndex": s_idx,
                "type": s_type,
                "status": s_status,
                "createdAt": created_at,
                "thinking": thinking[:600] if thinking else "",
                "response": response_text[:800] if response_text else "",
                "content": content_text[:800] if content_text else "",
                "tools": tool_labels,
            }
        )

    return {
        "exists": True,
        "steps": total_steps,
        "activity": activity,
        "activityKind": activity_kind,
        "lastThinking": last_thinking[:1200],
        "lastResponse": last_response[:2000],
        "lastError": last_error,
        "recentSteps": recent_steps[-25:],
        "mtime": mtime,
    }


DYNAMIC_TOOLS_SPEC: List[Dict[str, Any]] = [
    {
        "name": "agy_notify_parent",
        "description": (
            "Send an immediate status update, interim finding, or clarification question "
            "to the parent Antigravity (agy) agent without waiting for the current turn to end."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "Markdown message to deliver to the parent Antigravity agent.",
                },
                "title": {
                    "type": "string",
                    "description": "Short summary title for the notification.",
                },
            },
            "required": ["message"],
        },
    },
    {
        "name": "agy_list_sibling_subagents",
        "description": (
            "List sibling subagents (both Gemini and Codex) running under the same parent "
            "Antigravity conversation, including their role, status, recent activity, and findings."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
]


class UnifiedSubagentBridge:
    """
    Core state machine managing:
    - Codex subagent sessions & direct WebSocket connection to codex app-server daemon
    - Native Gemini subagent discovery & control via LanguageServerService Connect-RPC
    - Real-time transcript.jsonl synthesis & SQLite synchronization
    """

    def __init__(self, state_file: Optional[Path] = None):
        self.app_data = primary_app_data_dir()
        self.state_file = state_file or (self.app_data / "codex_subagent_state.json")
        self.lock = threading.RLock()

        # Language Server credentials (updated dynamically from env / hooks / sidecar)
        self.ls_address: str = os.environ.get("ANTIGRAVITY_LS_ADDRESS", "")
        self.csrf_token: str = os.environ.get("ANTIGRAVITY_CSRF_TOKEN", "")
        self.agentapi_exe: str = os.environ.get("ANTIGRAVITY_AGENTAPI_EXE", "/home/zhdi/.local/bin/agy")

        # Session maps:
        # sessions_by_conv: conversationId -> session dict
        # conv_by_thread: codex threadId -> conversationId
        self.sessions_by_conv: Dict[str, Dict[str, Any]] = {}
        self.conv_by_thread: Dict[str, str] = {}

        # Per-item live buffers for active Codex turns: itemId -> dict
        self._item_buffers: Dict[str, Dict[str, Any]] = {}
        # Pending approval requests: approvalId -> {req_id, method, params, conversationId, threadId, createdAt}
        self.pending_approvals: Dict[str, Dict[str, Any]] = {}

        self._load_state()

        self.codex = CodexAppServerClient()
        self.codex.add_notification_handler(self._on_codex_notification)
        self.codex.set_server_request_handler(self._on_codex_server_request)

    # -------------------------------------------------------------------------
    # Persistence & Credentials
    # -------------------------------------------------------------------------

    def _load_state(self) -> None:
        if not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            with self.lock:
                self.ls_address = data.get("lsAddress") or self.ls_address
                self.csrf_token = data.get("csrfToken") or self.csrf_token
                for conv_id, sess in (data.get("sessions") or {}).items():
                    if isinstance(sess, dict):
                        self.sessions_by_conv[conv_id] = sess
                        tid = sess.get("threadId")
                        if tid:
                            self.conv_by_thread[tid] = conv_id
        except Exception as e:
            logger.warning("Failed to load bridge state file %s: %s", self.state_file, e)

    def _save_state(self) -> None:
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with self.lock:
                payload = {
                    "lsAddress": self.ls_address,
                    "csrfToken": self.csrf_token,
                    "updatedAt": utc_now_iso(),
                    "sessions": self.sessions_by_conv,
                }
            tmp = self.state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp.replace(self.state_file)
        except Exception as e:
            logger.warning("Failed to save bridge state: %s", e)

    def update_ls_credentials(
        self,
        ls_address: Optional[str] = None,
        csrf_token: Optional[str] = None,
        agentapi_exe: Optional[str] = None,
    ) -> None:
        with self.lock:
            changed = False
            if ls_address and ls_address != self.ls_address:
                self.ls_address = ls_address
                changed = True
            if csrf_token and csrf_token != self.csrf_token:
                self.csrf_token = csrf_token
                changed = True
            if agentapi_exe and agentapi_exe != self.agentapi_exe:
                self.agentapi_exe = agentapi_exe
                changed = True
            if changed:
                self._save_state()

    # -------------------------------------------------------------------------
    # Antigravity Language Server Connect-RPC & SQLite Helpers
    # -------------------------------------------------------------------------

    def lsrpc(
        self,
        method: str,
        payload: Dict[str, Any],
        ls_address: Optional[str] = None,
        csrf_token: Optional[str] = None,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        addr = ls_address or self.ls_address
        token = csrf_token or self.csrf_token
        if not addr:
            raise RuntimeError("Language Server address (ANTIGRAVITY_LS_ADDRESS) is not known yet")
        url = f"http://{addr}/exa.language_server_pb.LanguageServerService/{method}"
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if token:
            headers["x-codeium-csrf-token"] = token
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return json.loads(raw) if raw.strip() else {}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"LS RPC {method} HTTP {e.code}: {err_body}")

    def send_message_to_agy_conversation(
        self,
        recipient_id: str,
        content: str,
        sender_id: Optional[str] = None,
        title: Optional[str] = None,
        ls_address: Optional[str] = None,
        csrf_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Sends an agent message into `recipient_id` via LanguageServerService/SendAgentMessage,
        triggering Reactive Wakeup on the parent or child conversation.
        Falls back to `agy agentapi send-message` if direct RPC is unavailable.
        """
        if not recipient_id:
            return {"sent": False, "reason": "empty recipient_id"}

        req_payload: Dict[str, Any] = {
            "recipient": recipient_id,
            "content": content,
        }
        if sender_id:
            req_payload["senderId"] = sender_id
        if title:
            req_payload["displayTitle"] = title

        try:
            res = self.lsrpc("SendAgentMessage", req_payload, ls_address=ls_address, csrf_token=csrf_token)
            logger.info("Sent agent message via SendAgentMessage RPC: %s -> %s", sender_id, recipient_id)
            return {"sent": True, "via": "SendAgentMessage", "response": res}
        except Exception as e:
            logger.warning("SendAgentMessage RPC failed (%s); falling back to agentapi CLI...", e)

        exe = self.agentapi_exe or "/home/zhdi/.local/bin/agy"
        cmd = [exe, "agentapi", "send-message"]
        if title:
            cmd.extend(["--title", title])
        cmd.extend([recipient_id, content])

        env = os.environ.copy()
        if ls_address or self.ls_address:
            env["ANTIGRAVITY_LS_ADDRESS"] = ls_address or self.ls_address
        if csrf_token or self.csrf_token:
            env["ANTIGRAVITY_CSRF_TOKEN"] = csrf_token or self.csrf_token
        if sender_id:
            env["ANTIGRAVITY_CONVERSATION_ID"] = sender_id

        try:
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=10.0)
            if proc.returncode == 0:
                return {"sent": True, "via": "agentapi", "stdout": proc.stdout.strip()}
            return {"sent": False, "via": "agentapi", "error": proc.stderr.strip() or proc.stdout.strip()}
        except Exception as ex:
            return {"sent": False, "error": str(ex)}

    def query_db_conversation_meta(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """Reads conversation metadata directly from conversation_summaries.db (< 1ms)."""
        for app_dir in find_app_data_dirs():
            db_path = app_dir / "conversation_summaries.db"
            if not db_path.exists():
                continue
            try:
                conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0)
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT conversation_id, title, preview, step_count, workspace_uris, status, "
                    "agent_name, parent_conversation_id, nesting_depth, not_fully_idle, killed, raw_summary "
                    "FROM conversation_summaries WHERE conversation_id = ?",
                    (conversation_id,),
                ).fetchone()
                conn.close()
                if row:
                    d = dict(row)
                    raw = d.pop("raw_summary", None)
                    role = ""
                    if isinstance(raw, (bytes, bytearray)):
                        # Extract readable role string from SubagentDescriptor in raw_summary
                        strings = [s.decode("utf-8", "ignore") for s in re.findall(rb"[ -~]{3,80}", raw)]
                        ag_name = d.get("agent_name", "")
                        if ag_name and ag_name in strings:
                            idx = len(strings) - 1 - strings[::-1].index(ag_name)
                            if idx + 1 < len(strings):
                                candidate = strings[idx + 1].rstrip("0123456789 ")
                                if candidate and not candidate.startswith("file://"):
                                    role = candidate
                    d["role"] = role
                    d["appDataDir"] = str(app_dir)
                    return d
            except Exception as e:
                logger.debug("SQLite read failed on %s: %s", db_path, e)
        return None

    # -------------------------------------------------------------------------
    # Real-time transcript.jsonl & SQLite Mirroring for Codex Subagents
    # -------------------------------------------------------------------------

    def _transcript_path_for(self, session: Dict[str, Any]) -> Path:
        tp = session.get("transcriptPath")
        if tp:
            p = Path(tp)
            if p.name == "transcript_full.jsonl":
                return p.parent / "transcript.jsonl"
            return p
        conv_id = session["conversationId"]
        return self.app_data / "brain" / conv_id / ".system_generated" / "logs" / "transcript.jsonl"

    def _append_transcript_step(self, session: Dict[str, Any], step: Dict[str, Any]) -> int:
        tp = self._transcript_path_for(session)
        tp.parent.mkdir(parents=True, exist_ok=True)
        with self.lock:
            idx = int(session.get("nextStepIndex", 0))
            if idx == 0 and tp.exists():
                try:
                    existing = [ln for ln in tp.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip()]
                    if existing:
                        last_obj = json.loads(existing[-1])
                        idx = max(len(existing), int(last_obj.get("step_index", len(existing) - 1)) + 1)
                except Exception:
                    pass
            step_record = {
                "step_index": idx,
                "created_at": utc_now_iso(),
                **step,
            }
            with open(tp, "a", encoding="utf-8") as f:
                f.write(json.dumps(step_record, ensure_ascii=False) + "\n")
            session["nextStepIndex"] = idx + 1
            session["stepCount"] = idx + 1
            session["updatedAt"] = utc_now_iso()
            return idx

    def _sync_sqlite_status(
        self,
        conversation_id: str,
        status: str,
        step_count: int,
        preview: str = "",
        not_fully_idle: int = 0,
    ) -> None:
        """Updates conversation_summaries.db so native agy UI reflects live step counts & run state."""
        db_path = self.app_data / "conversation_summaries.db"
        if not db_path.exists():
            return
        try:
            conn = sqlite3.connect(str(db_path), timeout=2.0)
            if preview:
                conn.execute(
                    "UPDATE conversation_summaries SET status = ?, step_count = MAX(step_count, ?), "
                    "preview = ?, not_fully_idle = ?, last_modified_time = datetime('now') "
                    "WHERE conversation_id = ?",
                    (status, step_count, preview[:240], not_fully_idle, conversation_id),
                )
            else:
                conn.execute(
                    "UPDATE conversation_summaries SET status = ?, step_count = MAX(step_count, ?), "
                    "not_fully_idle = ?, last_modified_time = datetime('now') "
                    "WHERE conversation_id = ?",
                    (status, step_count, not_fully_idle, conversation_id),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug("SQLite status sync skipped: %s", e)

    # -------------------------------------------------------------------------
    # Codex Subagent Dispatch (Initial Launch, Follow-up Turn, Mid-Turn Steer)
    # -------------------------------------------------------------------------

    def dispatch_subagent_input(
        self,
        conversation_id: str,
        user_input: str,
        parent_conversation_id: Optional[str] = None,
        cwd: Optional[str] = None,
        transcript_path: Optional[str] = None,
        role: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        sandbox: str = "workspace-write",
        approval_policy: str = "never",
        ls_address: Optional[str] = None,
        csrf_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Entry point called when `PreInvocation` hook intercepts a `codex` subagent turn
        or when the user/parent explicitly dispatches or steers a Codex subagent.
        """
        self.update_ls_credentials(ls_address=ls_address, csrf_token=csrf_token)

        # Unwrap "Message from <parent-id>: ..." or "<SYSTEM_MESSAGE>[Message] sender=<parent-id> content=..."
        clean_text = (user_input or "").strip()
        extracted_parent = parent_conversation_id or ""
        m_sys = SYSTEM_MSG_TAG_RE.search(clean_text)
        if m_sys:
            if not extracted_parent:
                extracted_parent = m_sys.group(1).strip()
            clean_text = m_sys.group(2).strip()
        else:
            m = MESSAGE_FROM_RE.match(clean_text)
            if m:
                if not extracted_parent:
                    extracted_parent = m.group(1).strip()
                clean_text = m.group(2).strip()

        # Check SQLite metadata if parent or role is still missing
        db_meta = self.query_db_conversation_meta(conversation_id)
        if db_meta:
            if not extracted_parent:
                extracted_parent = db_meta.get("parent_conversation_id") or ""
            if not role:
                role = db_meta.get("role") or db_meta.get("title") or "Codex Subagent"
            if not cwd:
                try:
                    uris = json.loads(db_meta.get("workspace_uris") or "[]")
                    if uris and isinstance(uris[0], str) and uris[0].startswith("file://"):
                        cwd = urllib.parse.unquote(uris[0][len("file://") :])
                except Exception:
                    pass

        with self.lock:
            session = self.sessions_by_conv.get(conversation_id)
            if session is None:
                session = {
                    "conversationId": conversation_id,
                    "parentConversationId": extracted_parent,
                    "engine": "codex",
                    "role": role or "Codex Engineer",
                    "cwd": cwd or os.getcwd(),
                    "transcriptPath": transcript_path
                    or str(
                        self.app_data
                        / "brain"
                        / conversation_id
                        / ".system_generated"
                        / "logs"
                        / "transcript.jsonl"
                    ),
                    "threadId": None,
                    "activeTurnId": None,
                    "status": "initializing",
                    "model": model or "",
                    "effort": effort or "high",
                    "sandbox": sandbox,
                    "approvalPolicy": approval_policy,
                    "approvalMode": "auto_accept",
                    "stepCount": 0,
                    "nextStepIndex": 0,
                    "currentActivity": "Initializing Codex thread...",
                    "activityKind": "thinking",
                    "latestDiff": "",
                    "plan": [],
                    "planExplanation": "",
                    "lastAgentMessage": "",
                    "lastThinking": "",
                    "turnHistory": [],
                    "events": [],
                    "createdAt": utc_now_iso(),
                    "updatedAt": utc_now_iso(),
                }
                self.sessions_by_conv[conversation_id] = session
            else:
                if extracted_parent and not session.get("parentConversationId"):
                    session["parentConversationId"] = extracted_parent
                if cwd:
                    session["cwd"] = cwd
                if transcript_path:
                    session["transcriptPath"] = transcript_path
                if role and (not session.get("role") or session["role"] == "Codex Subagent"):
                    session["role"] = role
                if model:
                    session["model"] = model
                if effort:
                    session["effort"] = effort

        # Ensure Codex thread exists
        thread_id = session.get("threadId")
        if not thread_id:
            dev_instructions = (
                "You are running as a native OpenAI Codex Subagent inside Google Antigravity (agy).\n"
                f"Your Assigned Role: {session.get('role', 'Codex Subagent')}\n"
                f"Subagent Conversation ID: {conversation_id}\n"
                f"Parent Conversation ID: {session.get('parentConversationId', 'unknown')}\n"
                "You have direct access to dynamic tools `agy_notify_parent` (to send mid-turn progress "
                "or questions to the parent agent) and `agy_list_sibling_subagents` (to inspect sibling subagents). "
                "When your turn completes, your final response and unified diff summary will be delivered "
                "automatically to the parent Antigravity agent."
            )
            start_res = self.codex.thread_start(
                cwd=session.get("cwd"),
                model=session.get("model") or None,
                approval_policy=session.get("approvalPolicy", "never"),
                sandbox=session.get("sandbox", "workspace-write"),
                developer_instructions=dev_instructions,
                dynamic_tools=DYNAMIC_TOOLS_SPEC,
            )
            thread_obj = start_res.get("thread") or {}
            thread_id = thread_obj.get("id")
            if not thread_id:
                raise RuntimeError(f"Codex thread/start did not return thread.id: {start_res}")
            with self.lock:
                session["threadId"] = thread_id
                if start_res.get("model"):
                    session["model"] = start_res["model"]
                if start_res.get("reasoningEffort"):
                    session["effort"] = start_res["reasoningEffort"]
                self.conv_by_thread[thread_id] = conversation_id
                self._save_state()

        # Check for control command prefixes: [STEER], [QUEUE], [INTERRUPT], [REVERT:N], [COMPACT], [MODEL:...], [EFFORT:...]
        cmd_match = PREFIX_CMD_RE.match(clean_text)
        if cmd_match:
            directive = cmd_match.group(1).upper()
            remainder = cmd_match.group(2).strip()
            return self._execute_directive(session, directive, remainder)

        # Log user/parent message into transcript.jsonl
        self._append_transcript_step(
            session,
            {
                "source": "USER_EXPLICIT",
                "type": "USER_INPUT",
                "status": "DONE",
                "content": clean_text,
            },
        )

        active_turn_id = session.get("activeTurnId")
        is_working = session.get("status") == "working" and bool(active_turn_id)

        # If a turn is currently running, AUTOMATICALLY MID-TURN STEER IT!
        if is_working and active_turn_id:
            try:
                steer_res = self.codex.turn_steer(
                    thread_id=thread_id,
                    expected_turn_id=active_turn_id,
                    steer_text=clean_text,
                )
                self._record_event(session, "turn/steer", {"turnId": active_turn_id, "text": clean_text[:300]})
                self._append_transcript_step(
                    session,
                    {
                        "source": "MODEL",
                        "type": "PLANNER_RESPONSE",
                        "status": "DONE",
                        "model": session.get("model") or "codex",
                        "thinking": f"**Mid-Turn Steer Applied to Turn {active_turn_id}**\n{clean_text}",
                        "tool_calls": [],
                    },
                )
                self._save_state()
                return {
                    "action": "turn/steer",
                    "conversationId": conversation_id,
                    "threadId": thread_id,
                    "turnId": steer_res.get("turnId", active_turn_id),
                    "status": "steered",
                }
            except CodexRpcError as e:
                logger.info("turn/steer pre-condition missed (%s); falling back to turn/start", e)

        # Start a new turn on the thread
        turn_res = self.codex.turn_start(
            thread_id=thread_id,
            prompt_text=clean_text,
            cwd=session.get("cwd"),
            model=session.get("model") or None,
            effort=session.get("effort") or None,
        )
        turn_obj = turn_res.get("turn") or {}
        turn_id = turn_obj.get("id")
        with self.lock:
            session["activeTurnId"] = turn_id
            session["status"] = "working"
            session["currentActivity"] = "Starting Codex turn..."
            session["activityKind"] = "thinking"
            session["lastAgentMessage"] = ""
            session["lastThinking"] = ""
            self._record_event(session, "turn/start", {"turnId": turn_id, "prompt": clean_text[:300]})
            self._save_state()

        self._sync_sqlite_status(
            conversation_id,
            status="CASCADE_RUN_STATUS_RUNNING",
            step_count=session.get("stepCount", 1),
            preview=clean_text[:160],
            not_fully_idle=1,
        )
        return {
            "action": "turn/start",
            "conversationId": conversation_id,
            "threadId": thread_id,
            "turnId": turn_id,
            "status": "working",
        }

    def _execute_directive(self, session: Dict[str, Any], directive: str, remainder: str) -> Dict[str, Any]:
        conv_id = session["conversationId"]
        thread_id = session["threadId"]
        active_turn = session.get("activeTurnId")

        if directive == "STEER":
            if not active_turn:
                # If idle, start a turn instead
                return self.dispatch_subagent_input(conv_id, remainder)
            res = self.codex.turn_steer(thread_id=thread_id, expected_turn_id=active_turn, steer_text=remainder)
            self._record_event(session, "turn/steer", {"turnId": active_turn, "text": remainder[:300]})
            self._append_transcript_step(
                session,
                {
                    "source": "MODEL",
                    "type": "PLANNER_RESPONSE",
                    "status": "DONE",
                    "model": session.get("model") or "codex",
                    "thinking": f"**Steered Turn {active_turn}**\n{remainder}",
                    "tool_calls": [],
                },
            )
            self._save_state()
            return {"action": "turn/steer", "conversationId": conv_id, "threadId": thread_id, "turnId": active_turn, "result": res}

        if directive == "QUEUE":
            res = self.codex.thread_queue_add(thread_id=thread_id, text=remainder)
            self._record_event(session, "thread/queue/add", {"text": remainder[:300]})
            self._save_state()
            return {"action": "thread/queue/add", "conversationId": conv_id, "threadId": thread_id, "result": res}

        if directive == "INTERRUPT":
            if active_turn:
                self.codex.turn_interrupt(thread_id=thread_id, turn_id=active_turn)
                with self.lock:
                    session["activeTurnId"] = None
                    session["status"] = "idle"
                    session["currentActivity"] = "Interrupted"
            self._record_event(session, "turn/interrupt", {"turnId": active_turn})
            self._save_state()
            if remainder:
                return self.dispatch_subagent_input(conv_id, remainder)
            return {"action": "turn/interrupt", "conversationId": conv_id, "threadId": thread_id, "interruptedTurnId": active_turn}

        if directive.startswith("REVERT"):
            parts = directive.split(":")
            turns_to_drop = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
            before_turn_id = self._resolve_before_turn_id(session, turns_to_drop)
            if not before_turn_id:
                return {"action": "thread/revert", "conversationId": conv_id, "threadId": thread_id, "skipped": "no turns to revert"}
            res = self.codex.thread_revert(thread_id=thread_id, before_turn_id=before_turn_id)
            self._record_event(session, "thread/revert", {"beforeTurnId": before_turn_id, "turnsToDrop": turns_to_drop})
            self._save_state()
            return {"action": "thread/revert", "conversationId": conv_id, "threadId": thread_id, "beforeTurnId": before_turn_id, "result": res}

        if directive == "COMPACT":
            res = self.codex.thread_compact_start(thread_id=thread_id)
            self._record_event(session, "thread/compact/start", {})
            return {"action": "thread/compact/start", "conversationId": conv_id, "threadId": thread_id, "result": res}

        if directive.startswith("MODEL:"):
            new_model = directive.split(":", 1)[1].strip()
            with self.lock:
                session["model"] = new_model
            res = None
            if active_turn:
                res = self.codex.turn_settings_update(thread_id=thread_id, turn_id=active_turn, model=new_model)
            self._save_state()
            if remainder:
                return self.dispatch_subagent_input(conv_id, remainder)
            return {"action": "turn/settings/update", "model": new_model, "result": res}

        if directive.startswith("EFFORT:"):
            new_effort = directive.split(":", 1)[1].strip().lower()
            with self.lock:
                session["effort"] = new_effort
            res = None
            if active_turn:
                res = self.codex.turn_settings_update(thread_id=thread_id, turn_id=active_turn, effort=new_effort)
            self._save_state()
            if remainder:
                return self.dispatch_subagent_input(conv_id, remainder)
            return {"action": "turn/settings/update", "effort": new_effort, "result": res}

        if directive in ("APPROVE", "DECLINE"):
            decision = "accept" if directive == "APPROVE" else "decline"
            resolved = self.resolve_pending_approvals_for_conversation(conv_id, decision=decision)
            return {"action": "approval", "decision": decision, "resolvedCount": resolved}

        return self.dispatch_subagent_input(conv_id, remainder)

    def _resolve_before_turn_id(self, session: Dict[str, Any], turns_to_drop: int = 1) -> Optional[str]:
        thread_id = session.get("threadId")
        history = session.get("turnHistory") or []
        if len(history) >= turns_to_drop:
            cand = history[-turns_to_drop].get("turnId")
            if cand:
                return str(cand)
        if thread_id:
            try:
                t_read = self.codex.thread_read(thread_id=thread_id, include_turns=True)
                turns = (t_read.get("thread") or {}).get("turns") or []
                if turns:
                    idx = max(0, len(turns) - turns_to_drop)
                    return turns[idx].get("id")
            except Exception as e:
                logger.warning("thread_read during revert failed: %s", e)
        return session.get("activeTurnId")

    def _record_event(self, session: Dict[str, Any], event_type: str, data: Dict[str, Any]) -> None:
        with self.lock:
            ev_list = session.setdefault("events", [])
            ev_list.append({"timestamp": utc_now_iso(), "type": event_type, **data})
            if len(ev_list) > 80:
                del ev_list[:-80]

    # -------------------------------------------------------------------------
    # Codex ServerNotification Stream Handler -> transcript.jsonl & Parent Wakeup
    # -------------------------------------------------------------------------

    def _on_codex_notification(self, method: str, params: Dict[str, Any]) -> None:
        thread_id = params.get("threadId")
        if not thread_id:
            return
        with self.lock:
            conv_id = self.conv_by_thread.get(thread_id)
            session = self.sessions_by_conv.get(conv_id) if conv_id else None
        if not session or not conv_id:
            return

        if method == "turn/started":
            turn = params.get("turn") or {}
            turn_id = turn.get("id")
            with self.lock:
                session["activeTurnId"] = turn_id
                session["status"] = "working"
                session["currentActivity"] = "Thinking..."
                session["activityKind"] = "thinking"
                session["updatedAt"] = utc_now_iso()
            self._record_event(session, "turn/started", {"turnId": turn_id})
            return

        if method == "turn/diff/updated":
            diff_text = params.get("diff") or ""
            with self.lock:
                session["latestDiff"] = diff_text
                session["updatedAt"] = utc_now_iso()
            return

        if method == "turn/plan/updated":
            plan = params.get("plan") or []
            explanation = params.get("explanation") or ""
            with self.lock:
                session["plan"] = plan
                session["planExplanation"] = explanation
                session["updatedAt"] = utc_now_iso()
            plan_lines = [f"- [{p.get('status', 'pending')}] {p.get('step', '')}" for p in plan if isinstance(p, dict)]
            self._append_transcript_step(
                session,
                {
                    "source": "MODEL",
                    "type": "PLANNER_RESPONSE",
                    "status": "DONE",
                    "model": session.get("model") or "codex",
                    "thinking": f"**Updated Execution Plan**\n{explanation}\n" + "\n".join(plan_lines),
                    "tool_calls": [],
                },
            )
            return

        if method in ("item/reasoning/summaryTextDelta", "item/reasoning/textDelta"):
            item_id = params.get("itemId", "")
            delta = params.get("delta", "")
            with self.lock:
                buf = self._item_buffers.setdefault(item_id, {"type": "reasoning", "text": ""})
                buf["text"] += delta
                full_thinking = buf["text"].strip()
                session["lastThinking"] = full_thinking[-2000:]
                lines = [ln.strip("*# ") for ln in full_thinking.splitlines() if ln.strip()]
                if lines:
                    session["currentActivity"] = lines[-1][:80]
                    session["activityKind"] = "thinking"
            return

        if method == "item/agentMessage/delta":
            item_id = params.get("itemId", "")
            delta = params.get("delta", "")
            with self.lock:
                buf = self._item_buffers.setdefault(item_id, {"type": "agentMessage", "text": ""})
                buf["text"] += delta
                session["lastAgentMessage"] = buf["text"]
            return

        if method in ("item/commandExecution/outputDelta", "item/fileChange/outputDelta"):
            item_id = params.get("itemId", "")
            delta = params.get("delta", "")
            with self.lock:
                buf = self._item_buffers.setdefault(item_id, {"type": "output", "output": ""})
                buf["output"] = (buf.get("output", "") + delta)[-16000:]
            return

        if method == "item/started":
            item = params.get("item") or {}
            item_id = item.get("id", str(uuid.uuid4()))
            item_type = item.get("type", "")
            thinking_snapshot = session.get("lastThinking", "")

            if item_type == "commandExecution":
                cmd_str = item.get("command", "")
                cwd_str = item.get("cwd", session.get("cwd", ""))
                summary_label = f"Run {cmd_str[:64]}"
                with self.lock:
                    session["currentActivity"] = summary_label
                    session["activityKind"] = "tool"
                    self._item_buffers[item_id] = {"type": "commandExecution", "command": cmd_str, "output": ""}
                self._append_transcript_step(
                    session,
                    {
                        "source": "MODEL",
                        "type": "PLANNER_RESPONSE",
                        "status": "DONE",
                        "model": session.get("model") or "codex",
                        "thinking": thinking_snapshot,
                        "tool_calls": [
                            {
                                "id": item_id,
                                "name": "run_command",
                                "args": {
                                    "CommandLine": json.dumps(cmd_str),
                                    "Cwd": json.dumps(cwd_str),
                                    "toolSummary": json.dumps(summary_label),
                                },
                            }
                        ],
                    },
                )
                self._record_event(session, "commandExecution/started", {"command": cmd_str, "itemId": item_id})

            elif item_type == "fileChange":
                changes = item.get("changes") or []
                first_path = changes[0].get("path", "file") if changes and isinstance(changes[0], dict) else "file"
                summary_label = f"Edit {Path(first_path).name}"
                with self.lock:
                    session["currentActivity"] = summary_label
                    session["activityKind"] = "tool"
                    self._item_buffers[item_id] = {"type": "fileChange", "path": first_path, "output": ""}
                self._append_transcript_step(
                    session,
                    {
                        "source": "MODEL",
                        "type": "PLANNER_RESPONSE",
                        "status": "DONE",
                        "model": session.get("model") or "codex",
                        "thinking": thinking_snapshot,
                        "tool_calls": [
                            {
                                "id": item_id,
                                "name": "edit_file",
                                "args": {
                                    "Path": json.dumps(first_path),
                                    "toolSummary": json.dumps(summary_label),
                                },
                            }
                        ],
                    },
                )
                self._record_event(session, "fileChange/started", {"path": first_path, "itemId": item_id})

            elif item_type in ("mcpToolCall", "dynamicToolCall"):
                t_name = item.get("tool", item_type)
                summary_label = f"Tool {t_name}"
                with self.lock:
                    session["currentActivity"] = summary_label
                    session["activityKind"] = "tool"
                self._append_transcript_step(
                    session,
                    {
                        "source": "MODEL",
                        "type": "PLANNER_RESPONSE",
                        "status": "DONE",
                        "model": session.get("model") or "codex",
                        "thinking": thinking_snapshot,
                        "tool_calls": [
                            {
                                "id": item_id,
                                "name": t_name,
                                "args": {"toolSummary": json.dumps(summary_label)},
                            }
                        ],
                    },
                )
            return

        if method == "item/completed":
            item = params.get("item") or {}
            item_id = item.get("id", "")
            item_type = item.get("type", "")
            buf = self._item_buffers.pop(item_id, {})

            if item_type == "reasoning":
                summary_list = item.get("summary") or []
                content_list = item.get("content") or []
                text = "\n".join(summary_list or content_list) or buf.get("text", "")
                if text:
                    with self.lock:
                        session["lastThinking"] = text

            elif item_type == "agentMessage":
                text = item.get("text") or buf.get("text", "")
                if text:
                    with self.lock:
                        session["lastAgentMessage"] = text

            elif item_type == "commandExecution":
                cmd_str = item.get("command", "")
                exit_code = item.get("exitCode")
                duration_ms = item.get("durationMs")
                agg_out = item.get("aggregatedOutput") or buf.get("output", "")
                status_str = "DONE" if (exit_code in (0, None)) else "ERROR"
                content = (
                    f"Command: {cmd_str}\n"
                    f"Exit Code: {exit_code} (Duration: {duration_ms}ms)\n"
                    f"Output:\n{agg_out[:4000]}"
                )
                self._append_transcript_step(
                    session,
                    {
                        "source": "MODEL",
                        "type": "GENERIC",
                        "status": status_str,
                        "content": content,
                    },
                )

            elif item_type == "fileChange":
                changes = item.get("changes") or []
                summary_lines = []
                for ch in changes:
                    if isinstance(ch, dict):
                        p = ch.get("path", "")
                        diff_snip = (ch.get("diff") or "")[:1500]
                        summary_lines.append(f"File: {p}\n{diff_snip}")
                self._append_transcript_step(
                    session,
                    {
                        "source": "MODEL",
                        "type": "GENERIC",
                        "status": "DONE",
                        "content": "\n---\n".join(summary_lines) or "File change completed.",
                    },
                )
            return

        if method == "turn/completed":
            turn = params.get("turn") or {}
            turn_id = turn.get("id", "")
            turn_status = turn.get("status", "completed")
            turn_error = turn.get("error")

            with self.lock:
                final_msg = session.get("lastAgentMessage", "").strip()
                final_thinking = session.get("lastThinking", "").strip()
                latest_diff = session.get("latestDiff", "").strip()
                plan = session.get("plan") or []
                role = session.get("role", "Codex Subagent")
                parent_id = session.get("parentConversationId", "")

                session["activeTurnId"] = None
                session["status"] = "error" if turn_status == "failed" else "idle"
                session["currentActivity"] = (
                    f"Failed: {turn_error}" if turn_status == "failed" else "Idle (turn completed)"
                )
                session["activityKind"] = ""
                session.setdefault("turnHistory", []).append(
                    {
                        "turnId": turn_id,
                        "status": turn_status,
                        "completedAt": utc_now_iso(),
                        "summary": final_msg[:500],
                    }
                )
                self._record_event(session, "turn/completed", {"turnId": turn_id, "status": turn_status})

            step_idx = self._append_transcript_step(
                session,
                {
                    "source": "MODEL",
                    "type": "PLANNER_RESPONSE",
                    "status": "ERROR" if turn_status == "failed" else "DONE",
                    "model": session.get("model") or "codex",
                    "thinking": final_thinking,
                    "response": final_msg or (f"Turn {turn_status}" if not turn_error else f"Error: {turn_error}"),
                    "tool_calls": [],
                    **({"error": json.dumps(turn_error)} if turn_error else {}),
                },
            )
            self._sync_sqlite_status(
                conv_id,
                status="CASCADE_RUN_STATUS_IDLE",
                step_count=step_idx + 1,
                preview=(final_msg or f"Turn {turn_status}")[:200],
                not_fully_idle=0,
            )
            self._save_state()

            # Deliver final message back to parent conversation to trigger Reactive Wakeup!
            if parent_id:
                report_parts = [
                    f"### Codex Subagent Report (`{role}`)",
                    f"- **Subagent Conversation**: [{role}](conversation://{conv_id})",
                    f"- **Codex Thread / Turn**: `{thread_id}` / `{turn_id}` (`{turn_status}`)",
                ]
                if plan:
                    plan_md = "\n".join(
                        f"  - `[{'x' if p.get('status') == 'completed' else ' '}]` {p.get('step', '')}"
                        for p in plan
                        if isinstance(p, dict)
                    )
                    report_parts.append(f"- **Execution Plan**:\n{plan_md}")
                if final_msg:
                    report_parts.append(f"\n{final_msg}")
                if turn_error:
                    report_parts.append(f"\n**Error Details**: `{json.dumps(turn_error)}`")
                if latest_diff:
                    diff_lines = latest_diff.splitlines()
                    preview_diff = "\n".join(diff_lines[:60])
                    if len(diff_lines) > 60:
                        preview_diff += f"\n... ({len(diff_lines) - 60} more diff lines; use `subagent_inspect` to view full diff)"
                    report_parts.append(f"\n<details><summary>Unified Diff Summary ({len(diff_lines)} lines)</summary>\n\n```diff\n{preview_diff}\n```\n</details>")

                full_report = "\n".join(report_parts)
                self.send_message_to_agy_conversation(
                    recipient_id=parent_id,
                    sender_id=conv_id,
                    title=f"Codex Subagent ({role}) Turn Completed",
                    content=full_report,
                )

    # -------------------------------------------------------------------------
    # Codex ServerRequest Handler (Approvals & Dynamic Tools)
    # -------------------------------------------------------------------------

    def _on_codex_server_request(
        self,
        req_id: int | str,
        method: str,
        params: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        thread_id = params.get("threadId", "")
        with self.lock:
            conv_id = self.conv_by_thread.get(thread_id, "")
            session = self.sessions_by_conv.get(conv_id) if conv_id else None

        # 1. Dynamic tool call from Codex back into Antigravity!
        if method == "item/tool/call":
            tool_name = params.get("tool", "")
            args = params.get("arguments") or {}
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = {"raw": args}

            if tool_name == "agy_notify_parent":
                msg_text = str(args.get("message", ""))
                title = str(args.get("title") or f"Codex Subagent Interim Update")
                parent_id = session.get("parentConversationId", "") if session else ""
                if parent_id and msg_text:
                    res = self.send_message_to_agy_conversation(
                        recipient_id=parent_id,
                        sender_id=conv_id or None,
                        title=title,
                        content=f"**[Codex Interim Notification (`{thread_id}`)]**\n\n{msg_text}",
                    )
                    return {
                        "success": bool(res.get("sent")),
                        "contentItems": [
                            {
                                "type": "inputText",
                                "text": f"Delivered notification to parent conversation {parent_id}: {json.dumps(res)}",
                            }
                        ],
                    }
                return {
                    "success": False,
                    "contentItems": [{"type": "inputText", "text": "No parentConversationId bound to this thread."}],
                }

            if tool_name == "agy_list_sibling_subagents":
                parent_id = session.get("parentConversationId", "") if session else ""
                roster = self.list_unified_subagents(root_conversation_id=parent_id or None)
                return {
                    "success": True,
                    "contentItems": [
                        {
                            "type": "inputText",
                            "text": json.dumps(roster, indent=2),
                        }
                    ],
                }

            return {
                "success": False,
                "contentItems": [{"type": "inputText", "text": f"Unknown dynamic tool: {tool_name}"}],
            }

        # 2. Approval requests (commandExecution, fileChange, permissions)
        if method in (
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
        ):
            approval_mode = session.get("approvalMode", "auto_accept") if session else "auto_accept"
            if approval_mode == "auto_accept":
                if method == "item/permissions/requestApproval":
                    return {"permissions": params.get("permissions", {}), "scope": "turn"}
                return {"decision": "accept"}

            # Hold request in pending_approvals for interactive UI / parent approval
            approval_id = f"appr_{uuid.uuid4().hex[:10]}"
            with self.lock:
                self.pending_approvals[approval_id] = {
                    "approvalId": approval_id,
                    "rpcRequestId": req_id,
                    "method": method,
                    "params": params,
                    "conversationId": conv_id,
                    "threadId": thread_id,
                    "createdAt": utc_now_iso(),
                }
                if session:
                    session["status"] = "awaiting_approval"
                    session["currentActivity"] = f"Awaiting approval ({method.split('/')[1]})"
            return None  # Hold open asynchronously until resolve_approval is called

        return {}

    def resolve_pending_approvals_for_conversation(
        self,
        conversation_id: str,
        decision: str = "accept",
        approval_id: Optional[str] = None,
    ) -> int:
        count = 0
        with self.lock:
            to_resolve = [
                aid
                for aid, item in self.pending_approvals.items()
                if (approval_id and aid == approval_id)
                or (not approval_id and item.get("conversationId") == conversation_id)
            ]
        for aid in to_resolve:
            with self.lock:
                item = self.pending_approvals.pop(aid, None)
            if not item:
                continue
            method = item["method"]
            req_id = item["rpcRequestId"]
            if method == "item/permissions/requestApproval":
                result = {"permissions": item["params"].get("permissions", {}) if decision == "accept" else {}, "scope": "turn"}
            else:
                result = {"decision": decision}
            self.codex.respond_to_server_request(req_id, result=result)
            count += 1
        with self.lock:
            sess = self.sessions_by_conv.get(conversation_id)
            if sess and sess.get("status") == "awaiting_approval":
                sess["status"] = "working"
        return count

    # -------------------------------------------------------------------------
    # Unified Subagent Discovery & Control (Both Gemini Native & Codex!)
    # -------------------------------------------------------------------------

    def list_unified_subagents(
        self,
        root_conversation_id: Optional[str] = None,
        include_all: bool = True,
    ) -> Dict[str, Any]:
        """
        Builds a unified roster of:
        1. All Codex subagents registered in the Bridge.
        2. All Gemini native subagents (`self`, `research`, custom `.md` agents) and Codex subagents
           from `<appDataDir>/conversation_summaries.db`.
        3. Optionally standalone loaded Codex threads from the daemon (`thread/loaded/list`).
        """
        items_by_conv: Dict[str, Dict[str, Any]] = {}

        # 1. Scan conversation_summaries.db across appDataDirs
        for app_dir in find_app_data_dirs():
            db_path = app_dir / "conversation_summaries.db"
            if not db_path.exists():
                continue
            try:
                conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0)
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT conversation_id, title, preview, step_count, last_modified_time, "
                    "workspace_uris, status, agent_name, parent_conversation_id, nesting_depth, "
                    "not_fully_idle, killed, raw_summary "
                    "FROM conversation_summaries "
                    "WHERE parent_conversation_id != '' OR agent_name != '' "
                    "ORDER BY last_modified_time DESC LIMIT 100"
                ).fetchall()
                conn.close()
                for r in rows:
                    d = dict(r)
                    cid = d["conversation_id"]
                    parent_id = d.get("parent_conversation_id") or ""
                    if root_conversation_id and not include_all:
                        if parent_id != root_conversation_id and cid != root_conversation_id:
                            continue

                    raw = d.pop("raw_summary", None)
                    role = d.get("title") or ""
                    model_opt = ""
                    if isinstance(raw, (bytes, bytearray)):
                        strings = [s.decode("utf-8", "ignore") for s in re.findall(rb"[ -~]{3,80}", raw)]
                        ag_name = d.get("agent_name", "")
                        if ag_name and ag_name in strings:
                            idx = len(strings) - 1 - strings[::-1].index(ag_name)
                            if idx + 1 < len(strings):
                                candidate = strings[idx + 1].rstrip("0123456789 ")
                                if candidate and not candidate.startswith("file://"):
                                    role = candidate
                            if idx + 2 < len(strings):
                                m_cand = strings[idx + 2].strip()
                                if m_cand in ("inherit", "flash_lite", "flash", "pro"):
                                    model_opt = m_cand

                    agent_name = d.get("agent_name") or "subagent"
                    engine = "codex" if agent_name == "codex" or cid in self.sessions_by_conv else "gemini"
                    tp = app_dir / "brain" / cid / ".system_generated" / "logs" / "transcript.jsonl"
                    tr = summarize_transcript_file(tp, max_steps=15)

                    raw_status = str(d.get("status") or "")
                    if d.get("killed"):
                        norm_status = "killed"
                    elif "RUNNING" in raw_status or d.get("not_fully_idle"):
                        norm_status = "working"
                    elif tr.get("lastError"):
                        norm_status = "error"
                    else:
                        norm_status = "idle"

                    workspace_path = ""
                    try:
                        uris = json.loads(d.get("workspace_uris") or "[]")
                        if uris and isinstance(uris[0], str):
                            workspace_path = urllib.parse.unquote(uris[0].replace("file://", ""))
                    except Exception:
                        pass

                    items_by_conv[cid] = {
                        "conversationId": cid,
                        "parentConversationId": parent_id,
                        "nestingDepth": d.get("nesting_depth", 1),
                        "engine": engine,
                        "agentName": agent_name,
                        "role": role or f"{agent_name} subagent",
                        "model": model_opt or ("codex" if engine == "codex" else "gemini"),
                        "effort": "",
                        "status": norm_status,
                        "steps": max(int(d.get("step_count") or 0), int(tr.get("steps") or 0)),
                        "activity": tr.get("activity") or d.get("preview") or "",
                        "activityKind": tr.get("activityKind") or "",
                        "lastThinking": tr.get("lastThinking") or "",
                        "lastResponse": tr.get("lastResponse") or "",
                        "cwd": workspace_path,
                        "threadId": None,
                        "activeTurnId": None,
                        "hasDiff": False,
                        "planCount": 0,
                        "pendingApprovals": [],
                        "matchesActiveConversation": bool(
                            root_conversation_id and (parent_id == root_conversation_id or cid == root_conversation_id)
                        ),
                        "updatedAt": str(d.get("last_modified_time") or ""),
                    }
            except Exception as e:
                logger.warning("Failed scanning conversation_summaries.db: %s", e)

        # 2. Overlay Codex Bridge sessions (authoritative for Codex subagents)
        with self.lock:
            sessions_copy = list(self.sessions_by_conv.values())
            approvals_copy = list(self.pending_approvals.values())

        for sess in sessions_copy:
            cid = sess["conversationId"]
            parent_id = sess.get("parentConversationId") or ""
            tp = self._transcript_path_for(sess)
            tr = summarize_transcript_file(tp, max_steps=15)
            conv_approvals = [a for a in approvals_copy if a.get("conversationId") == cid]

            existing = items_by_conv.get(cid, {})
            items_by_conv[cid] = {
                **existing,
                "conversationId": cid,
                "parentConversationId": parent_id or existing.get("parentConversationId", ""),
                "nestingDepth": existing.get("nestingDepth", 1),
                "engine": "codex",
                "agentName": "codex",
                "role": sess.get("role") or existing.get("role") or "Codex Engineer",
                "model": sess.get("model") or existing.get("model") or "gpt-5.4",
                "effort": sess.get("effort") or "high",
                "status": "awaiting_approval" if conv_approvals else sess.get("status", "idle"),
                "steps": max(int(sess.get("stepCount") or 0), int(tr.get("steps") or 0)),
                "activity": sess.get("currentActivity") or tr.get("activity") or "",
                "activityKind": sess.get("activityKind") or tr.get("activityKind") or "",
                "lastThinking": sess.get("lastThinking") or tr.get("lastThinking") or "",
                "lastResponse": sess.get("lastAgentMessage") or tr.get("lastResponse") or "",
                "cwd": sess.get("cwd") or existing.get("cwd") or "",
                "threadId": sess.get("threadId"),
                "activeTurnId": sess.get("activeTurnId"),
                "hasDiff": bool(sess.get("latestDiff")),
                "planCount": len(sess.get("plan") or []),
                "pendingApprovals": conv_approvals,
                "matchesActiveConversation": bool(
                    root_conversation_id and (parent_id == root_conversation_id or cid == root_conversation_id)
                ),
                "updatedAt": sess.get("updatedAt") or existing.get("updatedAt") or "",
            }

        subagents = list(items_by_conv.values())
        subagents.sort(
            key=lambda x: (
                0 if x.get("matchesActiveConversation") else 1,
                0 if x.get("status") in ("working", "awaiting_approval") else 1,
                x.get("updatedAt", ""),
            ),
            reverse=False,
        )
        return {
            "activeRootConversationId": root_conversation_id or "",
            "codexDaemonConnected": self.codex.is_connected(),
            "codexUserAgent": self.codex.user_agent,
            "lsAddress": self.ls_address,
            "subagents": subagents,
        }

    def inspect_subagent(self, conversation_id: str) -> Dict[str, Any]:
        """
        Detailed non-blocking inspection of EITHER a Codex subagent OR a Gemini native subagent.
        Returns transcript steps, live thinking, tool calls, unified file diffs, and plan checklist.
        """
        roster = self.list_unified_subagents(include_all=True)
        summary = next(
            (s for s in roster["subagents"] if s["conversationId"] == conversation_id),
            None,
        )
        with self.lock:
            codex_sess = self.sessions_by_conv.get(conversation_id)

        tp = (
            self._transcript_path_for(codex_sess)
            if codex_sess
            else (self.app_data / "brain" / conversation_id / ".system_generated" / "logs" / "transcript.jsonl")
        )
        tr = summarize_transcript_file(tp, max_steps=60)

        diff_text = ""
        plan = []
        plan_explanation = ""
        events = []
        turn_history = []

        if codex_sess:
            diff_text = codex_sess.get("latestDiff") or ""
            plan = codex_sess.get("plan") or []
            plan_explanation = codex_sess.get("planExplanation") or ""
            events = codex_sess.get("events") or []
            turn_history = codex_sess.get("turnHistory") or []

        # If diff_text is empty and we have a workspace cwd (or for Gemini subagents), fetch live git/LS diff
        cwd = (codex_sess.get("cwd") if codex_sess else None) or (summary.get("cwd") if summary else "")
        if not diff_text and self.ls_address:
            try:
                ls_diff = self.lsrpc("GetTrajectoryFileDiffs", {"conversationId": conversation_id})
                if ls_diff and ls_diff.get("fileDiffs"):
                    diff_text = json.dumps(ls_diff["fileDiffs"], indent=2)
            except Exception:
                pass
        if not diff_text and cwd and os.path.isdir(cwd):
            try:
                proc = subprocess.run(
                    ["git", "-C", cwd, "diff", "--no-ext-diff", "HEAD"],
                    capture_output=True,
                    text=True,
                    timeout=3.0,
                )
                if proc.returncode == 0 and proc.stdout.strip():
                    diff_text = proc.stdout[:32000]
            except Exception:
                pass

        return {
            "conversationId": conversation_id,
            "summary": summary or {"conversationId": conversation_id, "engine": "codex" if codex_sess else "gemini"},
            "transcript": tr,
            "diff": diff_text,
            "plan": plan,
            "planExplanation": plan_explanation,
            "events": events,
            "turnHistory": turn_history,
        }

    # -------------------------------------------------------------------------
    # Unified Actions (Work seamlessly across BOTH Codex and Gemini subagents!)
    # -------------------------------------------------------------------------

    def steer_subagent(
        self,
        conversation_id: str,
        message: str,
        mode: str = "steer",  # "steer" | "queue" | "interrupt_and_steer"
        additional_context: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Mid-turn steers EITHER a Codex subagent OR a Gemini native subagent!
        - Codex:
          - mode == "queue" -> `thread/queue/add`
          - mode == "interrupt_and_steer" -> `turn/interrupt` + `turn/start`
          - mode == "steer" -> `turn/steer` if a turn is active, else `turn/start`
        - Gemini native (`self`, `research`, etc.):
          - If mode == "interrupt_and_steer", first calls `CancelCascadeInvocation` (with notifyParent=false)
          - Calls `SendAgentMessage` into the Gemini subagent conversation so `messages.Hub`
            injects the steering instruction at the next step boundary (or wakes it up if idle)!
        """
        with self.lock:
            codex_sess = self.sessions_by_conv.get(conversation_id)

        if codex_sess:
            if mode == "queue":
                return self.dispatch_subagent_input(conversation_id, f"[QUEUE] {message}")
            if mode == "interrupt_and_steer":
                return self.dispatch_subagent_input(conversation_id, f"[INTERRUPT] {message}")
            if additional_context and codex_sess.get("activeTurnId") and codex_sess.get("threadId"):
                res = self.codex.turn_steer(
                    thread_id=codex_sess["threadId"],
                    expected_turn_id=codex_sess["activeTurnId"],
                    steer_text=message,
                    additional_context=additional_context,
                )
                self._record_event(codex_sess, "turn/steer", {"text": message[:300]})
                return {"engine": "codex", "action": "turn/steer", "conversationId": conversation_id, "result": res}
            return self.dispatch_subagent_input(conversation_id, message)

        # Gemini native subagent steering via LanguageServerService
        db_meta = self.query_db_conversation_meta(conversation_id)
        parent_id = (db_meta.get("parent_conversation_id") if db_meta else None) or ""

        if mode == "interrupt_and_steer" and self.ls_address:
            try:
                self.lsrpc(
                    "CancelCascadeInvocation",
                    {"cascadeId": conversation_id, "killBackgroundTasks": False, "notifyParent": False},
                )
            except Exception as e:
                logger.warning("CancelCascadeInvocation before steer returned: %s", e)

        send_res = self.send_message_to_agy_conversation(
            recipient_id=conversation_id,
            sender_id=parent_id or None,
            title="Live Steer Instruction",
            content=f"[LIVE STEER INSTRUCTION]: {message}"
            + (f"\n\nAdditional Context:\n{additional_context}" if additional_context else ""),
        )
        return {
            "engine": "gemini",
            "action": "SendAgentMessage",
            "conversationId": conversation_id,
            "mode": mode,
            "result": send_res,
        }

    def interrupt_subagent(self, conversation_id: str) -> Dict[str, Any]:
        """Interrupts the active turn/invocation of either a Codex or Gemini subagent without killing it."""
        with self.lock:
            codex_sess = self.sessions_by_conv.get(conversation_id)

        if codex_sess and codex_sess.get("threadId"):
            return self._execute_directive(codex_sess, "INTERRUPT", "")

        # Gemini native subagent: call CancelCascadeInvocation
        res = self.lsrpc(
            "CancelCascadeInvocation",
            {"cascadeId": conversation_id, "killBackgroundTasks": False, "notifyParent": False},
        )
        return {"engine": "gemini", "action": "CancelCascadeInvocation", "conversationId": conversation_id, "result": res}

    def kill_subagent(self, conversation_id: str) -> Dict[str, Any]:
        """Terminates a Codex or Gemini subagent permanently."""
        with self.lock:
            codex_sess = self.sessions_by_conv.get(conversation_id)

        results: Dict[str, Any] = {"conversationId": conversation_id}
        if codex_sess and codex_sess.get("threadId"):
            tid = codex_sess["threadId"]
            active_turn = codex_sess.get("activeTurnId")
            if active_turn:
                try:
                    self.codex.turn_interrupt(thread_id=tid, turn_id=active_turn)
                except Exception:
                    pass
            try:
                self.codex.thread_unsubscribe(thread_id=tid)
            except Exception:
                pass
            with self.lock:
                codex_sess["activeTurnId"] = None
                codex_sess["status"] = "killed"
                codex_sess["currentActivity"] = "Killed"
                self._save_state()
            results["codex"] = "terminated"

        if self.ls_address:
            try:
                results["lsForceStop"] = self.lsrpc("ForceStopCascadeTree", {"conversationId": conversation_id})
            except Exception as e:
                results["lsForceStopError"] = str(e)
        return results

    def revert_subagent(self, conversation_id: str, step_or_turns: int = 1) -> Dict[str, Any]:
        """
        Reverts a subagent:
        - For Codex: calls `thread/revert(turnsToDrop=step_or_turns)`
        - For Gemini: calls `RevertToCascadeStep(cascadeId=conversation_id, stepIndex=step_or_turns)`
        """
        with self.lock:
            codex_sess = self.sessions_by_conv.get(conversation_id)

        if codex_sess and codex_sess.get("threadId"):
            return self._execute_directive(codex_sess, f"REVERT:{max(1, step_or_turns)}", "")

        res = self.lsrpc(
            "RevertToCascadeStep",
            {"cascadeId": conversation_id, "stepIndex": step_or_turns},
        )
        return {
            "engine": "gemini",
            "action": "RevertToCascadeStep",
            "conversationId": conversation_id,
            "stepIndex": step_or_turns,
            "result": res,
        }

    def fork_subagent(
        self,
        conversation_id: str,
        before_turn_id: Optional[str] = None,
        new_role: Optional[str] = None,
        prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Forks a Codex subagent thread via `thread/fork` (or forks a Gemini subagent into a new Codex thread!).
        """
        with self.lock:
            codex_sess = self.sessions_by_conv.get(conversation_id)

        if codex_sess and codex_sess.get("threadId"):
            fork_res = self.codex.thread_fork(
                thread_id=codex_sess["threadId"],
                before_turn_id=before_turn_id,
                cwd=codex_sess.get("cwd"),
            )
            new_thread = (fork_res.get("thread") or {}).get("id")
            new_conv_id = f"codex-fork-{uuid.uuid4().hex[:12]}"
            with self.lock:
                self.sessions_by_conv[new_conv_id] = {
                    **codex_sess,
                    "conversationId": new_conv_id,
                    "threadId": new_thread,
                    "activeTurnId": None,
                    "role": new_role or f"{codex_sess.get('role', 'Codex')} (Fork)",
                    "status": "idle",
                    "createdAt": utc_now_iso(),
                    "updatedAt": utc_now_iso(),
                }
                if new_thread:
                    self.conv_by_thread[new_thread] = new_conv_id
                self._save_state()
            if prompt and new_thread:
                self.dispatch_subagent_input(new_conv_id, prompt)
            return {
                "action": "thread/fork",
                "sourceConversationId": conversation_id,
                "newConversationId": new_conv_id,
                "newThreadId": new_thread,
            }

        # Cross-engine fork: Gemini subagent -> Codex thread!
        return self.handoff_subagent(
            source_conversation_id=conversation_id,
            target_engine="codex",
            prompt=prompt or "Continue implementation based on the research findings above.",
            new_role=new_role,
        )

    def handoff_subagent(
        self,
        source_conversation_id: str,
        target_engine: str = "codex",
        prompt: str = "Continue from the previous subagent's findings.",
        new_role: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Cross-engine handoff: extracts the latest findings, thinking, and workspace cwd from
        any Gemini (`self`, `research`) or Codex subagent and launches/steers a subagent on the target engine.
        """
        inspected = self.inspect_subagent(source_conversation_id)
        summary = inspected.get("summary") or {}
        tr = inspected.get("transcript") or {}
        cwd = summary.get("cwd") or os.getcwd()
        parent_id = summary.get("parentConversationId") or ""
        source_role = summary.get("role") or source_conversation_id

        handoff_context = (
            f"### Cross-Subagent Handoff from `{source_role}` (`{source_conversation_id}`)\n"
            f"- **Source Engine**: {summary.get('engine', 'gemini')}\n"
            f"- **Workspace**: `{cwd}`\n"
            f"- **Latest Thinking**:\n{tr.get('lastThinking', '(none)')}\n\n"
            f"- **Latest Findings / Response**:\n{tr.get('lastResponse', '(none)')}\n\n"
            f"---\n### Your Task\n{prompt}"
        )

        if target_engine == "codex":
            new_conv_id = f"codex-handoff-{uuid.uuid4().hex[:12]}"
            res = self.dispatch_subagent_input(
                conversation_id=new_conv_id,
                parent_conversation_id=parent_id,
                cwd=cwd,
                role=new_role or f"Codex Handoff ({source_role})",
                user_input=handoff_context,
            )
            return {
                "action": "handoff_to_codex",
                "sourceConversationId": source_conversation_id,
                "newConversationId": new_conv_id,
                "threadId": res.get("threadId"),
                "turnId": res.get("turnId"),
            }
        else:
            # Handoff to parent or existing Gemini subagent via SendAgentMessage
            target_id = parent_id or source_conversation_id
            send_res = self.send_message_to_agy_conversation(
                recipient_id=target_id,
                sender_id=source_conversation_id,
                title=f"Handoff from {source_role}",
                content=handoff_context,
            )
            return {
                "action": "handoff_to_gemini",
                "sourceConversationId": source_conversation_id,
                "targetConversationId": target_id,
                "result": send_res,
            }

    def configure_subagent(
        self,
        conversation_id: str,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        approval_mode: Optional[str] = None,
        compact: bool = False,
    ) -> Dict[str, Any]:
        with self.lock:
            codex_sess = self.sessions_by_conv.get(conversation_id)
        if not codex_sess:
            raise ValueError(f"Conversation {conversation_id} is not a Codex subagent session")

        tid = codex_sess.get("threadId")
        active_turn = codex_sess.get("activeTurnId")
        out: Dict[str, Any] = {"conversationId": conversation_id, "threadId": tid}
        with self.lock:
            if model is not None:
                codex_sess["model"] = model
            if effort is not None:
                codex_sess["effort"] = effort
            if approval_mode in ("auto_accept", "interactive"):
                codex_sess["approvalMode"] = approval_mode
            self._save_state()

        if tid and active_turn and (model is not None or effort is not None):
            out["settingsUpdate"] = self.codex.turn_settings_update(
                thread_id=tid,
                turn_id=active_turn,
                model=model,
                effort=effort,
            )
        if tid and compact:
            out["compact"] = self.codex.thread_compact_start(thread_id=tid)
        return out


# -----------------------------------------------------------------------------
# HTTP Server Handler
# -----------------------------------------------------------------------------

BRIDGE_INSTANCE: Optional[UnifiedSubagentBridge] = None


class BridgeHttpHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        # Suppress noisy poll logs unless error
        pass

    def _send_json(self, status: int, data: Any) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self._send_json(200, {"ok": True})

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        return json.loads(raw) if raw.strip() else {}

    def do_GET(self) -> None:
        assert BRIDGE_INSTANCE is not None
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)

        try:
            if parsed.path == "/api/health":
                try:
                    BRIDGE_INSTANCE.codex.connect()
                except Exception as e:
                    logger.debug("Codex connect check on health: %s", e)
                self._send_json(
                    200,
                    {
                        "status": "ok",
                        "codexConnected": BRIDGE_INSTANCE.codex.is_connected(),
                        "codexUserAgent": BRIDGE_INSTANCE.codex.user_agent,
                        "lsAddress": BRIDGE_INSTANCE.ls_address,
                        "activeSessions": len(BRIDGE_INSTANCE.sessions_by_conv),
                    },
                )
                return

            if parsed.path == "/api/subagents":
                root_conv = (qs.get("rootConversationId") or [""])[0] or None
                inc_all = (qs.get("all") or ["true"])[0].lower() != "false"
                data = BRIDGE_INSTANCE.list_unified_subagents(
                    root_conversation_id=root_conv,
                    include_all=inc_all,
                )
                self._send_json(200, data)
                return

            if parsed.path == "/api/subagent/inspect":
                conv_id = (qs.get("conversationId") or [""])[0]
                if not conv_id:
                    self._send_json(400, {"error": "conversationId query parameter is required"})
                    return
                self._send_json(200, BRIDGE_INSTANCE.inspect_subagent(conv_id))
                return

            self._send_json(404, {"error": f"Unknown GET endpoint: {parsed.path}"})
        except Exception as e:
            logger.exception("Error in GET %s", self.path)
            self._send_json(500, {"error": str(e)})

    def do_POST(self) -> None:
        assert BRIDGE_INSTANCE is not None
        parsed = urllib.parse.urlparse(self.path)
        try:
            body = self._read_json_body()

            if body.get("lsAddress") or body.get("csrfToken"):
                BRIDGE_INSTANCE.update_ls_credentials(
                    ls_address=body.get("lsAddress"),
                    csrf_token=body.get("csrfToken"),
                    agentapi_exe=body.get("agentapiExe"),
                )

            if parsed.path == "/api/ls-credentials":
                self._send_json(200, {"ok": True, "lsAddress": BRIDGE_INSTANCE.ls_address})
                return

            if parsed.path == "/api/dispatch":
                conv_id = body.get("conversationId") or f"codex-{uuid.uuid4().hex[:12]}"
                res = BRIDGE_INSTANCE.dispatch_subagent_input(
                    conversation_id=conv_id,
                    user_input=body.get("userInput") or body.get("prompt") or "",
                    parent_conversation_id=body.get("parentConversationId"),
                    cwd=body.get("cwd"),
                    transcript_path=body.get("transcriptPath"),
                    role=body.get("role"),
                    model=body.get("model"),
                    effort=body.get("effort"),
                    sandbox=body.get("sandbox", "workspace-write"),
                    approval_policy=body.get("approvalPolicy", "never"),
                    ls_address=body.get("lsAddress"),
                    csrf_token=body.get("csrfToken"),
                )
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/steer":
                res = BRIDGE_INSTANCE.steer_subagent(
                    conversation_id=body["conversationId"],
                    message=body.get("message", ""),
                    mode=body.get("mode", "steer"),
                    additional_context=body.get("additionalContext"),
                )
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/interrupt":
                res = BRIDGE_INSTANCE.interrupt_subagent(body["conversationId"])
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/kill":
                res = BRIDGE_INSTANCE.kill_subagent(body["conversationId"])
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/revert":
                res = BRIDGE_INSTANCE.revert_subagent(
                    conversation_id=body["conversationId"],
                    step_or_turns=int(body.get("stepOrTurns", 1)),
                )
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/fork":
                res = BRIDGE_INSTANCE.fork_subagent(
                    conversation_id=body["conversationId"],
                    before_turn_id=body.get("beforeTurnId"),
                    new_role=body.get("newRole"),
                    prompt=body.get("prompt"),
                )
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/handoff":
                res = BRIDGE_INSTANCE.handoff_subagent(
                    source_conversation_id=body["sourceConversationId"],
                    target_engine=body.get("targetEngine", "codex"),
                    prompt=body.get("prompt", "Continue from the previous subagent's findings."),
                    new_role=body.get("newRole"),
                )
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/configure":
                res = BRIDGE_INSTANCE.configure_subagent(
                    conversation_id=body["conversationId"],
                    model=body.get("model"),
                    effort=body.get("effort"),
                    approval_mode=body.get("approvalMode"),
                    compact=bool(body.get("compact", False)),
                )
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/approve":
                count = BRIDGE_INSTANCE.resolve_pending_approvals_for_conversation(
                    conversation_id=body.get("conversationId", ""),
                    decision=body.get("decision", "accept"),
                    approval_id=body.get("approvalId"),
                )
                self._send_json(200, {"ok": True, "resolvedCount": count})
                return

            self._send_json(404, {"error": f"Unknown POST endpoint: {parsed.path}"})
        except Exception as e:
            logger.exception("Error in POST %s", self.path)
            self._send_json(500, {"error": str(e)})


def ensure_bridge_daemon_running(port: int = BRIDGE_PORT) -> str:
    """Client helper: checks if bridge HTTP daemon is running; spawns it if not."""
    base_url = f"http://{BRIDGE_HOST}:{port}"
    try:
        with urllib.request.urlopen(f"{base_url}/api/health", timeout=1.0) as resp:
            if resp.status == 200:
                return base_url
    except Exception:
        pass

    log_path = primary_app_data_dir() / "log" / "codex_bridge_daemon.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "a", encoding="utf-8")
    subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--port", str(port)],
        stdout=log_file,
        stderr=log_file,
        start_new_session=True,
        env=os.environ.copy(),
    )

    deadline = time.time() + 6.0
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/api/health", timeout=0.8) as resp:
                if resp.status == 200:
                    return base_url
        except Exception:
            time.sleep(0.15)
    raise RuntimeError(f"Failed to start codex_bridge_daemon.py on {base_url}")


def main() -> None:
    global BRIDGE_INSTANCE
    parser = argparse.ArgumentParser(description="Antigravity Codex & Unified Subagent Bridge Daemon")
    parser.add_argument("--host", default=BRIDGE_HOST)
    parser.add_argument("--port", type=int, default=BRIDGE_PORT)
    args = parser.parse_args()

    BRIDGE_INSTANCE = UnifiedSubagentBridge()
    try:
        BRIDGE_INSTANCE.codex.connect()
    except Exception as e:
        logger.warning("Initial connection to Codex daemon deferred: %s", e)

    server = ThreadingHTTPServer((args.host, args.port), BridgeHttpHandler)
    logger.info("Unified Subagent Bridge Daemon listening on http://%s:%d", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        BRIDGE_INSTANCE.codex.close()


if __name__ == "__main__":
    main()
