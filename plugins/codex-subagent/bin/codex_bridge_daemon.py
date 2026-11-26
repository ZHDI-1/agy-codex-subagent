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
import copy
import datetime
import json
import logging
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from codex_profiles import (
    PROFILE_DIR, apply_files, definition_changes, live_sdk_instance, load_config, load_profiles, normalize_profile,
    read_codex_config_toml, resolve_effective_permissions, validate_name, validate_settings, write_object,
)
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
    r"^\s*\[(STEER|QUEUE|INTERRUPT|FORCE|REVERT(?::\d+)?|COMPACT|APPROVE|DECLINE|MODEL:[^\]]+|EFFORT:[^\]]+|ADOPT:[^\]]+|PERMISSION:[^\]]+)\]\s*(.*)$",
    re.IGNORECASE | re.DOTALL,
)


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def find_app_data_dirs() -> List[Path]:
    override = os.environ.get("AGY_CODEX_APP_DATA_DIR")
    if override:
        return [Path(override).resolve()]
    dirs: List[Path] = []
    env_data = os.environ.get("ANTIGRAVITY_EXECUTABLE_DATA_DIR", "")
    if env_data:
        p = Path(env_data).resolve()
        for parent in (p, *p.parents):
            if (parent / "brain").is_dir():
                dirs.append(parent)
                break
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
                "error": str(step.get("error") or "")[:600],
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
        self.turn_changed = threading.Condition(self.lock)
        self._conversation_locks: Dict[str, threading.RLock] = {}
        self._thread_connections: Dict[str, Any] = {}
        self._approval_replies: Dict[str, Callable[..., None]] = {}

        # Language Server credentials (updated dynamically from env / hooks / sidecar)
        self.ls_address: str = os.environ.get("ANTIGRAVITY_LS_ADDRESS", "")
        self.csrf_token: str = os.environ.get("ANTIGRAVITY_CSRF_TOKEN", "")
        self.agentapi_exe: str = os.environ.get("ANTIGRAVITY_AGENTAPI_EXE", (shutil.which("agy") or "agy"))

        # Session maps:
        # sessions_by_conv: conversationId -> session dict
        # conv_by_thread: codex threadId -> conversationId
        self.sessions_by_conv: Dict[str, Dict[str, Any]] = {}
        self.conv_by_thread: Dict[str, str] = {}

        # Per-item live buffers for active Codex turns: itemId -> dict
        self._item_buffers: Dict[tuple, Dict[str, Any]] = {}
        # Pending approval requests: approvalId -> {req_id, method, params, conversationId, threadId, createdAt}
        self.pending_approvals: Dict[str, Dict[str, Any]] = {}

        self.profiles: Dict[str, Dict[str, Any]] = {}
        self._load_state()

        self.codex = CodexAppServerClient()
        self.codex.add_notification_handler(self._on_codex_notification)
        self.codex.set_server_request_handler(self._on_codex_server_request)

    # -------------------------------------------------------------------------
    # Persistence & Credentials
    # -------------------------------------------------------------------------

    def _load_state(self) -> None:
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text(encoding="utf-8"))
                self.ls_address = self.ls_address or data.get("lsAddress", "")
                self.csrf_token = self.csrf_token or data.get("csrfToken", "")
                self.profiles = {name: normalize_profile(profile) for name, profile in data.get("profiles", {}).items()}
                for conv_id, sess in (data.get("sessions") or {}).items():
                    if not isinstance(sess, dict):
                        continue
                    if sess.get("activeTurnId") or sess.get("status") in ("starting", "initializing", "interrupting"):
                        sess.update(status="error", activeTurnId=None, currentActivity="Bridge restarted during turn")
                    if sess.get("idleSignal") == "pending":
                        sess["idleSignal"] = "failed"
                    # Re-established by a live executor, including when it reconnects after a restart.
                    sess.pop("sdkInstanceId", None)
                    self.sessions_by_conv[conv_id] = sess
                    if sess.get("threadId"):
                        self.conv_by_thread[sess["threadId"]] = conv_id
            except (OSError, ValueError, TypeError, AttributeError) as e:
                logger.warning("Failed to load bridge state file %s: %s", self.state_file, e)
        self.profiles.update(load_profiles())

    def _save_state(self, *, required: bool = False) -> None:
        try:
            with self.lock:
                write_object(self.state_file, {
                    "lsAddress": self.ls_address,
                    "csrfToken": self.csrf_token,
                    "updatedAt": utc_now_iso(),
                    "sessions": self.sessions_by_conv,
                    "profiles": self.profiles,
                })
        except OSError as e:
            if required:
                raise
            logger.warning("Failed to save bridge state: %s", e)

    def list_profiles(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "profiles": [dict(self.profiles[name]) for name in sorted(self.profiles)],
                "defaults": load_config(),
                "codexConfigToml": read_codex_config_toml(),
            }

    def define_profile(self, body: Dict[str, Any]) -> Dict[str, Any]:
        name = validate_name(body.get("name"))
        with self.lock:
            previous = self.profiles.get(name)
            profile = normalize_profile(body, previous)
            changes = definition_changes(name, profile)
            definitions = [str(path) for path, value in changes.items() if value]
            changes[PROFILE_DIR / f"{name}.json"] = json.dumps(profile, ensure_ascii=False, indent=2) + "\n"
            updated = {**self.profiles, name: profile}
            self._persist_profiles(updated, changes)
        return {"profile": profile, "definitions": definitions}

    def delete_profile(self, name: str) -> Dict[str, Any]:
        validate_name(name)
        with self.lock:
            if name not in self.profiles:
                return {"deleted": False, "name": name}
            updated = {key: profile for key, profile in self.profiles.items() if key != name}
            changes = definition_changes(name, None)
            changes[PROFILE_DIR / f"{name}.json"] = None
            self._persist_profiles(updated, changes)
        return {"deleted": True, "name": name}

    def _persist_profiles(self, updated: Dict[str, Any], changes: Dict[Path, Optional[str]]) -> None:
        changes[self.state_file] = json.dumps({
            "lsAddress": self.ls_address, "csrfToken": self.csrf_token,
            "updatedAt": utc_now_iso(), "sessions": self.sessions_by_conv, "profiles": updated,
        }, ensure_ascii=False, indent=2) + "\n"
        apply_files(changes)
        self.profiles = updated

    @staticmethod
    def _validate_conversation_id(conversation_id: str) -> None:
        if not isinstance(conversation_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", conversation_id):
            raise ValueError("Invalid conversationId")

    def _conversation_lock(self, conversation_id: str) -> threading.RLock:
        self._validate_conversation_id(conversation_id)
        with self.lock:
            return self._conversation_locks.setdefault(conversation_id, threading.RLock())

    def _new_session(self, conversation_id: str, agent_name: str) -> Dict[str, Any]:
        settings = load_config()
        if agent_name != "codex":
            if agent_name not in self.profiles:
                raise ValueError(f"Unknown Codex profile {agent_name!r}")
            settings.update(self.profiles[agent_name])
        perm_src, eff_ap, eff_sb = resolve_effective_permissions(
            settings.get("permissionSource"),
            settings.get("approvalPolicy"),
            settings.get("sandboxMode"),
        )
        return {
            "conversationId": conversation_id, "parentConversationId": "", "agentName": agent_name,
            "engine": "codex", "role": "Codex Subagent", "cwd": "", "transcriptPath": "",
            "threadId": None, "activeTurnId": None, "status": "idle", "executionMode": "preInvocation",
            "model": settings["model"], "effort": settings["effort"],
            "permissionSource": perm_src, "sandbox": eff_sb, "approvalPolicy": eff_ap,
            "approvalMode": "interactive",
            "systemPrompt": settings.get("systemPrompt", ""),
            "stepCount": 0, "nextStepIndex": 0, "turnHistory": [], "events": [],
            "createdAt": utc_now_iso(), "updatedAt": utc_now_iso(),
        }

    def register_sdk(self, body: Dict[str, Any]) -> Dict[str, Any]:
        conversation_id, instance = body["conversationId"], body["sdkInstanceId"]
        if not isinstance(instance, str) or not instance:
            raise ValueError("sdkInstanceId is required")
        pid = body.get("sdkPid")
        if pid is not None and (type(pid) is not int or pid <= 0):
            raise ValueError("Invalid sdkPid")
        with self._conversation_lock(conversation_id), self.lock:
            session = self.sessions_by_conv.get(conversation_id)
            agent_name = body.get("agentName") or "codex"
            if session is None:
                session = self._new_session(conversation_id, agent_name)
                self.sessions_by_conv[conversation_id] = session
            elif session.get("agentName", "codex") != agent_name:
                raise ValueError("SDK executor agent name differs from the conversation profile")
            if body.get("parentConversationId"):
                session["parentConversationId"] = body["parentConversationId"]
            if body.get("lsAddress"):
                session.update(lsAddress=body["lsAddress"], csrfToken=body.get("csrfToken") or "")
            session.update(agentName=agent_name, executionMode="sdkExecutor", sdkInstanceId=instance, sdkPid=pid)
            self._save_state()
        return self.sdk_status(conversation_id, instance)

    def sdk_status(self, conversation_id: str, instance: str) -> Dict[str, Any]:
        self._validate_conversation_id(conversation_id)
        with self.lock:
            session = self.sessions_by_conv.get(conversation_id)
            if not session or session.get("sdkInstanceId") != instance:
                return {"registered": False, "replaced": bool(session and session.get("sdkInstanceId"))}
            return {
                "registered": True, "status": session["status"], "activeTurnId": session.get("activeTurnId"),
                "lastTerminalTurnId": session.get("lastTerminalTurnId"),
                "idleSignal": session.get("idleSignal"), "idleSignalTurnId": session.get("idleSignalTurnId"),
            }

    def unregister_sdk(self, conversation_id: str, instance: str) -> None:
        with self._conversation_lock(conversation_id), self.lock:
            session = self.sessions_by_conv.get(conversation_id)
            if session and session.get("sdkInstanceId") == instance:
                session.pop("sdkInstanceId")
                self._save_state()

    def update_ls_credentials(
        self,
        ls_address: Optional[str] = None,
        csrf_token: Optional[str] = None,
        agentapi_exe: Optional[str] = None,
    ) -> None:
        with self.lock:
            before = (self.ls_address, self.csrf_token, self.agentapi_exe)
            if ls_address:
                self.ls_address = ls_address
                self.csrf_token = csrf_token or ""
            if agentapi_exe:
                self.agentapi_exe = agentapi_exe
            if before != (self.ls_address, self.csrf_token, self.agentapi_exe):
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
        with self.lock:
            addr = ls_address or self.ls_address
            token = csrf_token if csrf_token is not None else self.csrf_token
        if not addr:
            raise RuntimeError("Language Server address (ANTIGRAVITY_LS_ADDRESS) is not known yet")
        base = addr if addr.startswith(("http://", "https://")) else f"http://{addr}"
        url = f"{base.rstrip('/')}/exa.language_server_pb.LanguageServerService/{method}"
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if token:
            headers["x-codeium-csrf-token"] = token
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw.strip() else {}

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

        exe = self.agentapi_exe or (shutil.which("agy") or "agy")
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
                with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0)) as conn:
                    conn.row_factory = sqlite3.Row
                    row = conn.execute(
                        "SELECT conversation_id, title, preview, step_count, workspace_uris, status, "
                        "agent_name, parent_conversation_id, nesting_depth, not_fully_idle, killed, raw_summary "
                        "FROM conversation_summaries WHERE conversation_id = ?",
                        (conversation_id,),
                    ).fetchone()
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
        session = self.sessions_by_conv.get(conversation_id) or {}
        db_dir = Path(session.get("appDataDir") or self.app_data)
        db_path = db_dir / "conversation_summaries.db"
        if not db_path.exists():
            return
        try:
            with closing(sqlite3.connect(str(db_path), timeout=0.25)) as conn:
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
        sandbox: Optional[str] = None,
        approval_policy: Optional[str] = None,
        ls_address: Optional[str] = None,
        csrf_token: Optional[str] = None,
        agent_name: Optional[str] = None,
        execution_mode: Optional[str] = None,
        sdk_instance: Optional[str] = None,
        input_id: Optional[str] = None,
        additional_context: Optional[str] = None,
        bypass_quota_check: bool = False,
    ) -> Dict[str, Any]:
        clean_text = user_input.strip()
        extracted_parent = parent_conversation_id or ""
        wrapped = SYSTEM_MSG_TAG_RE.search(clean_text) or MESSAGE_FROM_RE.match(clean_text)
        if wrapped:
            extracted_parent = extracted_parent or wrapped.group(1).strip()
            clean_text = wrapped.group(2).strip()

        with self._conversation_lock(conversation_id):
            with self.lock:
                session = self.sessions_by_conv.get(conversation_id)
            if not session or not session.get("threadId"):
                db_meta = self.query_db_conversation_meta(conversation_id) or {}
                extracted_parent = extracted_parent or db_meta.get("parent_conversation_id", "")
                role = role or db_meta.get("role") or db_meta.get("title")
                db_agent_name = db_meta.get("agent_name")
                with self.lock:
                    if not agent_name and db_agent_name in self.profiles:
                        agent_name = db_agent_name
                if not cwd:
                    try:
                        uris = json.loads(db_meta.get("workspace_uris") or "[]")
                        if uris and uris[0].startswith("file://"):
                            cwd = urllib.parse.unquote(uris[0][len("file://"):])
                    except (ValueError, TypeError, AttributeError):
                        pass

            with self.lock:
                if execution_mode == "sdkExecutor" and (
                    not sdk_instance or not session or session.get("sdkInstanceId") != sdk_instance
                ):
                    raise ValueError("SDK executor must register before dispatch")
                if session and execution_mode == "preInvocation" and live_sdk_instance(session):
                    return {"action": "sdkExecutor", "conversationId": conversation_id}
                if session is None:
                    session = self._new_session(conversation_id, agent_name or "codex")
                    self.sessions_by_conv[conversation_id] = session
                if agent_name and session.get("agentName", "codex") != agent_name:
                    raise ValueError("The conversation is already bound to a different profile")
                if execution_mode == "preInvocation":
                    session.pop("sdkInstanceId", None)
                    session.pop("sdkPid", None)
                    session["executionMode"] = "preInvocation"
                if session["status"] == "killed":
                    raise ValueError(f"Conversation {conversation_id} has been killed")
                if input_id and input_id in session.get("dispatchedInputs", {}):
                    return session["dispatchedInputs"][input_id]
                if not clean_text:
                    return {"action": "no_input", "conversationId": conversation_id}
                settings = validate_settings({
                    "executionMode": session.get("executionMode", "preInvocation"),
                    "model": model or session.get("model") or load_config()["model"],
                    "effort": effort or session.get("effort") or load_config()["effort"],
                    "approvalPolicy": approval_policy or session.get("approvalPolicy", "never"),
                    "sandboxMode": sandbox or session.get("sandbox", "workspace-write"),
                })
                session.update(model=settings["model"], effort=settings["effort"])
                if not session.get("threadId"):
                    session.update(sandbox=settings["sandboxMode"], approvalPolicy=settings["approvalPolicy"])
                if extracted_parent and not session.get("parentConversationId"):
                    session["parentConversationId"] = extracted_parent
                if cwd or not session.get("cwd"):
                    session["cwd"] = cwd or os.getcwd()
                if transcript_path and transcript_path != session.get("transcriptPath"):
                    session.update(transcriptPath=transcript_path, nextStepIndex=0)
                if not session.get("appDataDir"):
                    for app_data in find_app_data_dirs():
                        if (app_data / "brain" / conversation_id).is_dir():
                            session["appDataDir"] = str(app_data)
                            break
                if role and session.get("role") in ("", None, "Codex Subagent"):
                    session["role"] = role
                if ls_address:
                    session.update(lsAddress=ls_address, csrfToken=csrf_token or "")

            try:
                directive = PREFIX_CMD_RE.match(clean_text)
                if directive and directive[1].upper() == "FORCE":
                    bypass_quota_check = True
                    forced_body = directive[2].strip()
                    with self.lock:
                        pending_prompt = session.pop("pendingQuotaPrompt", "")
                    clean_text = forced_body or pending_prompt
                    if not clean_text:
                        return {"action": "no_input", "conversationId": conversation_id}
                    directive = PREFIX_CMD_RE.match(clean_text)

                if directive and directive[1].upper().startswith("ADOPT:"):
                    raw_target_tid = directive[1].split(":", 1)[1].strip()
                    result = self.adopt_codex_thread(
                        thread_id=raw_target_tid,
                        conversation_id=conversation_id,
                        parent_conversation_id=session.get("parentConversationId") or extracted_parent,
                        role=role or session.get("role"),
                        prompt=directive[2].strip() or None,
                    )
                else:
                    is_new_thread = not bool(session.get("threadId"))
                    if is_new_thread and not bypass_quota_check and not directive:
                        quota = self.get_codex_quota()
                        if quota.get("belowThreshold"):
                            result = self._handle_quota_blocked(session, clean_text, quota)
                            return result
                    self._ensure_thread(session)
                    if directive:
                        result = self._execute_directive(session, directive[1].upper(), directive[2].strip())
                    else:
                        result = self._dispatch_turn(session, clean_text, additional_context)
            except Exception as e:
                with self.lock:
                    if session["status"] == "starting" or not session.get("threadId"):
                        session.update(status="error", currentActivity=f"Dispatch failed: {e}", activityKind="")
                        self._sync_sqlite_status(conversation_id, "CASCADE_RUN_STATUS_IDLE", session.get("stepCount", 0))
                        self._save_state()
                raise
            if input_id:
                with self.lock:
                    inputs = session.setdefault("dispatchedInputs", {})
                    inputs[input_id] = result
                    if len(inputs) > 80:
                        inputs.pop(next(iter(inputs)))
                    self._save_state()
            return result

    def _ensure_thread(self, session: Dict[str, Any]) -> None:
        self.codex.connect()
        connection = self.codex.connection
        tid = session.get("threadId")
        if tid:
            with self.lock:
                same_connection = tid in self._thread_connections and self._thread_connections[tid] is connection
                if not same_connection and tid in self._thread_connections:
                    self._clear_disconnected_turn(session)
            if not same_connection:
                self.codex.thread_resume(tid, cwd=session["cwd"])
                with self.lock:
                    self._thread_connections[tid] = self.codex.connection
            return

        instructions = (
            "You are running as a native OpenAI Codex Subagent inside Google Antigravity (agy).\n"
            f"Your Assigned Role: {session['role']}\n"
            f"Subagent Conversation ID: {session['conversationId']}\n"
            f"Parent Conversation ID: {session['parentConversationId']}\n"
            "Use agy_notify_parent to send interim progress and agy_list_sibling_subagents to inspect siblings. "
            "Your final response and unified diff summary will be delivered to the parent conversation."
        )
        if session.get("systemPrompt"):
            instructions += f"\n\n{session['systemPrompt']}"
        result = self.codex.thread_start(
            cwd=session["cwd"], model=session["model"], effort=session["effort"],
            approval_policy=session["approvalPolicy"], sandbox=session["sandbox"],
            developer_instructions=instructions, dynamic_tools=DYNAMIC_TOOLS_SPEC,
        )
        tid = (result.get("thread") or {}).get("id")
        if not tid:
            raise RuntimeError("Codex thread/start did not return a thread ID")
        with self.lock:
            session["threadId"] = tid
            self.conv_by_thread[tid] = session["conversationId"]
            self._thread_connections[tid] = connection
            self._save_state()

    def _dispatch_turn(self, session: Dict[str, Any], text: str, context: Optional[str] = None) -> Dict[str, Any]:
        cid, tid = session["conversationId"], session["threadId"]
        with self.lock:
            active_turn = session.get("activeTurnId")
            if session["status"] == "interrupting":
                raise ValueError("Wait for the interrupted turn to complete before dispatching again")
            self._append_transcript_step(session, {
                "source": "USER_EXPLICIT", "type": "USER_INPUT", "status": "DONE", "content": text,
            })

        if active_turn:
            try:
                result = self.codex.turn_steer(tid, active_turn, text, additional_context=context)
            except CodexRpcError as e:
                # Only retry a steer the server explicitly rejected because its turn ended.
                if "no active turn" not in e.message.lower() and "expected active turn" not in e.message.lower():
                    raise
                with self.turn_changed:
                    self.turn_changed.wait_for(lambda: session.get("activeTurnId") != active_turn, timeout=2)
                    if session.get("activeTurnId"):
                        raise
            else:
                self._record_event(session, "turn/steer", {"turnId": active_turn, "text": text[:300]})
                self._save_state()
                return {"action": "turn/steer", "conversationId": cid, "threadId": tid,
                        "turnId": result.get("turnId", active_turn), "status": "steered"}

        with self.lock:
            session.update(status="starting", currentActivity="Starting Codex turn...", activityKind="thinking",
                           lastAgentMessage="", lastThinking="", latestDiff="", plan=[], planExplanation="")
            self._sync_sqlite_status(cid, "CASCADE_RUN_STATUS_RUNNING", session["stepCount"], text[:160], 1)
            # A pending signal belongs to the preceding native invocation.
            session.update(idleSignal=None, idleSignalTurnId=None)

        prompt = f"{session['systemPrompt']}\n\n{text}" if session.get("systemPrompt") else text
        if context:
            prompt += f"\n\nAdditional context:\n{context}"
        response = self.codex.turn_start(tid, prompt, cwd=session["cwd"], model=session["model"], effort=session["effort"])
        turn = response.get("turn") or {}
        turn_id = turn.get("id")
        if not turn_id:
            raise RuntimeError("Codex turn/start did not return a turn ID")
        with self.lock:
            if session["status"] == "starting" and session.get("lastTerminalTurnId") != turn_id:
                session.update(activeTurnId=turn_id, status="working")
            self._record_event(session, "turn/start", {"turnId": turn_id, "prompt": text[:300]})
            self._save_state()
            status = session["status"]
        return {"action": "turn/start", "conversationId": cid, "threadId": tid, "turnId": turn_id, "status": status}

    def _execute_directive(self, session: Dict[str, Any], directive: str, remainder: str) -> Dict[str, Any]:
        conv_id = session["conversationId"]
        thread_id = session.get("threadId")
        with self.lock:
            active_turn = session.get("activeTurnId")

        if directive == "STEER":
            return self._dispatch_turn(session, remainder)

        if directive == "QUEUE":
            res = self.codex.thread_queue_add(thread_id=thread_id, text=remainder)
            self._record_event(session, "thread/queue/add", {"text": remainder[:300]})
            self._save_state()
            return {"action": "thread/queue/add", "conversationId": conv_id, "threadId": thread_id, "result": res}

        if directive == "INTERRUPT":
            if active_turn:
                with self.lock:
                    if session.get("activeTurnId") == active_turn:
                        session["status"] = "interrupting"
                try:
                    self.codex.turn_interrupt(thread_id=thread_id, turn_id=active_turn)
                except Exception:
                    with self.lock:
                        if session.get("activeTurnId") == active_turn:
                            session["status"] = "working"
                            raise
                self._record_event(session, "turn/interrupt", {"turnId": active_turn})
                self._save_state()
                if remainder:
                    with self.turn_changed:
                        finished = self.turn_changed.wait_for(
                            lambda: session.get("activeTurnId") != active_turn, timeout=10,
                        )
                    if not finished:
                        raise TimeoutError(f"Codex turn {active_turn} has not acknowledged interruption")
            if remainder:
                return self._dispatch_turn(session, remainder)
            return {"action": "turn/interrupt", "conversationId": conv_id, "threadId": thread_id,
                    "interruptedTurnId": active_turn}

        if directive.startswith("REVERT"):
            parts = directive.split(":")
            if active_turn:
                raise ValueError("Wait for the active turn to complete before reverting")
            turns_to_drop = max(1, int(parts[1])) if len(parts) > 1 and parts[1].isdigit() else 1
            before_turn_id = self._resolve_before_turn_id(session, turns_to_drop)
            if not before_turn_id:
                return {"action": "thread/revert", "conversationId": conv_id, "threadId": thread_id, "skipped": "no turns to revert"}
            res = self.codex.thread_revert(thread_id=thread_id, before_turn_id=before_turn_id)
            with self.lock:
                history = session.get("turnHistory", [])
                index = next((i for i, item in enumerate(history) if item.get("turnId") == before_turn_id), 0)
                del history[index:]
            self._record_event(session, "thread/revert", {"beforeTurnId": before_turn_id, "turnsToDrop": turns_to_drop})
            self._save_state()
            return {"action": "thread/revert", "conversationId": conv_id, "threadId": thread_id, "beforeTurnId": before_turn_id, "result": res}

        if directive == "COMPACT":
            res = self.codex.thread_compact_start(thread_id=thread_id)
            self._record_event(session, "thread/compact/start", {})
            self._save_state()
            return {"action": "thread/compact/start", "conversationId": conv_id, "threadId": thread_id, "result": res}

        if directive.startswith(("MODEL:", "EFFORT:")):
            key, value = directive.split(":", 1)
            key = key.lower()
            value = value.strip().lower() if key == "effort" else value.strip()
            result = self.configure_subagent(conv_id, **{key: value})
            if remainder:
                return self._dispatch_turn(session, remainder)
            return {"action": "turn/settings/update", key: value, "result": result.get("settingsUpdate")}

        if directive.startswith("PERMISSION:"):
            spec = directive.split(":", 1)[1].strip()
            spec_lower = spec.lower()
            kwargs: Dict[str, Any] = {}
            if spec_lower in ("bypass", "full", "auto"):
                kwargs["permission_source"] = "bypass"
            elif spec_lower in ("codexconfig", "config", "toml", "inherit"):
                kwargs["permission_source"] = "codexConfig"
            elif ":" in spec_lower:
                ap_part, sb_part = [p.strip() for p in spec_lower.split(":", 1)]
                kwargs.update(permission_source="custom", approval_policy=ap_part, sandbox_mode=sb_part)
            elif spec_lower in ("never", "on-request", "untrusted"):
                kwargs.update(permission_source="custom", approval_policy=spec_lower)
            elif spec_lower in ("read-only", "workspace-write", "danger-full-access"):
                kwargs.update(permission_source="custom", sandbox_mode=spec_lower)
            else:
                raise ValueError(f"Unsupported PERMISSION directive value: {spec}")
            result = self.configure_subagent(conv_id, **kwargs)
            if remainder:
                return self._dispatch_turn(session, remainder)
            return {"action": "thread/settings/update", "conversationId": conv_id, "threadId": thread_id, **result}

        if directive in ("APPROVE", "DECLINE"):
            decision = "accept" if directive == "APPROVE" else "decline"
            resolved = self.resolve_pending_approvals_for_conversation(conv_id, decision=decision)
            return {"action": "approval", "decision": decision, "resolvedCount": resolved}

        raise ValueError(f"Unrecognized directive {directive}")

    def _resolve_before_turn_id(self, session: Dict[str, Any], turns_to_drop: int = 1) -> Optional[str]:
        with self.lock:
            thread_id = session.get("threadId")
            history = list(session.get("turnHistory") or [])
        if len(history) >= turns_to_drop:
            cand = history[-turns_to_drop].get("turnId")
            if cand:
                return str(cand)
        if thread_id:
            try:
                response = self.codex.request("thread/turns/list", {
                    "threadId": thread_id, "sortDirection": "desc", "limit": max(1, turns_to_drop),
                })
                turns = response.get("data") or []
                if turns:
                    return turns[min(len(turns), turns_to_drop) - 1].get("id")
            except Exception as e:
                logger.warning("thread_read during revert failed: %s", e)
        return None

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
        if method == "bridge/disconnected":
            with self.lock:
                for tid, connection in list(self._thread_connections.items()):
                    if connection is params["connection"]:
                        session = self.sessions_by_conv[self.conv_by_thread[tid]]
                        self._clear_disconnected_turn(session)
                        self._thread_connections.pop(tid, None)
                self._save_state()
            return

        thread_id = params.get("threadId")
        with self.lock:
            conv_id = self.conv_by_thread.get(thread_id)
            session = self.sessions_by_conv.get(conv_id) if conv_id else None
            if not session or session["status"] == "killed":
                return
            self._apply_codex_notification(session, method, params)

    def _apply_codex_notification(self, session: Dict[str, Any], method: str, params: Dict[str, Any]) -> None:
        # Ordered callback worker and state lock. RPC, process waits, and LS HTTP belong outside it.
        thread_id = session["threadId"]
        turn = params.get("turn") or {}
        turn_id = turn.get("id") or params.get("turnId")
        active = session.get("activeTurnId")
        if method == "serverRequest/resolved":
            for aid, approval in list(self.pending_approvals.items()):
                if approval["threadId"] == thread_id and approval["rpcRequestId"] == params.get("requestId"):
                    self._remove_approval(aid)
            self._refresh_approval_status(session)
            return
        if not turn_id and not active:
            return
        if turn_id and turn_id == session.get("lastTerminalTurnId"):
            return
        if active and turn_id and active != turn_id:
            return
        if method == "turn/started":
            if not active and session["status"] != "starting":
                session.update(lastAgentMessage="", lastThinking="", latestDiff="", plan=[], planExplanation="")
            session["activeTurnId"] = turn_id
            if session["status"] not in ("awaiting_approval", "interrupting"):
                session.update(status="working", currentActivity="Thinking...", activityKind="thinking")
            session["updatedAt"] = utc_now_iso()
            self._record_event(session, "turn/started", {"turnId": turn_id})
            self._save_state()
            return
        if method == "turn/completed":
            if active or session["status"] == "starting":
                self._on_turn_completed(session, turn)
            return
        if not active:
            return
        item_key = (thread_id, active, (params.get("item") or {}).get("id") or params.get("itemId"))

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
                buf = self._item_buffers.setdefault(item_key, {"type": "reasoning", "text": ""})
                buf["text"] = (buf["text"] + delta)[-16000:]
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
                buf = self._item_buffers.setdefault(item_key, {"type": "agentMessage", "text": ""})
                buf["text"] += delta
                session["lastAgentMessage"] = buf["text"]
            return

        if method in ("item/commandExecution/outputDelta", "item/fileChange/outputDelta"):
            item_id = params.get("itemId", "")
            delta = params.get("delta", "")
            with self.lock:
                buf = self._item_buffers.setdefault(item_key, {"type": "output", "output": ""})
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
                    self._item_buffers[item_key] = {"type": "commandExecution", "command": cmd_str, "output": ""}
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
                    self._item_buffers[item_key] = {"type": "fileChange", "path": first_path, "output": ""}
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
            buf = self._item_buffers.pop(item_key, {})

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

    def _on_turn_completed(self, session: Dict[str, Any], turn: Dict[str, Any]) -> None:
        conv_id, thread_id = session["conversationId"], session["threadId"]
        turn_id = turn["id"]
        result = turn.get("status", "completed")
        error = turn.get("error")
        message = session.get("lastAgentMessage", "").strip()
        thinking = session.get("lastThinking", "").strip()
        latest_diff = session.get("latestDiff", "").strip()
        plan = session.get("plan") or []
        role = session.get("role") or "Codex Subagent"
        parent = session.get("parentConversationId", "")
        credentials = self._session_credentials(session)

        self._clear_turn_resources(thread_id, turn_id)
        status = {"failed": "error", "interrupted": "interrupted"}.get(result, "idle")
        session.update(
            activeTurnId=None, lastTerminalTurnId=turn_id, status=status, activityKind="",
            currentActivity=f"Failed: {error}" if error else f"Turn {result}",
        )
        session.setdefault("turnHistory", []).append({
            "turnId": turn_id, "status": result, "completedAt": utc_now_iso(), "summary": message[:500],
        })
        self._record_event(session, "turn/completed", {"turnId": turn_id, "status": result})
        try:
            self._append_transcript_step(session, {
                "source": "MODEL", "type": "PLANNER_RESPONSE",
                "status": "ERROR" if result == "failed" else "DONE",
                "model": session.get("model") or "codex", "thinking": thinking,
                "response": message or (f"Error: {error}" if error else f"Turn {result}"),
                "tool_calls": [], **({"error": json.dumps(error)} if error else {}),
            })
        except OSError:
            logger.exception("Failed to write completed turn %s to the transcript", turn_id)
        self._sync_sqlite_status(
            conv_id, "CASCADE_RUN_STATUS_IDLE", session["stepCount"], message or f"Turn {result}",
        )
        self._schedule_sdk_idle(session, turn_id)
        self._save_state()
        self.turn_changed.notify_all()

        if parent:
            report = [
                f"### Codex Subagent Report (`{role}`)",
                f"- **Subagent Conversation**: [{role}](conversation://{conv_id})",
                f"- **Codex Thread / Turn**: `{thread_id}` / `{turn_id}` (`{result}`)",
            ]
            if plan:
                report.extend(
                    f"- [{'x' if entry.get('status') == 'completed' else ' '}] {entry.get('step', '')}"
                    for entry in plan if isinstance(entry, dict)
                )
            if message:
                report.append(f"\n{message}")
            if error:
                report.append(f"\n**Error Details**: `{json.dumps(error)}`")
            if latest_diff:
                lines = latest_diff.splitlines()
                excerpt = "\n".join(lines[:60])
                if len(lines) > 60:
                    excerpt += f"\n… ({len(lines) - 60} more lines)"
                report.append(f"\n<details><summary>Unified Diff Summary ({len(lines)} lines)</summary>"
                              f"\n\n```diff\n{excerpt}\n```\n</details>")
            threading.Thread(
                target=self.send_message_to_agy_conversation,
                kwargs={"recipient_id": parent, "sender_id": conv_id,
                        "title": f"Codex Subagent ({role}) Turn Completed", "content": "\n".join(report),
                        **credentials},
                name="codex-parent-report", daemon=True,
            ).start()

    def _session_credentials(self, session: Dict[str, Any]) -> Dict[str, Optional[str]]:
        return {"ls_address": session.get("lsAddress") or self.ls_address,
                "csrf_token": session.get("csrfToken", self.csrf_token)}

    def _schedule_sdk_idle(self, session: Dict[str, Any], turn_id: Optional[str]) -> None:
        if not session.get("sdkInstanceId") or not session.get("lsAddress"):
            return
        session.update(idleSignal="pending", idleSignalTurnId=turn_id)
        threading.Thread(
            target=self._signal_sdk_idle, args=(session, turn_id, session["sdkInstanceId"]),
            name="codex-sdk-idle", daemon=True,
        ).start()

    def _signal_sdk_idle(self, session: Dict[str, Any], turn_id: Optional[str], instance: str) -> None:
        conversation_id = session["conversationId"]
        with self._conversation_lock(conversation_id):
            with self.lock:
                if (session.get("sdkInstanceId") != instance or session.get("idleSignalTurnId") != turn_id
                        or session.get("idleSignal") != "pending" or session.get("activeTurnId")):
                    return
                credentials = self._session_credentials(session)
            try:
                self.lsrpc("SignalExecutableIdle", {"conversationId": conversation_id}, timeout=2, **credentials)
                signal_status = "sent"
            except Exception as e:
                logger.warning("Could not signal SDK executable idle for %s: %s", conversation_id, e)
                signal_status = "failed"
            with self.lock:
                session["idleSignal"] = signal_status
                self._save_state()

    def _clear_disconnected_turn(self, session: Dict[str, Any]) -> None:
        thread_id = session["threadId"]
        turn_id = session.get("activeTurnId")
        self._clear_turn_resources(thread_id)
        if turn_id or session["status"] == "starting":
            session.update(status="error", activeTurnId=None, lastTerminalTurnId=turn_id,
                           currentActivity="Codex connection lost", activityKind="")
            self._record_event(session, "bridge/disconnected", {"turnId": turn_id})
            self._sync_sqlite_status(session["conversationId"], "CASCADE_RUN_STATUS_IDLE", session["stepCount"])
            self._schedule_sdk_idle(session, turn_id)
            self.turn_changed.notify_all()

    def _clear_turn_resources(self, thread_id: str, turn_id: Optional[str] = None) -> None:
        for key in list(self._item_buffers):
            if key[0] == thread_id and (turn_id is None or key[1] == turn_id):
                self._item_buffers.pop(key)
        for aid, approval in list(self.pending_approvals.items()):
            if approval["threadId"] == thread_id and (turn_id is None or approval["turnId"] == turn_id):
                self._remove_approval(aid)

    def _remove_approval(self, approval_id: str) -> None:
        self.pending_approvals.pop(approval_id, None)
        self._approval_replies.pop(approval_id, None)

    def _refresh_approval_status(self, session: Dict[str, Any]) -> None:
        if session["status"] == "awaiting_approval" and not any(
            item["conversationId"] == session["conversationId"] for item in self.pending_approvals.values()
        ):
            session.update(status="working", currentActivity="Running Codex turn...")

    # -------------------------------------------------------------------------
    # Codex ServerRequest Handler (Approvals & Dynamic Tools)
    # -------------------------------------------------------------------------

    def _on_codex_server_request(
        self,
        req_id: int | str,
        method: str,
        params: Dict[str, Any],
        reply: Callable[..., None],
    ) -> Optional[Dict[str, Any]]:
        thread_id = params.get("threadId", "")
        with self.lock:
            conv_id = self.conv_by_thread.get(thread_id, "")
            live_session = self.sessions_by_conv.get(conv_id) if conv_id else None
            session = dict(live_session) if live_session else None

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
                title = str(args.get("title") or "Codex Subagent Interim Update")
                parent_id = session.get("parentConversationId", "") if session else ""
                if parent_id and msg_text:
                    res = self.send_message_to_agy_conversation(
                        recipient_id=parent_id,
                        sender_id=conv_id or None,
                        title=title,
                        content=f"**[Codex Interim Notification (`{thread_id}`)]**\n\n{msg_text}",
                        **self._session_credentials(session),
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
            with self.lock:
                active = live_session.get("activeTurnId") if live_session else None
                turn_id = params.get("turnId") or active
                if not live_session or not active or active != turn_id or live_session["status"] == "interrupting":
                    return self._approval_result(method, params, "decline")
                if live_session.get("approvalMode") == "auto_accept":
                    return self._approval_result(method, params, "accept")

                approval_id = f"appr_{uuid.uuid4().hex[:10]}"
                self.pending_approvals[approval_id] = {
                    "approvalId": approval_id, "rpcRequestId": req_id, "method": method, "params": params,
                    "conversationId": conv_id, "threadId": thread_id, "turnId": turn_id, "createdAt": utc_now_iso(),
                }
                self._approval_replies[approval_id] = reply
                live_session.update(status="awaiting_approval", currentActivity=f"Awaiting approval ({method.split('/')[1]})")
            return None

        return {}

    @staticmethod
    def _approval_result(method: str, params: Dict[str, Any], decision: str) -> Dict[str, Any]:
        if method == "item/permissions/requestApproval":
            return {"permissions": params.get("permissions", {}) if decision == "accept" else {}, "scope": "turn"}
        return {"decision": decision}

    def resolve_pending_approvals_for_conversation(
        self, conversation_id: str, decision: str = "accept", approval_id: Optional[str] = None,
    ) -> int:
        if decision not in ("accept", "decline", "cancel"):
            raise ValueError("Invalid approval decision")
        with self.lock:
            replies = []
            for aid, item in list(self.pending_approvals.items()):
                if item["conversationId"] == conversation_id and (approval_id is None or aid == approval_id):
                    replies.append((self._approval_replies[aid], item))
                    self._remove_approval(aid)
            session = self.sessions_by_conv.get(conversation_id)
            if session:
                self._refresh_approval_status(session)

        count = 0
        for reply, item in replies:
            try:
                reply(result=self._approval_result(item["method"], item["params"], decision))
                count += 1
            except OSError:
                logger.debug("Approval %s closed before delivery", item["approvalId"])
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

        with self.lock:
            codex_names = {"codex", *self.profiles}
            codex_ids = set(self.sessions_by_conv)

        # 1. Scan conversation_summaries.db across appDataDirs
        for app_dir in find_app_data_dirs():
            db_path = app_dir / "conversation_summaries.db"
            if not db_path.exists():
                continue
            try:
                with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0)) as conn:
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute(
                        "SELECT conversation_id, title, preview, step_count, last_modified_time, "
                        "workspace_uris, status, agent_name, parent_conversation_id, nesting_depth, "
                        "not_fully_idle, killed, raw_summary "
                        "FROM conversation_summaries "
                        "WHERE parent_conversation_id != '' OR agent_name != '' "
                        "ORDER BY last_modified_time DESC LIMIT 100"
                    ).fetchall()
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
                    engine = "codex" if agent_name in codex_names or cid in codex_ids else "gemini"
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
            sessions_copy = [dict(session) for session in self.sessions_by_conv.values()]
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
                "agentName": sess.get("agentName", "codex"),
                "executionMode": sess.get("executionMode", "preInvocation"),
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
                "permissionSource": sess.get("permissionSource", "codexConfig"),
                "approvalPolicy": sess.get("approvalPolicy", "never"),
                "sandboxMode": sess.get("sandbox", "danger-full-access"),
                "isAdopted": bool(sess.get("isAdopted")),
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
                0 if x.get("status") in ("starting", "working", "interrupting", "awaiting_approval") else 1,
                x.get("updatedAt", ""),
            ),
            reverse=False,
        )
        return {
            "activeRootConversationId": root_conversation_id or "",
            "codexDaemonConnected": self.codex.is_connected(),
            "codexUserAgent": self.codex.user_agent,
            "lsAddress": self.ls_address,
            "codexQuota": self.get_codex_quota(use_cache=True),
            "subagents": subagents,
        }

    def inspect_subagent(self, conversation_id: str) -> Dict[str, Any]:
        """
        Detailed non-blocking inspection of EITHER a Codex subagent OR a Gemini native subagent.
        Returns transcript steps, live thinking, tool calls, unified file diffs, and plan checklist.
        """
        self._validate_conversation_id(conversation_id)
        roster = self.list_unified_subagents(include_all=True)
        summary = next(
            (s for s in roster["subagents"] if s["conversationId"] == conversation_id),
            None,
        )
        with self.lock:
            codex_sess = copy.deepcopy(self.sessions_by_conv.get(conversation_id))

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
            return self.dispatch_subagent_input(conversation_id, message, additional_context=additional_context)

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

    def interrupt_subagent(self, conversation_id: str, sdk_instance: Optional[str] = None) -> Dict[str, Any]:
        """Interrupt a Codex turn or a native Gemini invocation."""
        with self._conversation_lock(conversation_id):
            with self.lock:
                session = self.sessions_by_conv.get(conversation_id)
                if sdk_instance and (not session or sdk_instance != session.get("sdkInstanceId")):
                    raise ValueError("SDK executor registration is no longer current")
            if session:
                return self._execute_directive(session, "INTERRUPT", "")

        # Gemini native subagent: call CancelCascadeInvocation
        res = self.lsrpc(
            "CancelCascadeInvocation",
            {"cascadeId": conversation_id, "killBackgroundTasks": False, "notifyParent": False},
        )
        return {"engine": "gemini", "action": "CancelCascadeInvocation", "conversationId": conversation_id, "result": res}

    def kill_subagent(self, conversation_id: str) -> Dict[str, Any]:
        """Terminates a Codex or Gemini subagent permanently."""
        results: Dict[str, Any] = {"conversationId": conversation_id}
        with self._conversation_lock(conversation_id):
            with self.lock:
                session = self.sessions_by_conv.get(conversation_id)
                tid = session.get("threadId") if session else None
                credentials = self._session_credentials(session or {})
            if session:
                try:
                    self._execute_directive(session, "INTERRUPT", "")
                except Exception as e:
                    results["codexError"] = str(e)
                if tid:
                    try:
                        self.codex.thread_unsubscribe(tid)
                    except Exception as e:
                        results["unsubscribeError"] = str(e)
                with self.lock:
                    if tid:
                        self._clear_turn_resources(tid)
                        self._thread_connections.pop(tid, None)
                        self.conv_by_thread.pop(tid, None)
                    session.update(activeTurnId=None, status="killed", currentActivity="Killed", activityKind="")
                    self._save_state()
                    self.turn_changed.notify_all()
                results["codex"] = "interrupt_failed" if "codexError" in results else "terminated"

        if credentials["ls_address"]:
            try:
                results["lsForceStop"] = self.lsrpc("ForceStopCascadeTree", {"conversationId": conversation_id}, **credentials)
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
            return self.dispatch_subagent_input(conversation_id, f"[REVERT:{max(1, step_or_turns)}]")

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
            with self._conversation_lock(conversation_id):
                self._ensure_thread(codex_sess)
                result = self.codex.thread_fork(
                    thread_id=codex_sess["threadId"], before_turn_id=before_turn_id, cwd=codex_sess.get("cwd"),
                )
                new_thread = (result.get("thread") or {}).get("id")
                if not new_thread:
                    raise RuntimeError("Codex thread/fork did not return a thread ID")
                new_conv_id = f"codex-fork-{uuid.uuid4().hex[:12]}"
                with self.lock:
                    fields = ("agentName", "parentConversationId", "engine", "model", "effort", "sandbox",
                              "approvalPolicy", "approvalMode", "systemPrompt", "cwd")
                    history = codex_sess.get("turnHistory", [])
                    limit = next((i for i, entry in enumerate(history) if entry["turnId"] == before_turn_id), len(history))
                    self.sessions_by_conv[new_conv_id] = {
                        **{key: codex_sess[key] for key in fields if key in codex_sess},
                        "conversationId": new_conv_id, "threadId": new_thread, "activeTurnId": None,
                        "role": new_role or f"{codex_sess.get('role', 'Codex')} (Fork)",
                        "status": "idle", "executionMode": "preInvocation",
                        "stepCount": 0, "nextStepIndex": 0, "events": [],
                        "turnHistory": copy.deepcopy(history[:limit]),
                        "createdAt": utc_now_iso(), "updatedAt": utc_now_iso(),
                    }
                    self.conv_by_thread[new_thread] = new_conv_id
                    self._thread_connections[new_thread] = self.codex.connection
                    self._save_state()
            if prompt:
                self.dispatch_subagent_input(new_conv_id, prompt)
            return {"action": "thread/fork", "sourceConversationId": conversation_id,
                    "newConversationId": new_conv_id, "newThreadId": new_thread}

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

    def _resolve_quota_threshold(self, threshold_percent: Optional[int] = None) -> int:
        if threshold_percent is not None:
            return max(0, min(100, int(threshold_percent)))
        env_raw = os.environ.get("AGY_CODEX_MIN_QUOTA_PERCENT")
        if env_raw is not None and env_raw.strip() != "":
            try:
                return max(0, min(100, int(env_raw.strip())))
            except ValueError:
                pass
        try:
            cfg = load_config()
            return max(0, min(100, int(cfg.get("minRemainingQuotaPercent", 10))))
        except Exception:
            return 10

    @staticmethod
    def _format_window_label(mins: Any) -> str:
        if not isinstance(mins, (int, float)) or mins <= 0:
            return "window"
        mins_i = int(mins)
        if mins_i % 1440 == 0:
            return f"{mins_i // 1440}d"
        if mins_i % 60 == 0:
            return f"{mins_i // 60}h"
        return f"{mins_i}m"

    def get_codex_quota(
        self,
        threshold_percent: Optional[int] = None,
        use_cache: bool = False,
    ) -> Dict[str, Any]:
        """
        Queries `account/rateLimits/read` on the Codex App-Server Daemon and evaluates
        whether remaining quota is below `minRemainingQuotaPercent`.
        """
        threshold = self._resolve_quota_threshold(threshold_percent)
        now = time.monotonic()
        cached = getattr(self, "_quota_cache", None)
        if use_cache and cached and (now - cached[0] < 15.0) and cached[1].get("thresholdPercent") == threshold:
            return dict(cached[1])

        try:
            raw = self.codex.account_rate_limits_read()
        except Exception as e:
            logger.debug("account/rateLimits/read failed: %s", e)
            return {
                "available": False,
                "belowThreshold": False,
                "thresholdPercent": threshold,
                "error": str(e),
                "summary": f"Codex quota check unavailable: {e}",
            }

        if not isinstance(raw, dict) or not raw.get("rateLimits"):
            return {
                "available": False,
                "belowThreshold": False,
                "thresholdPercent": threshold,
                "summary": "Codex quota telemetry not reported by daemon.",
            }

        rl = raw.get("rateLimits") or {}
        credits = rl.get("credits") or {}
        unlimited = bool(credits.get("unlimited"))
        ordinary_allowed = raw.get("ordinaryUsageAllowed")
        spend_reached = bool(rl.get("spendControlReached"))
        reached_type = rl.get("rateLimitReachedType")
        plan_type = rl.get("planType") or ""
        reset_credits = (raw.get("rateLimitResetCredits") or {}).get("availableCount", 0)

        windows: List[Dict[str, Any]] = []
        remaining_values: List[float] = []
        for wname in ("primary", "secondary"):
            w = rl.get(wname)
            if not isinstance(w, dict):
                continue
            used = float(w.get("usedPercent", 0) or 0)
            rem = max(0.0, round(100.0 - used, 2))
            resets_at = w.get("resetsAt")
            resets_iso = ""
            if isinstance(resets_at, (int, float)) and resets_at > 0:
                resets_iso = datetime.datetime.fromtimestamp(
                    resets_at, tz=datetime.timezone.utc
                ).strftime("%Y-%m-%d %H:%M UTC")
            dur = w.get("windowDurationMins")
            windows.append({
                "name": wname,
                "usedPercent": used,
                "remainingPercent": rem,
                "windowDurationMins": dur,
                "windowLabel": self._format_window_label(dur),
                "resetsAt": resets_at,
                "resetsAtIso": resets_iso,
            })
            remaining_values.append(rem)

        min_remaining: Optional[float] = min(remaining_values) if remaining_values else None
        below_threshold = False
        reasons: List[str] = []
        if threshold > 0 and not unlimited:
            if ordinary_allowed is False:
                below_threshold = True
                reasons.append("ordinaryUsageAllowed=false")
            if spend_reached:
                below_threshold = True
                reasons.append("spendControlReached=true")
            if reached_type:
                below_threshold = True
                reasons.append(f"rateLimitReachedType={reached_type}")
            if min_remaining is not None and min_remaining < threshold:
                below_threshold = True
                reasons.append(f"remaining {min_remaining:g}% < threshold {threshold}%")

        win_parts = [
            f"{w['name']}({w['windowLabel']}): {w['remainingPercent']:g}% left ({w['usedPercent']:g}% used"
            + (f", resets {w['resetsAtIso']}" if w.get("resetsAtIso") else "")
            + ")"
            for w in windows
        ]
        summary_body = "; ".join(win_parts) if win_parts else "no active limit windows"
        if below_threshold:
            summary = f"Codex quota LOW ({', '.join(reasons)}) — {summary_body}"
        elif unlimited:
            summary = f"Codex quota unlimited ({plan_type or 'plan'})"
        else:
            summary = f"Codex quota OK (threshold {threshold}%) — {summary_body}"

        result = {
            "available": True,
            "planType": plan_type,
            "ordinaryUsageAllowed": ordinary_allowed,
            "spendControlReached": spend_reached,
            "rateLimitReachedType": reached_type,
            "unlimited": unlimited,
            "hasCredits": bool(credits.get("hasCredits")),
            "creditsBalance": str(credits.get("balance") or "0"),
            "resetCreditsAvailable": int(reset_credits or 0),
            "thresholdPercent": threshold,
            "minRemainingPercent": min_remaining,
            "belowThreshold": below_threshold,
            "reasons": reasons,
            "windows": windows,
            "summary": summary,
        }
        self._quota_cache = (now, result)
        return dict(result)

    def _handle_quota_blocked(
        self,
        session: Dict[str, Any],
        prompt_text: str,
        quota: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Cancels launching a new Codex thread when quota is below threshold and notifies the parent."""
        conv_id = session["conversationId"]
        role = session.get("role") or "Codex Subagent"
        parent = session.get("parentConversationId", "")
        credentials = self._session_credentials(session)

        msg_lines = [
            f"### ⚠️ Codex Launch Cancelled — Quota Below Threshold ({quota.get('thresholdPercent')}%)",
            f"- **Subagent Conversation**: [{role}](conversation://{conv_id})",
            f"- **Quota Status**: {quota.get('summary')}",
        ]
        if quota.get("resetCreditsAvailable"):
            msg_lines.append(
                f"- **Reset Credits Available**: {quota['resetCreditsAvailable']} free rate-limit reset credit(s) on account."
            )
        msg_lines.extend([
            "",
            "**Action Required**: Ask the user if they still want to continue launching Codex despite the low remaining quota.",
            f"- If the user confirms **Yes**, send `[FORCE]` to `{conv_id}` (`send_message(Recipient=\"{conv_id}\", Message=\"[FORCE]\")`) to replay the queued task immediately without creating a new conversation.",
            "- If the user confirms **No**, keep this subagent cancelled and handle the task with another agent or wait for reset.",
        ])
        report_text = "\n".join(msg_lines)

        with self.lock:
            session.update(
                status="quota_blocked",
                currentActivity=quota.get("summary", "Quota below threshold"),
                activityKind="warning",
                pendingQuotaPrompt=prompt_text,
                quotaCheck=quota,
                updatedAt=utc_now_iso(),
            )
            self._append_transcript_step(session, {
                "source": "MODEL",
                "type": "PLANNER_RESPONSE",
                "status": "DONE",
                "model": session.get("model") or "codex",
                "response": report_text,
                "tool_calls": [],
            })
            self._record_event(session, "quota/blocked", {
                "minRemainingPercent": quota.get("minRemainingPercent"),
                "thresholdPercent": quota.get("thresholdPercent"),
            })
            self._sync_sqlite_status(conv_id, "CASCADE_RUN_STATUS_IDLE", session["stepCount"], quota.get("summary", ""), 0)
            self._save_state()

        if parent:
            threading.Thread(
                target=self.send_message_to_agy_conversation,
                kwargs={
                    "recipient_id": parent,
                    "sender_id": conv_id,
                    "title": f"Codex Launch Cancelled: Low Quota ({quota.get('minRemainingPercent')}% left)",
                    "content": report_text,
                    **credentials,
                },
                name="codex-quota-blocked-notify",
                daemon=True,
            ).start()

        return {
            "action": "quota_blocked",
            "conversationId": conv_id,
            "status": "quota_blocked",
            "quota": quota,
            "message": report_text,
        }

    def list_codex_threads(
        self,
        limit: int = 25,
        search_term: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Lists existing standalone or active Codex threads from the running `codex app-server` daemon
        (`thread/loaded/list` + `thread/list`) so users or the parent agent can take over any existing session.
        """
        self.codex.connect()
        loaded_ids: set[str] = set()
        try:
            loaded_res = self.codex.thread_loaded_list()
            loaded_ids = set(loaded_res.get("data") or [])
        except Exception as e:
            logger.debug("thread_loaded_list failed: %s", e)

        list_res = self.codex.thread_list(limit=max(1, min(100, int(limit))), search_term=search_term, cwd=cwd)
        raw_threads = list(list_res.get("data") or [])
        seen_ids = {t.get("id") for t in raw_threads if isinstance(t, dict) and t.get("id")}

        # Ensure any currently loaded in-memory threads not in the top N list are included
        for lid in list(loaded_ids):
            if lid and lid not in seen_ids:
                try:
                    t_res = self.codex.thread_read(lid, include_turns=False)
                    t_obj = t_res.get("thread")
                    if isinstance(t_obj, dict) and t_obj.get("id"):
                        raw_threads.insert(0, t_obj)
                        seen_ids.add(lid)
                except Exception:
                    pass

        with self.lock:
            conv_map = dict(self.conv_by_thread)

        threads: List[Dict[str, Any]] = []
        for t in raw_threads:
            if not isinstance(t, dict) or not t.get("id"):
                continue
            tid = str(t["id"])
            adopted_cid = conv_map.get(tid)
            status_obj = t.get("status")
            status_type = status_obj.get("type", "idle") if isinstance(status_obj, dict) else str(status_obj or "idle")
            threads.append({
                "threadId": tid,
                "preview": str(t.get("preview") or "").strip()[:240],
                "cwd": str(t.get("cwd") or ""),
                "model": str(t.get("model") or ""),
                "reasoningEffort": str(t.get("reasoningEffort") or ""),
                "status": status_type,
                "isLoaded": tid in loaded_ids,
                "isAdopted": bool(adopted_cid),
                "adoptedConversationId": adopted_cid,
                "originator": str(t.get("originator") or ""),
                "source": str(t.get("source") or ""),
                "createdAt": t.get("createdAt"),
                "updatedAt": t.get("updatedAt"),
                "name": t.get("name"),
            })

        threads.sort(
            key=lambda item: (
                0 if item.get("isLoaded") else 1,
                -(int(item.get("updatedAt") or 0)),
            )
        )
        return {
            "codexConnected": self.codex.is_connected(),
            "loadedThreadIds": sorted(loaded_ids),
            "codexConfigToml": read_codex_config_toml(),
            "threads": threads[: max(1, min(100, int(limit)))],
        }

    def _hydrate_adopted_thread_turns(self, session: Dict[str, Any], thread_id: str) -> int:
        """
        Backfills existing turns from an adopted Codex thread via `thread/turns/list` into
        the subagent's `transcript.jsonl` and `turnHistory`.
        """
        with self.lock:
            existing_turns = {t.get("turnId") for t in (session.get("turnHistory") or []) if isinstance(t, dict)}

        try:
            turns_res = self.codex.thread_turns_list(
                thread_id=thread_id,
                limit=20,
                sort_direction="desc",
                items_view="full",
            )
            turns = list(reversed(turns_res.get("data") or []))
        except Exception as e:
            logger.debug("thread_turns_list on adopted thread %s returned: %s", thread_id, e)
            return 0

        hydrated = 0
        for turn in turns:
            if not isinstance(turn, dict):
                continue
            turn_id = str(turn.get("id") or "")
            if not turn_id or turn_id in existing_turns:
                continue
            turn_status = str(turn.get("status") or "completed")
            items = turn.get("items") or []
            turn_thinking_parts: List[str] = []
            turn_agent_messages: List[str] = []

            for item in items:
                if not isinstance(item, dict):
                    continue
                itype = item.get("type", "")
                iid = str(item.get("id") or uuid.uuid4())
                if itype == "userMessage":
                    content_list = item.get("content") or []
                    user_texts = [
                        str(c.get("text") or "")
                        for c in content_list
                        if isinstance(c, dict) and c.get("text")
                    ]
                    user_text = "\n".join(user_texts).strip()
                    if user_text:
                        self._append_transcript_step(session, {
                            "source": "USER_EXPLICIT",
                            "type": "USER_INPUT",
                            "status": "DONE",
                            "content": user_text,
                        })
                elif itype == "reasoning":
                    summaries = item.get("summary") or []
                    contents = item.get("content") or []
                    r_text = "\n".join(str(x) for x in (summaries or contents) if x).strip()
                    if r_text:
                        turn_thinking_parts.append(r_text)
                elif itype == "commandExecution":
                    cmd_str = str(item.get("command") or "")
                    cwd_str = str(item.get("cwd") or session.get("cwd") or "")
                    agg_out = str(item.get("aggregatedOutput") or "")
                    exit_code = item.get("exitCode")
                    self._append_transcript_step(session, {
                        "source": "MODEL",
                        "type": "PLANNER_RESPONSE",
                        "status": "DONE",
                        "model": session.get("model") or "codex",
                        "thinking": turn_thinking_parts[-1] if turn_thinking_parts else "",
                        "tool_calls": [{
                            "id": iid,
                            "name": "run_command",
                            "args": {
                                "CommandLine": json.dumps(cmd_str),
                                "Cwd": json.dumps(cwd_str),
                                "toolSummary": json.dumps(f"Run {cmd_str[:64]}"),
                            },
                        }],
                    })
                    if agg_out or exit_code is not None:
                        self._append_transcript_step(session, {
                            "source": "MODEL",
                            "type": "GENERIC",
                            "status": "DONE" if exit_code in (0, None) else "ERROR",
                            "content": f"Command: {cmd_str}\nExit Code: {exit_code}\nOutput:\n{agg_out[:3000]}",
                        })
                elif itype == "fileChange":
                    changes = item.get("changes") or []
                    first_path = changes[0].get("path", "file") if changes and isinstance(changes[0], dict) else "file"
                    self._append_transcript_step(session, {
                        "source": "MODEL",
                        "type": "PLANNER_RESPONSE",
                        "status": "DONE",
                        "model": session.get("model") or "codex",
                        "thinking": turn_thinking_parts[-1] if turn_thinking_parts else "",
                        "tool_calls": [{
                            "id": iid,
                            "name": "edit_file",
                            "args": {
                                "Path": json.dumps(first_path),
                                "toolSummary": json.dumps(f"Edit {Path(first_path).name}"),
                            },
                        }],
                    })
                elif itype == "agentMessage":
                    msg_txt = str(item.get("text") or "").strip()
                    if msg_txt:
                        turn_agent_messages.append(msg_txt)

            combined_thinking = "\n\n".join(turn_thinking_parts).strip()
            combined_msg = "\n\n".join(turn_agent_messages).strip()
            if combined_msg or combined_thinking:
                self._append_transcript_step(session, {
                    "source": "MODEL",
                    "type": "PLANNER_RESPONSE",
                    "status": "ERROR" if turn_status == "failed" else "DONE",
                    "model": session.get("model") or "codex",
                    "thinking": combined_thinking,
                    "response": combined_msg or f"Turn {turn_id} ({turn_status})",
                    "tool_calls": [],
                })
            with self.lock:
                if combined_thinking:
                    session["lastThinking"] = combined_thinking[-2000:]
                if combined_msg:
                    session["lastAgentMessage"] = combined_msg
                if turn_status in ("inProgress", "running", "working", "active"):
                    session["activeTurnId"] = turn_id
                    session["status"] = "working"
                    session["currentActivity"] = "Adopted active turn in progress..."
                else:
                    session.setdefault("turnHistory", []).append({
                        "turnId": turn_id,
                        "status": turn_status,
                        "completedAt": utc_now_iso(),
                        "summary": (combined_msg or combined_thinking)[:500],
                    })
            existing_turns.add(turn_id)
            hydrated += 1
        return hydrated

    def adopt_codex_thread(
        self,
        thread_id: str,
        conversation_id: Optional[str] = None,
        parent_conversation_id: Optional[str] = None,
        role: Optional[str] = None,
        prompt: Optional[str] = None,
        permission_source: Optional[str] = None,
        approval_policy: Optional[str] = None,
        sandbox_mode: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Directly takes over an existing Codex session (`threadId`) from `codex app-server`:
        1. Calls `thread/resume` (with fallback to `thread/read`) to subscribe our WebSocket connection
           to live events from that thread and inspect its metadata.
        2. Binds it to an Antigravity subagent `conversationId` and hydrates historical turns into
           `transcript.jsonl` so it appears in `agy`, TUI Split-View, and Web Sidecar UI.
        3. Optionally updates permissions (`codexConfig` from `~/.codex/config.toml`, `bypass`, or `custom`)
           via `thread/settings/update` and optionally dispatches an immediate steering/follow-up `prompt`.
        """
        clean_tid = str(thread_id or "").strip()
        if not clean_tid:
            raise ValueError("threadId is required to adopt an existing Codex session")

        self.codex.connect()
        connection = self.codex.connection

        with self.lock:
            existing_cid = self.conv_by_thread.get(clean_tid)
            target_cid = conversation_id or existing_cid or (
                f"codex-adopted-{re.sub(r'[^A-Za-z0-9_.-]', '', clean_tid)[:16] or uuid.uuid4().hex[:12]}"
            )
        self._validate_conversation_id(target_cid)

        with self._conversation_lock(target_cid):
            try:
                resumed = self.codex.thread_resume(clean_tid, cwd=cwd)
            except CodexRpcError as e:
                if "no rollout found" in e.message.lower():
                    resumed = self.codex.thread_read(clean_tid, include_turns=False)
                else:
                    raise

            thread_obj = resumed.get("thread") or {}
            live_cwd = cwd or resumed.get("cwd") or thread_obj.get("cwd") or os.getcwd()
            cfg = load_config()
            live_model = model or resumed.get("model") or thread_obj.get("model") or cfg["model"]
            live_effort = effort or resumed.get("reasoningEffort") or thread_obj.get("reasoningEffort") or cfg["effort"]
            preview = str(thread_obj.get("preview") or "").strip()
            status_obj = thread_obj.get("status")
            raw_status = status_obj.get("type", "idle") if isinstance(status_obj, dict) else "idle"

            perm_src = permission_source or cfg.get("permissionSource", "codexConfig")
            perm_src, eff_ap, eff_sb = resolve_effective_permissions(
                perm_src,
                approval_policy or cfg.get("approvalPolicy", "inherit"),
                sandbox_mode or cfg.get("sandboxMode", "inherit"),
            )

            with self.lock:
                if existing_cid and existing_cid != target_cid:
                    old_sess = self.sessions_by_conv.get(existing_cid)
                    if old_sess and old_sess.get("threadId") == clean_tid:
                        old_sess["threadId"] = None
                session = self.sessions_by_conv.get(target_cid)
                if session is None:
                    session = self._new_session(target_cid, "codex")
                    self.sessions_by_conv[target_cid] = session
                old_tid = session.get("threadId")
                if old_tid and old_tid != clean_tid:
                    self.conv_by_thread.pop(old_tid, None)
                    self._thread_connections.pop(old_tid, None)

                computed_role = (
                    role
                    or (session.get("role") if session.get("role") not in ("", None, "Codex Subagent") else None)
                    or (f"Adopted: {preview[:36]}" if preview else f"Adopted Codex ({clean_tid[:8]})")
                )
                session.update(
                    threadId=clean_tid,
                    isAdopted=True,
                    role=computed_role,
                    cwd=live_cwd,
                    model=live_model,
                    effort=live_effort,
                    permissionSource=perm_src,
                    approvalPolicy=eff_ap,
                    sandbox=eff_sb,
                    approvalMode="auto_accept" if eff_ap == "never" else "interactive",
                    status="working" if raw_status in ("running", "working", "inProgress", "active") else "idle",
                    currentActivity=f"Adopted thread {clean_tid[:12]}" + (f": {preview[:80]}" if preview else ""),
                    updatedAt=utc_now_iso(),
                )
                if parent_conversation_id:
                    session["parentConversationId"] = parent_conversation_id
                self.conv_by_thread[clean_tid] = target_cid
                self._thread_connections[clean_tid] = connection

            # Apply permission/model/effort settings onto the adopted thread if supported
            if permission_source or approval_policy or sandbox_mode or model or effort:
                try:
                    self.codex.thread_settings_update(
                        thread_id=clean_tid,
                        model=model,
                        effort=effort,
                        approval_policy=eff_ap,
                        sandbox_mode=eff_sb,
                        cwd=live_cwd,
                    )
                except CodexRpcError as e:
                    logger.debug("thread_settings_update during adopt skipped: %s", e)

            hydrated_turns = self._hydrate_adopted_thread_turns(session, clean_tid)
            self._record_event(session, "thread/adopted", {
                "threadId": clean_tid,
                "hydratedTurns": hydrated_turns,
                "permissionSource": perm_src,
                "approvalPolicy": eff_ap,
                "sandboxMode": eff_sb,
            })
            self._sync_sqlite_status(
                target_cid,
                "CASCADE_RUN_STATUS_RUNNING" if session["status"] == "working" else "CASCADE_RUN_STATUS_IDLE",
                session.get("stepCount", 0),
                session.get("lastAgentMessage") or preview or f"Adopted Codex thread {clean_tid[:12]}",
                1 if session["status"] == "working" else 0,
            )
            self._save_state()

            dispatch_res = None
            if prompt and prompt.strip():
                dispatch_res = self._dispatch_turn(session, prompt.strip())

            return {
                "action": "thread/adopt",
                "conversationId": target_cid,
                "threadId": clean_tid,
                "role": session["role"],
                "cwd": session["cwd"],
                "model": session["model"],
                "effort": session["effort"],
                "permissionSource": session.get("permissionSource", perm_src),
                "approvalPolicy": session["approvalPolicy"],
                "sandboxMode": session["sandbox"],
                "hydratedTurns": hydrated_turns,
                "status": session["status"],
                **({"dispatch": dispatch_res} if dispatch_res else {}),
            }

    def configure_subagent(
        self,
        conversation_id: str,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        approval_mode: Optional[str] = None,
        permission_source: Optional[str] = None,
        approval_policy: Optional[str] = None,
        sandbox_mode: Optional[str] = None,
        compact: bool = False,
    ) -> Dict[str, Any]:
        with self._conversation_lock(conversation_id):
            with self.lock:
                session = self.sessions_by_conv.get(conversation_id)
                if not session:
                    raise ValueError(f"Conversation {conversation_id} is not a Codex subagent session")
                if effort is not None and effort not in ("low", "medium", "high", "xhigh", "ultra"):
                    raise ValueError("Invalid reasoning effort")
                if model is not None and (not isinstance(model, str) or not model.strip()):
                    raise ValueError("Invalid model")
                if permission_source is not None and permission_source not in ("codexConfig", "bypass", "custom"):
                    raise ValueError("Invalid permissionSource; expected codexConfig, bypass, or custom")
                if approval_policy is not None and approval_policy not in ("never", "on-request", "untrusted", "inherit"):
                    raise ValueError("Invalid approvalPolicy")
                if sandbox_mode is not None and sandbox_mode not in (
                    "read-only", "workspace-write", "danger-full-access", "inherit"
                ):
                    raise ValueError("Invalid sandboxMode")

                tid, turn_id = session.get("threadId"), session.get("activeTurnId")
                if model is not None:
                    session["model"] = model
                if effort is not None:
                    session["effort"] = effort

                perm_changed = any(
                    x is not None for x in (permission_source, approval_policy, sandbox_mode, approval_mode)
                )
                eff_ap = session.get("approvalPolicy", "never")
                eff_sb = session.get("sandbox", "danger-full-access")
                if perm_changed:
                    if approval_mode in ("auto_accept", "interactive") and not permission_source and not approval_policy:
                        approval_policy = "never" if approval_mode == "auto_accept" else "on-request"
                        permission_source = "custom"
                    eff_src = permission_source or (
                        "custom" if (approval_policy or sandbox_mode) else session.get("permissionSource", "codexConfig")
                    )
                    eff_src, eff_ap, eff_sb = resolve_effective_permissions(
                        eff_src,
                        approval_policy or session.get("approvalPolicy", "never"),
                        sandbox_mode or session.get("sandbox", "danger-full-access"),
                    )
                    session["permissionSource"] = eff_src
                    session["approvalPolicy"] = eff_ap
                    session["sandbox"] = eff_sb
                    session["approvalMode"] = (
                        approval_mode
                        if approval_mode in ("auto_accept", "interactive")
                        else ("auto_accept" if eff_ap == "never" else "interactive")
                    )
                self._save_state()

            result: Dict[str, Any] = {
                "conversationId": conversation_id,
                "threadId": tid,
                "model": session.get("model"),
                "effort": session.get("effort"),
                "permissionSource": session.get("permissionSource", "codexConfig"),
                "approvalPolicy": session.get("approvalPolicy"),
                "sandboxMode": session.get("sandbox"),
                "approvalMode": session.get("approvalMode"),
            }
            if tid and (perm_changed or model is not None or effort is not None):
                try:
                    self._ensure_thread(session)
                    result["threadSettingsUpdate"] = self.codex.thread_settings_update(
                        thread_id=tid,
                        model=model,
                        effort=effort,
                        approval_policy=eff_ap if perm_changed else None,
                        sandbox_mode=eff_sb if perm_changed else None,
                        cwd=session.get("cwd"),
                    )
                    self._record_event(session, "thread/settings/update", {
                        "permissionSource": session.get("permissionSource"),
                        "approvalPolicy": eff_ap,
                        "sandboxMode": eff_sb,
                        "model": session.get("model"),
                        "effort": session.get("effort"),
                    })
                except CodexRpcError as e:
                    logger.debug("thread_settings_update returned: %s", e)
            if tid and turn_id and (model is not None or effort is not None):
                try:
                    result["settingsUpdate"] = self.codex.turn_settings_update(tid, turn_id, model, effort)
                except CodexRpcError:
                    with self.lock:
                        if session.get("activeTurnId") == turn_id:
                            raise
            if tid and compact:
                self._ensure_thread(session)
                result["compact"] = self.codex.thread_compact_start(tid)
            return result


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
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if not 0 < length <= 4 * 1024 * 1024:
            raise ValueError("A JSON body of at most 4 MiB is required")
        raw = self.rfile.read(length).decode("utf-8")
        body = json.loads(raw)
        if not isinstance(body, dict):
            raise ValueError("The JSON body must be an object")
        return body

    def local_host(self) -> bool:
        return self.headers.get("Host", "").split(":", 1)[0] in ("127.0.0.1", "localhost")

    def do_GET(self) -> None:
        assert BRIDGE_INSTANCE is not None
        if not self.local_host():
            self._send_json(403, {"error": "The bridge accepts local requests"})
            return
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)

        try:
            if parsed.path == "/api/ready":
                self._send_json(200, {"status": "ok"})
                return

            if parsed.path == "/api/profiles":
                self._send_json(200, BRIDGE_INSTANCE.list_profiles())
                return

            if parsed.path == "/api/subagent/sdk-status":
                self._send_json(200, BRIDGE_INSTANCE.sdk_status(
                    (qs.get("conversationId") or [""])[0], (qs.get("sdkInstanceId") or [""])[0],
                ))
                return

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

            if parsed.path in ("/api/subagents", "/api/mission-control"):
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

            if parsed.path == "/api/codex/threads":
                limit = int((qs.get("limit") or ["25"])[0] or "25")
                search = (qs.get("search") or [None])[0]
                cwd = (qs.get("cwd") or [None])[0]
                self._send_json(200, BRIDGE_INSTANCE.list_codex_threads(limit=limit, search_term=search, cwd=cwd))
                return

            if parsed.path == "/api/codex/quota":
                raw_thr = (qs.get("threshold") or [None])[0]
                thr = int(raw_thr) if raw_thr is not None and raw_thr != "" else None
                self._send_json(200, BRIDGE_INSTANCE.get_codex_quota(threshold_percent=thr, use_cache=False))
                return

            self._send_json(404, {"error": f"Unknown GET endpoint: {parsed.path}"})
        except ValueError as e:
            self._send_json(400, {"error": str(e)})
        except Exception as e:
            logger.exception("Error in GET %s", self.path)
            self._send_json(500, {"error": str(e)})

    def do_POST(self) -> None:
        assert BRIDGE_INSTANCE is not None
        parsed = urllib.parse.urlparse(self.path)
        if not self.local_host() or self.headers.get("Origin") or self.headers.get_content_type() != "application/json":
            self._send_json(403, {"error": "Use a local client or the Mission Control sidecar"})
            return
        try:
            body = self._read_json_body()

            if body.get("lsAddress") or body.get("agentapiExe"):
                BRIDGE_INSTANCE.update_ls_credentials(
                    ls_address=body.get("lsAddress"), csrf_token=body.get("csrfToken"),
                    agentapi_exe=body.get("agentapiExe"),
                )

            if parsed.path == "/api/ls-credentials":
                if body.get("sdkInstanceId"):
                    registration = BRIDGE_INSTANCE.register_sdk(body)
                    self._send_json(200, {"ok": True, **registration})
                else:
                    self._send_json(200, {"ok": True, "lsAddress": BRIDGE_INSTANCE.ls_address})
                return

            if parsed.path == "/api/subagent/sdk-unregister":
                BRIDGE_INSTANCE.unregister_sdk(body["conversationId"], body["sdkInstanceId"])
                self._send_json(200, {"ok": True})
                return

            if parsed.path == "/api/profiles/define":
                self._send_json(200, BRIDGE_INSTANCE.define_profile(body))
                return

            if parsed.path == "/api/profiles/delete":
                self._send_json(200, BRIDGE_INSTANCE.delete_profile(body["name"]))
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
                    sandbox=body.get("sandboxMode") or body.get("sandbox"),
                    approval_policy=body.get("approvalPolicy"),
                    ls_address=body.get("lsAddress"),
                    csrf_token=body.get("csrfToken"),
                    agent_name=body.get("agentName"),
                    execution_mode=body.get("executionMode"),
                    sdk_instance=body.get("sdkInstanceId"),
                    input_id=body.get("inputId"),
                    bypass_quota_check=bool(body.get("bypassQuotaCheck", False)),
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
                res = BRIDGE_INSTANCE.interrupt_subagent(body["conversationId"], body.get("sdkInstanceId"))
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

            if parsed.path == "/api/codex/adopt":
                res = BRIDGE_INSTANCE.adopt_codex_thread(
                    thread_id=body.get("threadId", ""),
                    conversation_id=body.get("conversationId"),
                    parent_conversation_id=body.get("parentConversationId"),
                    role=body.get("role"),
                    prompt=body.get("prompt"),
                    permission_source=body.get("permissionSource"),
                    approval_policy=body.get("approvalPolicy"),
                    sandbox_mode=body.get("sandboxMode"),
                    model=body.get("model"),
                    effort=body.get("effort"),
                    cwd=body.get("cwd"),
                )
                self._send_json(200, res)
                return

            if parsed.path == "/api/subagent/configure":
                res = BRIDGE_INSTANCE.configure_subagent(
                    conversation_id=body["conversationId"],
                    model=body.get("model"),
                    effort=body.get("effort"),
                    approval_mode=body.get("approvalMode"),
                    permission_source=body.get("permissionSource"),
                    approval_policy=body.get("approvalPolicy"),
                    sandbox_mode=body.get("sandboxMode"),
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
        except (ValueError, KeyError) as e:
            self._send_json(400, {"error": str(e)})
        except Exception as e:
            logger.exception("Error in POST %s", self.path)
            self._send_json(500, {"error": str(e)})


def ensure_bridge_daemon_running(port: int = BRIDGE_PORT) -> str:
    """Check HTTP readiness without waiting for the independent Codex connection."""
    base_url = f"http://{BRIDGE_HOST}:{port}"

    def ready() -> bool:
        try:
            with urllib.request.urlopen(f"{base_url}/api/ready", timeout=0.5) as resp:
                return resp.status == 200
        except urllib.error.HTTPError as e:
            e.close()
            if e.code == 404:
                try:
                    with urllib.request.urlopen(f"{base_url}/api/health", timeout=0.5) as hresp:
                        if hresp.status == 200:
                            return True
                except OSError:
                    pass
            return False
        except OSError:
            return False

    if ready():
        return base_url

    log_path = primary_app_data_dir() / "log" / "codex_bridge_daemon.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--port", str(port)],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
        )
    threading.Thread(target=process.wait, name="codex-bridge-reaper", daemon=True).start()

    deadline = time.monotonic() + 6.0
    while time.monotonic() < deadline:
        if ready():
            return base_url
        time.sleep(0.15)
    raise RuntimeError(f"Failed to start codex_bridge_daemon.py on {base_url}")


def main() -> None:
    global BRIDGE_INSTANCE
    parser = argparse.ArgumentParser(description="Antigravity Codex & Unified Subagent Bridge Daemon")
    parser.add_argument("--port", type=int, default=BRIDGE_PORT)
    parser.add_argument("--host", choices=("127.0.0.1",), default=BRIDGE_HOST)
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with ThreadingHTTPServer((args.host, args.port), BridgeHttpHandler) as server:
        BRIDGE_INSTANCE = UnifiedSubagentBridge()
        logger.info("Unified Subagent Bridge Daemon listening on http://%s:%d", args.host, args.port)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            BRIDGE_INSTANCE.codex.close()


if __name__ == "__main__":
    main()
