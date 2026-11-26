#!/usr/bin/env python3
"""
Unified Subagent & Codex Control Plane MCP Server (`codex-subagent-control`).

Exposes MCP tools to the parent Antigravity agent for non-blocking inspection,
mid-turn steering (`turn/steer` / `SendAgentMessage`), interruption, step/turn
reversion, thread forking, dynamic model/effort reconfiguration, and cross-engine
handoff across BOTH OpenAI Codex subagents AND native Gemini subagents (`self`, `research`, etc.).
"""

from __future__ import annotations

import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from codex_profiles import APPROVAL_POLICIES, BACKENDS, EFFORTS, EXECUTION_MODES, PERMISSION_SOURCES, SANDBOX_MODES
from codex_bridge_daemon import BRIDGE_PORT, ensure_bridge_daemon_running


def bridge_get(path: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    base_url = ensure_bridge_daemon_running(BRIDGE_PORT)
    url = f"{base_url}{path}"
    if params:
        qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        if qs:
            url = f"{url}?{qs}"
    with urllib.request.urlopen(url, timeout=20.0) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def bridge_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    base_url = ensure_bridge_daemon_running(BRIDGE_PORT)
    enriched = {
        **payload,
        "lsAddress": os.environ.get("ANTIGRAVITY_LS_ADDRESS", ""),
        "csrfToken": os.environ.get("ANTIGRAVITY_CSRF_TOKEN", ""),
        "agentapiExe": os.environ.get("ANTIGRAVITY_AGENTAPI_EXE", ""),
    }
    data = json.dumps(enriched).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30.0) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        with e:
            err_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Bridge HTTP {e.code}: {err_body}") from e


MCP_TOOLS: List[Dict[str, Any]] = [
    {
        "name": "subagent_define_profile",
        "description": (
            "Create or update a subagent profile (backend='codex' or backend='agy') and register "
            "its native Antigravity subagent type. With backend='agy', you can pin any exact Antigravity "
            "model ID (e.g. 'claude-opus-5-5-high', 'claude-sonnet-5-5-high', 'gemini-4-neon-xhigh', "
            "'gemini-4-argon-high', 'gpt-oss-120b-medium') via `agy acp`!"
        ),
        "inputSchema": {
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {"type": "string", "pattern": "^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$"},
                "backend": {
                    "type": "string",
                    "enum": BACKENDS,
                    "description": "'codex' (default, OpenAI Codex app-server) or 'agy' (Antigravity ACP with exact model selection).",
                },
                "description": {"type": "string"},
                "model": {"type": "string"},
                "effort": {"type": "string", "enum": EFFORTS},
                "systemPrompt": {"type": "string"},
                "executionMode": {"type": "string", "enum": EXECUTION_MODES},
                "permissionSource": {
                    "type": "string",
                    "enum": PERMISSION_SOURCES,
                    "description": (
                        "'codexConfig' (default): inherit approval_policy & sandbox_mode from ~/.codex/config.toml. "
                        "'bypass': full autonomous bypass (approvalPolicy='never', sandboxMode='danger-full-access'). "
                        "'custom': use explicit sandboxMode and approvalPolicy."
                    ),
                },
                "sandboxMode": {"type": "string", "enum": SANDBOX_MODES},
                "approvalPolicy": {"type": "string", "enum": APPROVAL_POLICIES},
            },
        },
    },
    {
        "name": "subagent_list_profiles",
        "description": "List saved Codex & AGY profiles and defaults for new profiles (including ~/.codex/config.toml permissions).",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "subagent_list_agy_models",
        "description": (
            "List all exact selectable Antigravity model IDs supported by the `agy` ACP backend "
            "(e.g. `claude-opus-5-5-high`, `claude-sonnet-5-5-high`, `gemini-4-neon-xhigh`, "
            "`gemini-4-argon-high`, `gemini-3.8-flash-high`, `gpt-oss-120b-medium`)."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "subagent_check_codex_quota",
        "description": (
            "Check remaining Codex account quota and rate-limit windows (`account/rateLimits/read`) "
            "before launching a new Codex subagent. Returns `minRemainingPercent`, `thresholdPercent`, "
            "`belowThreshold`, `windows`, and `summary`. If `belowThreshold` is true, DO NOT launch "
            "Codex automatically — cancel and ask the user whether they want to continue anyway."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "thresholdPercent": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 100,
                    "description": "Optional override for minimum remaining quota percentage (defaults to config minRemainingQuotaPercent, usually 10).",
                },
            },
        },
    },
    {
        "name": "subagent_list_codex_sessions",
        "description": (
            "List existing Codex sessions/threads from the running `codex app-server` daemon "
            "(both currently loaded in-memory threads and recent persisted threads on disk) "
            "that can be directly taken over / adopted via `subagent_adopt_codex_session`."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of Codex threads to list (default 25).",
                },
                "search": {
                    "type": "string",
                    "description": "Optional search keyword to filter threads.",
                },
                "cwd": {
                    "type": "string",
                    "description": "Optional working directory filter.",
                },
            },
        },
    },
    {
        "name": "subagent_adopt_codex_session",
        "description": (
            "Directly take over / adopt an existing Codex session (`threadId`) into Antigravity's "
            "Unified Subagent Control Plane. Subscribes to live notifications via `thread/resume`, "
            "imports historical turns into `transcript.jsonl`, optionally applies permission overrides "
            "(`permissionSource`: 'codexConfig' | 'bypass' | 'custom'), and optionally sends an "
            "immediate steering or follow-up `prompt`."
        ),
        "inputSchema": {
            "type": "object",
            "required": ["threadId"],
            "properties": {
                "threadId": {
                    "type": "string",
                    "description": "Existing Codex threadId to take over (from `subagent_list_codex_sessions`).",
                },
                "conversationId": {
                    "type": "string",
                    "description": "Optional custom subagent conversationId to bind this thread to.",
                },
                "role": {
                    "type": "string",
                    "description": "Optional role title for the adopted Codex session.",
                },
                "prompt": {
                    "type": "string",
                    "description": "Optional immediate steering (`turn/steer`) or follow-up (`turn/start`) instruction.",
                },
                "permissionSource": {
                    "type": "string",
                    "enum": PERMISSION_SOURCES,
                    "description": "'codexConfig' (from ~/.codex/config.toml), 'bypass' (never + danger-full-access), or 'custom'.",
                },
                "approvalPolicy": {
                    "type": "string",
                    "enum": APPROVAL_POLICIES,
                    "description": "Approval policy override when permissionSource='custom'.",
                },
                "sandboxMode": {
                    "type": "string",
                    "enum": SANDBOX_MODES,
                    "description": "Sandbox policy override when permissionSource='custom'.",
                },
            },
        },
    },
    {
        "name": "subagent_list",
        "description": (
            "List all active and recent subagents (both OpenAI Codex subagents and native Gemini "
            "subagents like `self` and `research`), showing their conversationId, engine, role, "
            "model, effort, status (working/idle/awaiting_approval/error), step count, live activity, "
            "and Codex threadId/activeTurnId."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "rootConversationId": {
                    "type": "string",
                    "description": "Optional parent conversation ID to prioritize.",
                },
                "includeAll": {
                    "type": "boolean",
                    "description": "Whether to include subagents from all recent conversations (default true).",
                },
            },
        },
    },
    {
        "name": "subagent_inspect",
        "description": (
            "Non-blocking live inspection of any running or idle subagent (Codex or Gemini). "
            "Returns its live thinking stream, recent tool executions and outputs, Codex execution plan "
            "checklist (`turn/plan/updated`), live unified file diff (`turn/diff/updated` or trajectory diffs), "
            "and pending approval requests. Use this before deciding whether to `subagent_steer`."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversationId": {
                    "type": "string",
                    "description": "The conversationId of the subagent (returned by invoke_subagent or subagent_list).",
                },
            },
            "required": ["conversationId"],
        },
    },
    {
        "name": "subagent_steer",
        "description": (
            "Steer a running (or idle) Codex or Gemini subagent without killing its session. "
            "For an actively running Codex subagent, `mode='steer'` executes `turn/steer` on the "
            "Codex App-Server Daemon (`expectedTurnId`), injecting new instructions or constraints "
            "mid-turn with zero interruption. For a Gemini subagent, it delivers a high-priority "
            "steering message via `SendAgentMessage` (and optionally interrupts the current tool step first)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversationId": {
                    "type": "string",
                    "description": "Target subagent conversationId.",
                },
                "message": {
                    "type": "string",
                    "description": "Steering instruction, correction, or follow-up prompt.",
                },
                "mode": {
                    "type": "string",
                    "enum": ["steer", "queue", "interrupt_and_steer"],
                    "description": (
                        "'steer' (default): mid-turn steer (`turn/steer` for Codex, immediate message for Gemini). "
                        "'queue': queue for next turn (`thread/queue/add`). "
                        "'interrupt_and_steer': abort current turn/step first, then start with the new instruction."
                    ),
                },
                "additionalContext": {
                    "type": "string",
                    "description": "Optional extra context block injected alongside the steering instruction.",
                },
            },
            "required": ["conversationId", "message"],
        },
    },
    {
        "name": "subagent_interrupt",
        "description": (
            "Cleanly interrupt/pause the currently running turn of a Codex subagent (`turn/interrupt`) "
            "or Gemini subagent (`CancelCascadeInvocation`) while keeping its thread/conversation alive "
            "for subsequent steering."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversationId": {
                    "type": "string",
                    "description": "Target subagent conversationId.",
                },
            },
            "required": ["conversationId"],
        },
    },
    {
        "name": "subagent_revert",
        "description": (
            "Revert a subagent to an earlier point. For Codex subagents, executes `thread/revert` "
            "(dropping the last N turns). For Gemini subagents, executes `RevertToCascadeStep` "
            "to rewind the trajectory to `stepOrTurns`."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversationId": {
                    "type": "string",
                    "description": "Target subagent conversationId.",
                },
                "stepOrTurns": {
                    "type": "integer",
                    "description": "For Codex: number of turns to drop (default 1). For Gemini: target stepIndex to revert to.",
                },
            },
            "required": ["conversationId"],
        },
    },
    {
        "name": "subagent_fork",
        "description": (
            "Fork an existing Codex subagent thread via `thread/fork` (optionally at `beforeTurnId`) "
            "or fork a Gemini subagent's context into a new Codex subagent thread to explore an "
            "alternative implementation path in parallel."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversationId": {
                    "type": "string",
                    "description": "Source subagent conversationId to fork.",
                },
                "beforeTurnId": {
                    "type": "string",
                    "description": "Optional Codex turnId before which to fork the thread history.",
                },
                "newRole": {
                    "type": "string",
                    "description": "Role title for the forked subagent.",
                },
                "prompt": {
                    "type": "string",
                    "description": "Optional initial prompt to start immediately on the forked subagent.",
                },
            },
            "required": ["conversationId"],
        },
    },
    {
        "name": "subagent_configure",
        "description": (
            "Dynamically update a Codex subagent's model, reasoning effort (`low`, `medium`, `high`, "
            "`xhigh`, `ultra`), or permissions (`permissionSource`: 'codexConfig' from ~/.codex/config.toml, "
            "'bypass' for never + danger-full-access, or 'custom' with explicit `approvalPolicy` and `sandboxMode`) "
            "live mid-session via `thread/settings/update` and `turn/settings/update`, or trigger context "
            "compaction via `thread/compact/start`."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversationId": {
                    "type": "string",
                    "description": "Target Codex subagent conversationId.",
                },
                "model": {
                    "type": "string",
                    "description": "Optional model override (e.g., 'gpt-6.2-sol', 'gpt-5.4', 'o3').",
                },
                "effort": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "xhigh", "ultra"],
                    "description": "Optional reasoning effort override.",
                },
                "permissionSource": {
                    "type": "string",
                    "enum": PERMISSION_SOURCES,
                    "description": (
                        "'codexConfig': inherit approval_policy & sandbox_mode from ~/.codex/config.toml. "
                        "'bypass': bypass all permission prompts and sandbox restrictions (never + danger-full-access). "
                        "'custom': use explicit approvalPolicy and sandboxMode."
                    ),
                },
                "approvalPolicy": {
                    "type": "string",
                    "enum": APPROVAL_POLICIES,
                    "description": "Optional approval policy ('never', 'on-request', 'untrusted', 'inherit').",
                },
                "sandboxMode": {
                    "type": "string",
                    "enum": SANDBOX_MODES,
                    "description": "Optional sandbox mode ('danger-full-access', 'workspace-write', 'read-only', 'inherit').",
                },
                "approvalMode": {
                    "type": "string",
                    "enum": ["auto_accept", "interactive"],
                    "description": "Whether to auto-accept command/file approvals or hold them for UI/parent review.",
                },
                "compact": {
                    "type": "boolean",
                    "description": "Set true to trigger `thread/compact/start` on the Codex thread.",
                },
            },
            "required": ["conversationId"],
        },
    },
    {
        "name": "subagent_handoff",
        "description": (
            "Cross-engine handoff between Gemini (`self`, `research`) and OpenAI Codex (`codex`). "
            "Extracts the latest thinking, findings, and workspace directory from `sourceConversationId` "
            "and launches or messages a target subagent on `targetEngine` ('codex' or 'gemini')."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "sourceConversationId": {
                    "type": "string",
                    "description": "Source subagent conversationId whose findings/context should be handed off.",
                },
                "targetEngine": {
                    "type": "string",
                    "enum": ["codex", "agy", "gemini"],
                    "description": "Target engine to hand off to ('codex', 'agy', or 'gemini').",
                },
                "prompt": {
                    "type": "string",
                    "description": "Task instructions for the receiving subagent.",
                },
                "newRole": {
                    "type": "string",
                    "description": "Role label for the new Codex subagent if targetEngine='codex'.",
                },
            },
            "required": ["sourceConversationId", "prompt"],
        },
    },
]


def handle_tool_call(name: str, args: Dict[str, Any]) -> Any:
    if name == "subagent_define_profile":
        return bridge_post("/api/profiles/define", args)
    if name == "subagent_list_profiles":
        return bridge_get("/api/profiles")
    if name == "subagent_list_agy_models":
        return bridge_get("/api/agy/models")
    if name == "subagent_check_codex_quota":
        params = {}
        if args.get("thresholdPercent") is not None:
            params["threshold"] = str(args["thresholdPercent"])
        return bridge_get("/api/codex/quota", params)
    if name == "subagent_list_codex_sessions":
        return bridge_get(
            "/api/codex/threads",
            {
                "limit": str(args.get("limit", 25)),
                "search": args.get("search"),
                "cwd": args.get("cwd"),
            },
        )
    if name == "subagent_adopt_codex_session":
        return bridge_post(
            "/api/codex/adopt",
            {
                "threadId": args["threadId"],
                "conversationId": args.get("conversationId"),
                "parentConversationId": os.environ.get("ANTIGRAVITY_CONVERSATION_ID", ""),
                "role": args.get("role"),
                "prompt": args.get("prompt"),
                "permissionSource": args.get("permissionSource"),
                "approvalPolicy": args.get("approvalPolicy"),
                "sandboxMode": args.get("sandboxMode"),
            },
        )
    if name == "subagent_list":
        return bridge_get(
            "/api/subagents",
            {
                "rootConversationId": args.get("rootConversationId") or os.environ.get("ANTIGRAVITY_CONVERSATION_ID", ""),
                "all": "false" if args.get("includeAll") is False else "true",
            },
        )

    if name == "subagent_inspect":
        return bridge_get("/api/subagent/inspect", {"conversationId": args["conversationId"]})

    if name == "subagent_steer":
        return bridge_post(
            "/api/subagent/steer",
            {
                "conversationId": args["conversationId"],
                "message": args["message"],
                "mode": args.get("mode", "steer"),
                "additionalContext": args.get("additionalContext"),
            },
        )

    if name == "subagent_interrupt":
        return bridge_post("/api/subagent/interrupt", {"conversationId": args["conversationId"]})

    if name == "subagent_revert":
        return bridge_post(
            "/api/subagent/revert",
            {
                "conversationId": args["conversationId"],
                "stepOrTurns": int(args.get("stepOrTurns", 1)),
            },
        )

    if name == "subagent_fork":
        return bridge_post(
            "/api/subagent/fork",
            {
                "conversationId": args["conversationId"],
                "beforeTurnId": args.get("beforeTurnId"),
                "newRole": args.get("newRole"),
                "prompt": args.get("prompt"),
            },
        )

    if name == "subagent_configure":
        return bridge_post(
            "/api/subagent/configure",
            {
                "conversationId": args["conversationId"],
                "model": args.get("model"),
                "effort": args.get("effort"),
                "permissionSource": args.get("permissionSource"),
                "approvalPolicy": args.get("approvalPolicy"),
                "sandboxMode": args.get("sandboxMode"),
                "approvalMode": args.get("approvalMode"),
                "compact": bool(args.get("compact", False)),
            },
        )

    if name == "subagent_handoff":
        return bridge_post(
            "/api/subagent/handoff",
            {
                "sourceConversationId": args["sourceConversationId"],
                "targetEngine": args.get("targetEngine", "codex"),
                "prompt": args["prompt"],
                "newRole": args.get("newRole"),
            },
        )

    raise ValueError(f"Unknown tool: {name}")


stdout_lock = threading.Lock()


def send_mcp_response(msg: Dict[str, Any]) -> None:
    with stdout_lock:
        sys.stdout.write(json.dumps(msg, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def run_tool(req_id: str | int, params: Dict[str, Any]) -> None:
    try:
        result = handle_tool_call(params.get("name", ""), params.get("arguments") or {})
        message = json.dumps(result, indent=2, ensure_ascii=False)
        is_error = False
    except Exception as e:
        message, is_error = f"Error: {e}", True
    send_mcp_response({
        "jsonrpc": "2.0", "id": req_id,
        "result": {"content": [{"type": "text", "text": message}], "isError": is_error},
    })


def main() -> None:
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="codex-mcp-tool") as workers:
        for line in sys.stdin:
            try:
                req = json.loads(line)
            except ValueError:
                continue
            if not isinstance(req, dict) or req.get("id") is None:
                continue

            req_id, method, params = req["id"], req.get("method"), req.get("params") or {}
            if not isinstance(params, dict):
                send_mcp_response({"jsonrpc": "2.0", "id": req_id,
                                   "error": {"code": -32602, "message": "Invalid params"}})
                continue
            response: Dict[str, Any] = {"jsonrpc": "2.0", "id": req_id}
            if method == "initialize":
                response["result"] = {
                    "protocolVersion": params.get("protocolVersion") or "2024-11-05",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "codex-subagent-control", "version": "1.0.0"},
                }
            elif method == "ping":
                response["result"] = {}
            elif method == "tools/list":
                response["result"] = {"tools": MCP_TOOLS}
            elif method == "tools/call":
                workers.submit(run_tool, req_id, params)
                continue
            else:
                response["error"] = {"code": -32601, "message": f"Method not found: {method}"}
            send_mcp_response(response)


if __name__ == "__main__":
    main()
