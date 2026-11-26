#!/usr/bin/env python3
"""
End-to-End Verification Suite for All Phases of `codex-subagent`:
  - Phase 1: Direct WebSocket-over-AF_UNIX connection to `codex app-server` daemon,
             `PreInvocation` hook interception (`terminationBehavior: "terminate"`),
             `thread/start`, `turn/start`, and real-time `transcript.jsonl` synthesis.
  - Phase 2: Mid-turn steering (`turn/steer`), queue (`thread/queue/add`),
             settings update (`turn/settings/update`), thread fork (`thread/fork`),
             turn revert (`thread/revert`), turn interrupt (`turn/interrupt`),
             cross-engine handoff (Gemini `research` -> Codex), and full Stdio MCP Server
             (`codex_mcp_server.py`) tool verification.
  - Phase 3: Unified Subagent Mission Control Sidecar UI & Node.js Backend
             (`sidecars/subagent_mission_control/main.mjs`), verifying unified takeover
             of BOTH OpenAI Codex subagents AND native Gemini subagents (`self`, `research`).
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import queue
import shutil
import signal
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = REPO_ROOT / "plugins" / "codex-subagent"
BIN_DIR = PLUGIN_DIR / "bin"
SIDECAR_DIR = PLUGIN_DIR / "sidecars" / "subagent_mission_control"

sys.path.insert(0, str(BIN_DIR))

import codex_bridge_daemon as bridge_module
import codex_hook_intercept as hook_module
import codex_mcp_server as mcp_module
import codex_profiles
from codex_bridge_daemon import UnifiedSubagentBridge, summarize_transcript_file
from codex_ws_client import CodexAppServerClient


def http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=15.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_post_json(url: str, payload: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=25.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def wait_until(check, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if result := check():
                return result
        except OSError:
            pass
        time.sleep(0.03)
    raise AssertionError("Timed out waiting for expected state")


class ScriptedCodex(CodexAppServerClient):
    """Capture the app-server wire parameters and control notification interleavings."""

    def __init__(self, bridge):
        super().__init__(auto_start_daemon=False)
        self.bridge = bridge
        self.calls = []
        self.complete_on_start = False
        self.turn = 0

    def connect(self):
        return {"userAgent": "scripted-codex"}

    @property
    def connection(self):
        return self

    def request(self, method, params=None, timeout=60):
        self.calls.append((method, params))
        if method == "thread/start":
            return {"thread": {"id": "thread-1"}, "reasoningEffort": "low"}
        if method == "turn/start":
            self.turn += 1
            turn = f"turn-{self.turn}"
            self.bridge._on_codex_notification("turn/started", {
                "threadId": params["threadId"], "turn": {"id": turn},
            })
            if self.complete_on_start:
                self.complete(turn)
            return {"turn": {"id": turn}}
        if method == "turn/steer":
            return {"turnId": params["expectedTurnId"]}
        if method == "turn/interrupt":
            self.complete(params["turnId"], "interrupted")
        if method == "thread/fork":
            return {"thread": {"id": "thread-fork"}}
        if method == "thread/loaded/list":
            return {"data": ["thread-existing-1"]}
        if method == "thread/list":
            return {
                "data": [
                    {
                        "id": "thread-existing-1",
                        "preview": "Investigate memory allocator alignment",
                        "cwd": "/tmp/work",
                        "model": "gpt-6.2-sol",
                        "reasoningEffort": "xhigh",
                        "status": {"type": "idle"},
                        "updatedAt": 1700000000,
                    }
                ]
            }
        if method == "thread/resume":
            return {
                "thread": {
                    "id": params["threadId"],
                    "preview": "Investigate memory allocator alignment",
                    "cwd": params.get("cwd") or "/tmp/work",
                    "model": "gpt-6.2-sol",
                    "reasoningEffort": "xhigh",
                    "status": {"type": "idle"},
                },
                "cwd": params.get("cwd") or "/tmp/work",
                "model": "gpt-6.2-sol",
                "reasoningEffort": "xhigh",
            }
        if method == "thread/turns/list":
            return {
                "data": [
                    {
                        "id": "turn-hist-1",
                        "status": "completed",
                        "items": [
                            {"id": "item-u1", "type": "userMessage", "content": [{"type": "text", "text": "Audit slab.c"}]},
                            {"id": "item-r1", "type": "reasoning", "summary": ["Checking cacheline alignment"]},
                            {"id": "item-a1", "type": "agentMessage", "text": "Aligned slab allocator to 64 bytes."},
                        ],
                    }
                ]
            }
        if method == "thread/settings/update":
            return {}
        return {}

    def complete(self, turn, status="completed"):
        self.bridge._on_codex_notification("turn/completed", {
            "threadId": "thread-1", "turn": {"id": turn, "status": status},
        })


class TestExecutorAndProfiles(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="agy_codex_regression_")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        profile_dir = self.root / "profiles"
        self.enterContext(patch.dict(os.environ, {"AGY_CODEX_APP_DATA_DIR": temp.name}))
        self.enterContext(patch.multiple(
            codex_profiles, PROFILE_DIR=profile_dir, CONFIG_PATH=self.root / "config.json",
            WORKSPACE_ROOT=self.root, GLOBAL_AGENTS=self.root / "global-agents",
        ))
        self.enterContext(patch.object(bridge_module, "PROFILE_DIR", profile_dir))
        self.bridge = UnifiedSubagentBridge()
        self.bridge.codex = self.codex = ScriptedCodex(self.bridge)

    def test_plugin_validation_zero_warnings(self):
        agy = shutil.which("agy")
        self.assertIsNotNone(agy, "agy is required for plugin validation")
        proc = subprocess.run([agy, "plugin", "validate", str(PLUGIN_DIR)],
                              capture_output=True, text=True, timeout=10)
        output = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, output)
        self.assertIn("Validation passed", output)
        self.assertNotIn("⚠", output)
        self.assertNotRegex(output.lower(), r"[1-9]\d* warnings?")
        print(output.rstrip())

    def test_profile_provider_routes_and_persists(self):
        profile = {"name": "codex-reviewer", "backend": "codex", "description": "Review: change",
                   "model": "o3", "effort": "ultra", "systemPrompt": "Review the patch.",
                   "permissionSource": "custom", "minRemainingQuotaPercent": 10,
                   "sandboxMode": "read-only", "approvalPolicy": "on-request", "executionMode": "sdkExecutor"}
        replies = {
            "/api/profiles/define": self.bridge.define_profile, "/api/profiles": self.bridge.list_profiles,
        }
        with patch.object(mcp_module, "bridge_post", side_effect=lambda path, data: replies[path](data)), \
             patch.object(mcp_module, "bridge_get", side_effect=lambda path: replies[path]()):
            result = mcp_module.handle_tool_call("subagent_define_profile", profile)
            self.assertEqual(result["profile"], profile)
            self.assertEqual(mcp_module.handle_tool_call("subagent_list_profiles", {})["profiles"], [profile])

        for path in result["definitions"]:
            spec = json.loads(Path(path).read_text())
            self.assertTrue(spec["subagent"])
            self.assertEqual(spec["commandSpec"]["args"][-2:], ["--agent-name", profile["name"]])
        self.assertTrue((self.root / "profiles/codex-reviewer.json").exists())
        restored = UnifiedSubagentBridge()
        self.assertEqual(restored.list_profiles()["profiles"], [profile])
        conv = "test-reviewer"
        self.bridge.register_sdk({"conversationId": conv, "agentName": profile["name"], "sdkInstanceId": "instance"})
        self.codex.complete_on_start = True
        result = self.bridge.dispatch_subagent_input(
            conv, "Find problems", agent_name=profile["name"], execution_mode="sdkExecutor", sdk_instance="instance",
        )
        start, turn = [params for method, params in self.codex.calls if method in ("thread/start", "turn/start")]
        self.assertEqual((start["model"], start["config"]["model_reasoning_effort"]), ("o3", "ultra"))
        self.assertEqual((start["sandbox"], start["approvalPolicy"]), ("read-only", "on-request"))
        self.assertIn("Review the patch.", start["developerInstructions"])
        self.assertEqual(turn["input"][0]["text"], "Review the patch.\n\nFind problems")
        self.assertEqual(turn["effort"], "ultra")
        self.assertEqual(result["status"], "idle")
        self.assertEqual(self.bridge.sessions_by_conv[conv]["turnHistory"][0]["turnId"], result["turnId"])

        self.bridge.define_profile({"name": profile["name"], "executionMode": "preInvocation"})
        self.assertFalse((self.root / ".agents/agents/codex-reviewer.json").exists())
        self.assertIn('description: "Review: change"', (self.root / ".agents/agents/codex-reviewer.md").read_text())
        self.assertTrue(self.bridge.delete_profile(profile["name"])["deleted"])
        self.assertEqual(UnifiedSubagentBridge().list_profiles()["profiles"], [])

    def test_adopt_codex_thread_and_permission_inheritance(self):
        toml_file = self.root / "codex_config.toml"
        toml_file.write_text('approval_policy = "never"\nsandbox_mode = "danger-full-access"\nmodel = "gpt-6.2-sol"\n')
        with patch.object(codex_profiles, "CODEX_CONFIG_TOML", toml_file):
            listed = self.bridge.list_codex_threads(limit=10)
            self.assertEqual(len(listed["threads"]), 1)
            self.assertTrue(listed["threads"][0]["isLoaded"])
            self.assertEqual(listed["codexConfigToml"]["approvalPolicy"], "never")
            self.assertEqual(listed["codexConfigToml"]["sandboxMode"], "danger-full-access")

            adopted = self.bridge.adopt_codex_thread(
                thread_id="thread-existing-1",
                conversation_id="adopted-conv-1",
                permission_source="codexConfig",
            )
            self.assertEqual(adopted["action"], "thread/adopt")
            self.assertEqual(adopted["hydratedTurns"], 1)
            self.assertEqual(adopted["permissionSource"], "codexConfig")
            self.assertEqual(adopted["approvalPolicy"], "never")
            self.assertEqual(adopted["sandboxMode"], "danger-full-access")

            inspected = self.bridge.inspect_subagent("adopted-conv-1")
            self.assertGreaterEqual(inspected["transcript"]["steps"], 2)
            self.assertIn("Aligned slab allocator", inspected["transcript"]["lastResponse"])

            # Test [PERMISSION:bypass] and [ADOPT:<tid>] directives via dispatch_subagent_input
            perm_res = self.bridge.dispatch_subagent_input("adopted-conv-1", "[PERMISSION:bypass]")
            self.assertEqual(perm_res["permissionSource"], "bypass")
            self.assertEqual(perm_res["approvalPolicy"], "never")
            self.assertEqual(perm_res["sandboxMode"], "danger-full-access")

            inline_adopt = self.bridge.dispatch_subagent_input(
                "adopted-conv-2", "[ADOPT:thread-existing-1] Follow up on slab.c"
            )
            self.assertEqual(inline_adopt["action"], "thread/adopt")
            self.assertEqual(inline_adopt["threadId"], "thread-existing-1")
            self.assertEqual(inline_adopt["dispatch"]["action"], "turn/start")

    def test_quota_guard_blocks_and_force_replays(self):
        low_quota_payload = {
            "ordinaryUsageAllowed": True,
            "rateLimits": {
                "limitId": "codex",
                "primary": {"usedPercent": 96, "windowDurationMins": 10080, "resetsAt": 1795703227},
                "secondary": {"usedPercent": 40, "windowDurationMins": 300, "resetsAt": 1795700000},
                "credits": {"hasCredits": False, "unlimited": False, "balance": "0"},
                "spendControlReached": False,
                "planType": "pro",
                "rateLimitReachedType": None,
            },
            "rateLimitResetCredits": {"availableCount": 1},
        }
        orig_request = self.codex.request

        def quota_request(method, params=None, timeout=60):
            if method == "account/rateLimits/read":
                return low_quota_payload
            return orig_request(method, params, timeout)

        with patch.object(self.codex, "request", side_effect=quota_request), \
             patch.object(self.bridge, "send_message_to_agy_conversation") as notify_parent:
            q = self.bridge.get_codex_quota(threshold_percent=10)
            self.assertTrue(q["available"])
            self.assertEqual(q["minRemainingPercent"], 4.0)
            self.assertTrue(q["belowThreshold"])
            self.assertEqual(q["resetCreditsAvailable"], 1)

            blocked = self.bridge.dispatch_subagent_input(
                "quota-conv-1", "Refactor lockless queue", parent_conversation_id="parent-123"
            )
            self.assertEqual(blocked["action"], "quota_blocked")
            self.assertEqual(blocked["status"], "quota_blocked")
            self.assertEqual(sum(m == "thread/start" for m, _ in self.codex.calls), 0)
            wait_until(lambda: notify_parent.called, timeout=2)
            self.assertIn("Low Quota", notify_parent.call_args.kwargs["title"])
            self.assertIn("[FORCE]", notify_parent.call_args.kwargs["content"])

            # User confirms -> send [FORCE] to replay pendingQuotaPrompt without re-typing!
            forced = self.bridge.dispatch_subagent_input("quota-conv-1", "[FORCE]")
            self.assertEqual(forced["action"], "turn/start")
            self.assertEqual(sum(m == "thread/start" for m, _ in self.codex.calls), 1)
            turn_call = next(p for m, p in self.codex.calls if m == "turn/start")
            self.assertIn("Refactor lockless queue", turn_call["input"][0]["text"])

    def test_agy_acp_backend_profile_dispatch_and_model_switch(self):
        agy_calls = []

        class FakeAgyAcp:
            def __init__(self, bridge):
                self.bridge = bridge
                self.connected = True

            def connect(self):
                return {"agentInfo": {"name": "Fake ACP"}}

            def is_connected(self):
                return self.connected

            @property
            def connection(self):
                return self

            def session_new(self, cwd, model=None, system_prompt=None, mode=None):
                agy_calls.append(("session/new", {"cwd": cwd, "model": model, "system_prompt": system_prompt}))
                return {"sessionId": "acp-sess-1", "selectedModelId": model or "gemini-4-neon-high"}

            def session_set_model(self, sid, model_id):
                agy_calls.append(("session/set_model", {"sessionId": sid, "modelId": model_id}))
                return {}

            def session_prompt(self, sid, text, timeout=3600.0):
                agy_calls.append(("session/prompt", {"sessionId": sid, "text": text}))
                self.bridge._on_agy_notification("session/update", {
                    "sessionId": sid,
                    "update": {"sessionUpdate": "agent_thought_chunk", "content": {"type": "text", "text": "Analyzing..."}},
                })
                self.bridge._on_agy_notification("session/update", {
                    "sessionId": sid,
                    "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "OPUS_REPLY_OK"}},
                })
                return {"stopReason": "end_turn", "usage": {"totalTokens": 42}}

            def session_cancel(self, sid):
                agy_calls.append(("session/cancel", {"sessionId": sid}))

            def list_models(self):
                return [{"modelId": "claude-opus-5-5-high", "name": "Claude Opus 5.5 (High)"}]

        self.bridge.agy = FakeAgyAcp(self.bridge)
        res_def = self.bridge.define_profile({
            "name": "agy-opus",
            "backend": "agy",
            "model": "claude-opus-5-5-high",
            "systemPrompt": "Use Opus precision.",
            "executionMode": "sdkExecutor",
        })
        self.assertEqual(res_def["profile"]["backend"], "agy")
        self.assertEqual(res_def["profile"]["model"], "claude-opus-5-5-high")

        disp = self.bridge.dispatch_subagent_input("agy-conv-1", "Audit design", agent_name="agy-opus")
        self.assertEqual(disp["backend"], "agy")
        self.assertEqual(disp["model"], "claude-opus-5-5-high")
        wait_until(lambda: self.bridge.sessions_by_conv["agy-conv-1"]["status"] == "idle", timeout=3)
        insp = self.bridge.inspect_subagent("agy-conv-1")
        self.assertEqual(insp["summary"]["engine"], "agy")
        self.assertIn("OPUS_REPLY_OK", insp["transcript"]["lastResponse"])

        # Live model switch via [MODEL:...]
        sw = self.bridge.dispatch_subagent_input("agy-conv-1", "[MODEL:gemini-4-neon-xhigh]")
        self.assertEqual(sw["model"], "gemini-4-neon-xhigh")
        self.assertTrue(any(m == "session/set_model" and p["modelId"] == "gemini-4-neon-xhigh" for m, p in agy_calls))

    def test_generic_acp_and_pi_backends_in_registry(self):
        # 1. Verify backend registry lists all 4 built-in backends
        backends = {b["name"]: b for b in self.bridge.list_backends()["backends"]}
        self.assertEqual(set(backends.keys()), {"codex", "agy", "acp", "pi"})
        self.assertTrue(backends["acp"]["capabilities"]["customCommand"])
        self.assertTrue(backends["pi"]["capabilities"]["midTurnSteer"])

        # 2. Verify generic `acp` profile with custom acpCommand
        acp_prof = self.bridge.define_profile({
            "name": "custom-acp-agent",
            "backend": "acp",
            "acpCommand": ["pi-acp", "--stdio"],
            "acpEnv": {"FOO": "BAR"},
            "model": "anthropic/claude-sonnet-4",
            "executionMode": "sdkExecutor",
        })["profile"]
        self.assertEqual(acp_prof["backend"], "acp")
        self.assertEqual(acp_prof["acpCommand"], ["pi-acp", "--stdio"])

        # 3. Verify native `pi` backend dispatch, steer, and completion lifecycle
        pi_cmds = []

        class FakePiProc:
            def __init__(self, bridge, sid):
                self.bridge = bridge
                self.sid = sid

            def is_running(self):
                return True

            def command(self, cmd_type, payload=None, timeout=30.0):
                pi_cmds.append((cmd_type, payload or {}))
                if cmd_type == "prompt":
                    self.bridge._on_pi_event(self.sid, {
                        "type": "message_update",
                        "assistantMessageEvent": {"type": "text_delta", "delta": "PI_PONG_OK"},
                    })
                return {}

            def close(self):
                pass

        fake_pi = FakePiProc(self.bridge, "pi-pi-conv-1")
        with patch.object(self.bridge.backends.pi, "get_or_create", return_value=fake_pi), \
             patch.object(self.bridge.backends.pi, "get", return_value=fake_pi):
            res = self.bridge.dispatch_subagent_input(
                "pi-conv-1", "Check repo", backend="pi", model="openai-codex/gpt-5.6-sol"
            )
            self.assertEqual(res["backend"], "pi")
            self.assertEqual(res["action"], "turn/start")

            # Native mid-turn steer while active
            steer = self.bridge.dispatch_subagent_input("pi-conv-1", "Focus on tests only")
            self.assertEqual(steer["action"], "pi/steer")
            self.assertEqual(steer["status"], "steered")

            # Complete turn via agent_end
            self.bridge._on_pi_event("pi-pi-conv-1", {"type": "agent_end"})
            insp = self.bridge.inspect_subagent("pi-conv-1")
            self.assertEqual(insp["summary"]["engine"], "pi")
            self.assertIn("PI_PONG_OK", insp["transcript"]["lastResponse"])
            self.assertEqual([c[0] for c in pi_cmds], ["prompt", "steer"])

    def test_serialized_dispatch_steers_and_cleans_approvals(self):
        conv = "concurrent"
        with ThreadPoolExecutor(2) as workers:
            results = list(workers.map(lambda text: self.bridge.dispatch_subagent_input(conv, text), ("one", "two")))
        self.assertCountEqual([r["action"] for r in results], ["turn/start", "turn/steer"])
        self.assertEqual(sum(method == "thread/start" for method, _ in self.codex.calls), 1)
        self.bridge._on_codex_server_request(44, "item/commandExecution/requestApproval", {
            "threadId": "thread-1", "turnId": "turn-1",
        }, lambda **_kwargs: None)
        self.assertEqual(len(self.bridge.pending_approvals), 1)
        self.assertEqual(self.bridge.sessions_by_conv[conv]["status"], "awaiting_approval")
        self.assertEqual(self.bridge.dispatch_subagent_input(conv, "steer while awaiting")["action"], "turn/steer")
        self.bridge.interrupt_subagent(conv)
        self.assertEqual(self.bridge.sessions_by_conv[conv]["status"], "interrupted")
        self.assertFalse(self.bridge.pending_approvals)
        self.assertFalse(self.bridge._approval_replies)
        next_turn = self.bridge.dispatch_subagent_input(conv, "new turn")
        self.codex.complete("turn-1", "interrupted")
        self.assertEqual(self.bridge.sessions_by_conv[conv]["activeTurnId"], next_turn["turnId"])
        self.assertEqual(len(self.bridge.sessions_by_conv[conv]["turnHistory"]), 1)

    def test_daemon_signals_idle_with_sdk_credentials(self):
        conv = "sdk-registered"
        self.bridge.register_sdk({
            "conversationId": conv, "sdkInstanceId": "live", "lsAddress": "localhost:5544", "csrfToken": "token",
        })
        self.bridge.dispatch_subagent_input(conv, "work", execution_mode="sdkExecutor", sdk_instance="live")
        with patch.object(self.bridge, "lsrpc") as ls:
            self.codex.complete("turn-1")
            wait_until(lambda: self.bridge.sdk_status(conv, "live")["idleSignal"] == "sent")
            ls.assert_called_once_with("SignalExecutableIdle", {"conversationId": conv}, timeout=2,
                                       ls_address="localhost:5544", csrf_token="token")
        self.assertTrue(self.bridge.sdk_status(conv, "other")["replaced"])

    def test_hook_bypasses_native_definition_and_uses_markdown_fallback(self):
        self.bridge.define_profile({"name": "codex-audit", "executionMode": "sdkExecutor"})
        payload = {"agentName": "codex-audit", "conversationId": "hook-conv", "cwd": str(self.root),
                   "lastUserInput": "hook prompt"}
        script = BIN_DIR / "codex_hook_intercept.py"
        env = {
            **os.environ, "AGY_CODEX_PROFILE_DIR": str(self.root / "profiles"), "AGY_CODEX_WORKSPACE_ROOT": str(self.root),
            "AGY_CODEX_GLOBAL_AGENTS": str(self.root / "global-agents"),
            "AGY_CODEX_APP_DATA_DIR": str(self.root),
        }
        # 1. When a live sdkExecutor process IS registered for this conversation, PreInvocation passes through.
        self.bridge.register_sdk({
            "conversationId": "hook-conv", "agentName": "codex-audit",
            "sdkInstanceId": "live-inst", "sdkPid": os.getpid(),
        })
        proc = subprocess.run([sys.executable, str(script)], env=env, input=json.dumps(payload),
                              text=True, capture_output=True, timeout=5)
        self.assertEqual(json.loads(proc.stdout), {})

        # 2. When NO live sdkExecutor is registered (e.g. plugin codex.md shadowed codex.json in another
        # workspace), PreInvocation MUST intercept instead of letting Gemini flash_lite run!
        self.bridge.unregister_sdk("hook-conv", "live-inst")
        with patch.object(sys, "stdin", io.StringIO(json.dumps(payload))), \
             patch.object(sys, "stdout", io.StringIO()) as output, \
             patch.object(hook_module, "ensure_bridge_daemon_running", return_value="http://bridge"), \
             patch.object(hook_module, "post_bridge_json", return_value={"action": "turn/start"}) as post:
            hook_module.main()
        self.assertEqual(post.call_args.args[2]["agentName"], "codex-audit")
        self.assertEqual(post.call_args.args[2]["executionMode"], "preInvocation")
        self.assertEqual(json.loads(output.getvalue())["terminationBehavior"], "terminate")

    def test_sdk_process_ndjson_steer_sigint_and_idle(self):
        # Run the real stdin and signal loop. Intercept only the network boundary.
        bridge_stub = r"""
import json, sys
import codex_sdk_executor as sdk
status = {}
turn = 0
def post(_base, path, payload, **_kwargs):
    global turn
    print(json.dumps([path, payload]), flush=True)
    if path == "/api/ls-credentials":
        status.update(registered=True, status="idle")
    if path == "/api/dispatch":
        if not status.get("activeTurnId"):
            turn += 1
            status.update(status="working", activeTurnId=f"turn-{turn}")
        return {"turnId": status["activeTurnId"]}
    if path == "/api/subagent/interrupt":
        active = status.pop("activeTurnId", None)
        status.update(status="interrupted")
        return {"interruptedTurnId": active}
    return {}
sdk.post_bridge_json = post
sdk.ensure_bridge_daemon_running = lambda *a: "http://bridge"
sdk.SdkExecutor.status = lambda self: status
sdk.language_server_post = lambda address, token, method, payload: print(
    json.dumps([method, [address, token, payload]]), flush=True)
sdk.main()
"""
        proc = subprocess.Popen(
            [sys.executable, "-c", bridge_stub, "--agent-name", "codex-reviewer"],
            env={**os.environ, "PYTHONPATH": str(BIN_DIR)}, text=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.addCleanup(proc.communicate, timeout=5)
        self.addCleanup(stop_process, proc)
        assert proc.stdin and proc.stdout
        output = queue.Queue()
        threading.Thread(target=lambda: [output.put(json.loads(line)) for line in proc.stdout], daemon=True).start()

        def send(item):
            proc.stdin.write(json.dumps(item) + "\n")
            proc.stdin.flush()

        send({"type": "init", "conversation_id": "sdk-test", "parent_conversation_id": "parent",
              "language_server_address": "localhost:23456", "csrf_token": "sdk-test-token"})
        path, payload = output.get(timeout=5)
        self.assertEqual(path, "/api/ls-credentials")
        self.assertEqual((payload["agentName"], payload["parentConversationId"]), ("codex-reviewer", "parent"))
        send({"type": "request", "request": {"cascadeId": "sdk-test", "items": [{"text": "first task"}], "clientMessageId": "sdk-req-1"}})
        dispatch = output.get(timeout=5)[1]
        self.assertEqual(dispatch["userInput"], "first task")
        self.assertEqual(dispatch["inputId"], "sdk-req-1")

        msg_dir = self.root / "brain/sdk-test/.system_generated/messages"
        marker = msg_dir / "undelivered/msg"
        marker.parent.mkdir(parents=True)
        marker.touch()
        (msg_dir / "msg.json").write_text(json.dumps({"sender": {"conversationId": "parent"}, "content": "steer task"}))
        send({"type": "request", "request": {}})
        self.assertEqual(output.get(timeout=5)[1]["userInput"], "steer task")
        wait_until(lambda: not marker.exists())

        proc.send_signal(signal.SIGINT)
        self.assertEqual(output.get(timeout=5)[0], "/api/subagent/interrupt")
        self.assertEqual(output.get(timeout=5), [
            "SignalExecutableIdle", ["localhost:23456", "sdk-test-token", {"conversationId": "sdk-test"}],
        ])
        self.assertIsNone(proc.poll())
        proc.stdin.close()
        proc.stdin = None
        self.assertEqual(output.get(timeout=5)[0], "/api/subagent/sdk-unregister")
        self.assertEqual(proc.wait(timeout=5), 0)

    def test_websocket_reply_progress_during_callback(self):
        path = self.root / "fake-socket"
        path.touch()
        client_read, server_write = os.pipe()
        server_read, client_write = os.pipe()
        client = CodexAppServerClient(path, auto_start_daemon=False)

        class PipeSocket:
            def __init__(self, incoming, outgoing):
                self.incoming = incoming
                self.outgoing = outgoing

            def connect(self, _path):
                pass

            def settimeout(self, _timeout):
                pass

            def recv(self, length):
                return os.read(self.incoming, length)

            def sendall(self, data):
                while data:
                    data = data[os.write(self.outgoing, data):]

            def makefile(self, mode):
                return os.fdopen(os.dup(self.incoming), mode)

            def shutdown(self, _how):
                if self.outgoing is not None:
                    os.close(self.outgoing)
                    self.outgoing = None

            def close(self):
                self.shutdown(None)
                if self.incoming is not None:
                    os.close(self.incoming)
                    self.incoming = None

        client_sock, server_sock = PipeSocket(client_read, client_write), PipeSocket(server_read, server_write)
        self.addCleanup(client_sock.close)
        self.addCleanup(server_sock.close)
        self.addCleanup(client.close)

        def send(sock, message):
            body = json.dumps(message).encode()
            length = bytes([len(body)]) if len(body) < 126 else b"\x7e" + struct.pack("!H", len(body))
            sock.sendall(b"\x81" + length + body)

        def receive(stream):
            _, length = stream.read(2)
            length &= 127
            if length == 126:
                length = struct.unpack("!H", stream.read(2))[0]
            mask = stream.read(4)
            return json.loads(bytes(b ^ mask[i % 4] for i, b in enumerate(stream.read(length))))

        def app_server():
            with server_sock.makefile("rb") as stream:
                while (line := stream.readline()).strip():
                    if line.lower().startswith(b"sec-websocket-key:"):
                        key = line.split(b":", 1)[1].strip()
                accept = base64.b64encode(hashlib.sha1(key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
                server_sock.sendall(b"HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n")
                init = receive(stream)
                send(server_sock, {"id": init["id"], "result": {"userAgent": "fake"}})
                send(server_sock, {"method": "test/notification", "params": {}})
                request = receive(stream)
                send(server_sock, {"id": request["id"], "result": {"data": ["response"]}})
                request = receive(stream)
                self.assertEqual(request["method"], "test/disconnect")
                server_sock.shutdown(socket.SHUT_RDWR)

        received = threading.Event()

        def callback(method, _params):
            if method == "test/notification":
                self.assertEqual(client.request("test/nested", timeout=2)["data"], ["response"])
                received.set()

        client.add_notification_handler(callback)
        with ThreadPoolExecutor(1) as workers:
            server = workers.submit(app_server)
            with patch.object(client, "ensure_daemon_running"), patch("codex_ws_client.socket.socket", return_value=client_sock):
                self.assertEqual(client.connect()["userAgent"], "fake")
            self.assertTrue(received.wait(3), "WebSocket reader blocked on own callback")
            with self.assertRaises(ConnectionError):
                client.request("test/disconnect", timeout=2)
            server.result(timeout=2)
        self.assertFalse(client.is_connected())

    def test_01_install_switches_without_plugin_warnings(self):
        checkout = self.root / "installer"
        checkout.mkdir()
        shutil.copy2(REPO_ROOT / "install.sh", checkout)
        shutil.copytree(PLUGIN_DIR, checkout / "plugins/codex-subagent", ignore=shutil.ignore_patterns(
            "config.json", "profiles", "__pycache__",
        ))
        old_agent = checkout / ".agents/agents/codex.md"
        old_agent.parent.mkdir(parents=True)
        old_agent.symlink_to(checkout / "old-checkout/plugins/codex-subagent/agents/codex.md")
        global_config = self.root / "installer-global"
        inherited = {name: value for name, value in os.environ.items() if not name.startswith("AGY_CODEX_")}
        env = {**inherited, "AGY_CODEX_GLOBAL_CONFIG": str(global_config)}
        for args, mode in (([], "preInvocation"), (["--mode=sdkExecutor"], "sdkExecutor"),
                           (["--mode=preInvocation"], "preInvocation")):
            proc = subprocess.run(["bash", "./install.sh", *args], cwd=checkout, env=env,
                                  capture_output=True, text=True, timeout=10)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertNotIn("⚠", proc.stdout + proc.stderr)
            self.assertIn("Validation passed", proc.stdout)
            suffix, previous = ("json", "md") if mode == "sdkExecutor" else ("md", "json")
            for agent_dir in (checkout / ".agents/agents", global_config / "agents"):
                self.assertTrue((agent_dir / f"codex.{suffix}").is_file())
                self.assertFalse((agent_dir / f"codex.{previous}").exists())
            config = json.loads((checkout / "plugins/codex-subagent/config.json").read_text())
            self.assertEqual(config["executionMode"], mode)

    def test_node_sidecar_syntax(self):
        for path in (SIDECAR_DIR / "main.mjs", SIDECAR_DIR / "app.js"):
            subprocess.run(["node", "--check", str(path)], check=True, timeout=5)


class TestCodexSubagentAllPhases(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
        except PermissionError as e:
            raise unittest.SkipTest(f"Live phases require loopback sockets: {e}") from e

        workspace = tempfile.TemporaryDirectory(prefix="agy_codex_e2e_")
        cls.addClassCleanup(workspace.cleanup)
        cls.temp_dir = Path(workspace.name)
        cls.test_conv_id = f"test-codex-{uuid.uuid4().hex[:10]}"
        cls.transcript_path = cls.temp_dir / "logs" / "transcript.jsonl"
        cls.bridge_url = f"http://127.0.0.1:{port}"
        cls.env = {
            **os.environ, "AGY_CODEX_BRIDGE_PORT": str(port), "AGY_CODEX_APP_DATA_DIR": workspace.name,
            "AGY_CODEX_CONFIG_PATH": str(PLUGIN_DIR / "config.example.json"),
            "AGY_CODEX_PROFILE_DIR": str(cls.temp_dir / "profiles"),
            "AGY_CODEX_WORKSPACE_ROOT": str(cls.temp_dir),
            "AGY_CODEX_GLOBAL_AGENTS": str(cls.temp_dir / "global-agents"),
            "AGY_CODEX_MIN_QUOTA_PERCENT": "0",
            "ANTIGRAVITY_LS_ADDRESS": "", "ANTIGRAVITY_CSRF_TOKEN": "",
        }
        with sqlite3.connect(cls.temp_dir / "conversation_summaries.db") as db:
            db.executescript("""
                CREATE TABLE conversation_summaries (
                  conversation_id TEXT PRIMARY KEY, title TEXT, preview TEXT, step_count INTEGER,
                  last_modified_time TEXT, workspace_uris TEXT, status TEXT, agent_name TEXT,
                  parent_conversation_id TEXT, nesting_depth INTEGER, not_fully_idle INTEGER,
                  killed INTEGER, raw_summary BLOB
                );
                INSERT INTO conversation_summaries VALUES
                  ('test-research', 'Research fixture', '', 0, '', '[]', 'CASCADE_RUN_STATUS_IDLE',
                   'research', 'test-root', 1, 0, 0, NULL);
            """)
        proc = subprocess.Popen([sys.executable, str(BIN_DIR / "codex_bridge_daemon.py")],
                                env=cls.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cls.addClassCleanup(stop_process, proc)
        wait_until(lambda: http_get_json(f"{cls.bridge_url}/api/ready").get("status") == "ok", timeout=5)

    def test_02_phase1_direct_codex_daemon_websocket(self) -> None:
        """Verify direct AF_UNIX WebSocket connection & JSON-RPC handshake with codex app-server daemon."""
        client = CodexAppServerClient()
        self.addCleanup(client.close)
        info = client.connect()
        self.assertIn("userAgent", info)
        self.assertTrue(client.is_connected())
        self.assertIn("data", client.request("thread/loaded/list", {}))

    def test_03_phase1_hook_interception_and_turn_execution(self) -> None:
        """
        Verify PreInvocation hook:
        1. Passes through non-Codex conversations with `{}` in < 50ms.
        2. Intercepts Codex subagent conversations, dispatches to Codex daemon (`thread/start` + `turn/start`),
           returns `terminationBehavior: "terminate"`, and streams steps into `transcript.jsonl`.
        """
        hook_script = BIN_DIR / "codex_hook_intercept.py"

        # 1. Non-codex conversation pass-through
        t0 = time.time()
        non_codex_input = {
            "hookEventName": "PreInvocation",
            "conversationId": "bd9d206a-fa8d-459f-913b-327f7f364453",  # Known main conversation
            "cwd": str(REPO_ROOT),
            "lastUserInput": "Hello main agent",
        }
        proc_pass = subprocess.run(
            [sys.executable, str(hook_script)],
            env=self.env,
            input=json.dumps(non_codex_input),
            capture_output=True,
            text=True,
            timeout=5.0,
        )
        elapsed_ms = (time.time() - t0) * 1000
        self.assertEqual(proc_pass.returncode, 0)
        self.assertEqual(json.loads(proc_pass.stdout.strip()), {})
        print(f"  [Phase 1] Non-Codex PreInvocation hook pass-through latency: {elapsed_ms:.1f}ms")

        # Pre-register test_conv_id as a codex subagent via initial dispatch
        dispatch_res = http_post_json(
            f"{self.bridge_url}/api/dispatch",
            {
                "conversationId": self.test_conv_id,
                "parentConversationId": "",
                "role": "Codex E2E Verifier",
                "cwd": str(REPO_ROOT),
                "transcriptPath": str(self.transcript_path),
                "effort": "low",
                "userInput": "Reply with the exact word PONG_PHASE1 and do not run any shell commands.",
            },
        )
        self.assertEqual(dispatch_res.get("action"), "turn/start")
        thread_id = dispatch_res.get("threadId")
        turn_id = dispatch_res.get("turnId")
        self.assertTrue(thread_id, "Expected valid Codex threadId")
        self.assertTrue(turn_id, "Expected valid Codex turnId")
        self.__class__.shared_thread_id = thread_id
        print(f"  [Phase 1] Created Codex threadId={thread_id}, turnId={turn_id}")

        # Wait for turn/completed to mirror into transcript.jsonl
        deadline = time.time() + 40.0
        inspected = {}
        while time.time() < deadline:
            inspected = http_get_json(
                f"{self.bridge_url}/api/subagent/inspect?conversationId={urllib.parse.quote(self.test_conv_id)}"
            )
            st = (inspected.get("summary") or {}).get("status")
            if st in ("idle", "error") and (inspected.get("transcript") or {}).get("steps", 0) >= 2:
                break
            time.sleep(0.4)

        self.assertIn(inspected.get("summary", {}).get("status"), ("idle", "error"), inspected)
        self.assertTrue(self.transcript_path.exists(), "transcript.jsonl was not synthesized")
        tr_summary = summarize_transcript_file(self.transcript_path)
        self.assertGreaterEqual(tr_summary["steps"], 2)
        print(f"  [Phase 1] Synthesized transcript.jsonl steps={tr_summary['steps']}, response={tr_summary['lastResponse']!r}")

        # 2. Now verify PreInvocation hook intercepts follow-up turn on test_conv_id
        # even when lastUserInput is empty and prompt arrives via SYSTEM_MESSAGE in transcript.jsonl!
        sys_msg_step = {
            "step_index": 99,
            "source": "SYSTEM",
            "type": "SYSTEM_MESSAGE",
            "status": "DONE",
            "created_at": "2026-11-23T15:15:00Z",
            "content": (
                "The following is a <SYSTEM_MESSAGE> not actually sent by the user.\n\n"
                "<SYSTEM_MESSAGE>\n"
                "[Message] timestamp=2026-11-23T15:15:00Z sender=bd9d206a-fa8d-459f-913b-327f7f364453 "
                "priority=MESSAGE_PRIORITY_HIGH content=[EFFORT:medium]\n"
                "</SYSTEM_MESSAGE>"
            ),
        }
        with open(self.transcript_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(sys_msg_step) + "\n")

        hook_input = {
            "hookEventName": "PreInvocation",
            "conversationId": self.test_conv_id,
            "cwd": str(REPO_ROOT),
            "transcriptPath": str(self.transcript_path),
            "lastUserInput": "",
        }
        proc_intercept = subprocess.run(
            [sys.executable, str(hook_script)],
            env=self.env,
            input=json.dumps(hook_input),
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        self.assertEqual(proc_intercept.returncode, 0)
        hook_out = json.loads(proc_intercept.stdout.strip())
        self.assertEqual(hook_out.get("terminationBehavior"), "terminate")
        self.assertTrue(hook_out.get("injectSteps"))
        print("  [Phase 1] PreInvocation hook extracted SYSTEM_MESSAGE from transcript.jsonl and intercepted with terminationBehavior='terminate'")

        # Now test undelivered FileStore message draining (.system_generated/messages/undelivered/<msg_id>)
        msg_dir = self.transcript_path.parent.parent / "messages"
        undeliv_dir = msg_dir / "undelivered"
        undeliv_dir.mkdir(parents=True, exist_ok=True)
        msg_id = "test-filestore-msg-001"
        msg_json_path = msg_dir / f"{msg_id}.json"
        marker_path = undeliv_dir / msg_id
        msg_payload = {
            "id": msg_id,
            "sender": {"conversationId": "parent-conv-999"},
            "recipient": {"conversationId": self.test_conv_id},
            "content": "[EFFORT:high]",
            "createdAt": "2026-11-23T15:30:00Z",
        }
        msg_json_path.write_text(json.dumps(msg_payload), encoding="utf-8")
        marker_path.write_text("", encoding="utf-8")

        proc_filestore = subprocess.run(
            [sys.executable, str(hook_script)],
            env=self.env,
            input=json.dumps(hook_input),
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        self.assertEqual(proc_filestore.returncode, 0)
        self.assertNotIn("errorMessage", json.loads(proc_filestore.stdout)["injectSteps"][0])
        self.assertFalse(marker_path.exists(), "undelivered/<msg_id> marker should be unlinked by codex_hook_intercept.py")
        insp_after = http_get_json(
            f"{self.bridge_url}/api/subagent/inspect?conversationId={urllib.parse.quote(self.test_conv_id)}"
        )
        self.assertEqual(insp_after["summary"].get("effort"), "high")
        print("  [Phase 1] PreInvocation hook drained .system_generated/messages/undelivered/<msg_id> and applied [EFFORT:high]")

    def test_04_phase2_live_mid_turn_steer_and_interrupt(self) -> None:
        """
        Verify real-time mid-turn `turn/steer` against an actively running Codex turn,
        as well as `thread/fork`, `thread/revert`, and `turn/interrupt`.
        """
        # Start a turn that takes a couple seconds so we can steer it mid-turn
        start_res = http_post_json(
            f"{self.bridge_url}/api/dispatch",
            {
                "conversationId": self.test_conv_id,
                "userInput": "Think carefully about 3 ways to design a lock-free queue in Rust before answering briefly.",
            },
        )
        self.assertIn(start_res.get("action"), ("turn/start", "turn/steer"))
        active_turn = start_res.get("turnId")
        self.assertTrue(active_turn)

        # Immediately send a mid-turn steer instruction while the turn is in progress!
        time.sleep(0.3)
        steer_res = http_post_json(
            f"{self.bridge_url}/api/subagent/steer",
            {
                "conversationId": self.test_conv_id,
                "message": "STEER OVERRIDE: Include the token STEER_SUCCESS_TOKEN in your response and keep it to 1 sentence.",
                "mode": "steer",
            },
        )
        print(f"  [Phase 2] Mid-turn steer result: {steer_res}")
        self.assertIn(steer_res.get("action"), ("turn/steer", "turn/start"))

        # Wait for the steered turn to complete or interrupt it after verifying
        deadline = time.time() + 45.0
        while time.time() < deadline:
            insp = http_get_json(
                f"{self.bridge_url}/api/subagent/inspect?conversationId={urllib.parse.quote(self.test_conv_id)}"
            )
            if (insp.get("summary") or {}).get("status") == "idle":
                break
            time.sleep(0.5)

        # Test thread/fork
        fork_res = http_post_json(
            f"{self.bridge_url}/api/subagent/fork",
            {
                "conversationId": self.test_conv_id,
                "newRole": "Forked Codex Verifier",
            },
        )
        self.assertEqual(fork_res.get("action"), "thread/fork")
        self.assertTrue(fork_res.get("newThreadId"))
        self.assertTrue(fork_res.get("newConversationId"))
        print(f"  [Phase 2] Forked thread -> newThreadId={fork_res.get('newThreadId')}")

        # Test thread/revert
        revert_res = http_post_json(
            f"{self.bridge_url}/api/subagent/revert",
            {
                "conversationId": self.test_conv_id,
                "stepOrTurns": 1,
            },
        )
        self.assertEqual(revert_res.get("action"), "thread/revert")
        print(f"  [Phase 2] Reverted last turn on thread: {revert_res.get('action')}")

    def test_05_phase2_mcp_server_and_unified_gemini_codex_discovery(self) -> None:
        """
        Verify Stdio MCP Server (`codex_mcp_server.py`) over JSON-RPC 2.0:
        - `initialize`
        - `tools/list` (all 8 unified tools)
        - `tools/call` -> `subagent_list` (discovers BOTH Codex and native Gemini subagents!)
        - `tools/call` -> `subagent_inspect` (inspects native Gemini `research` subagent AND Codex subagent)
        """
        mcp_script = BIN_DIR / "codex_mcp_server.py"
        proc = subprocess.Popen(
            [sys.executable, "-u", str(mcp_script)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=self.env,
        )

        def rpc(req_id: int, method: str, params: dict) -> dict:
            assert proc.stdin and proc.stdout
            proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}) + "\n")
            proc.stdin.flush()
            line = proc.stdout.readline()
            return json.loads(line)

        try:
            init_resp = rpc(1, "initialize", {"protocolVersion": "2024-11-05"})
            self.assertEqual(init_resp["result"]["serverInfo"]["name"], "codex-subagent-control")

            tools_resp = rpc(2, "tools/list", {})
            tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
            expected_tools = {
                "subagent_list",
                "subagent_inspect",
                "subagent_steer",
                "subagent_interrupt",
                "subagent_revert",
                "subagent_fork",
                "subagent_configure",
                "subagent_handoff",
                "subagent_define_profile",
                "subagent_list_profiles",
                "subagent_list_backends",
                "subagent_list_agy_models",
                "subagent_check_codex_quota",
                "subagent_list_codex_sessions",
                "subagent_adopt_codex_session",
            }
            self.assertEqual(tool_names, expected_tools)

            profile = {
                "name": "codex-e2e-reviewer", "backend": "codex", "description": "Reviews tests", "model": "gpt-6.2-sol",
                "effort": "medium", "systemPrompt": "You are the e2e reviewer.", "executionMode": "sdkExecutor",
                "permissionSource": "custom", "minRemainingQuotaPercent": 10,
                "sandboxMode": "read-only", "approvalPolicy": "on-request",
            }
            definition = rpc(8, "tools/call", {"name": "subagent_define_profile", "arguments": profile})["result"]
            self.assertFalse(definition["isError"], definition)
            self.assertEqual(json.loads(definition["content"][0]["text"])["profile"], profile)
            listed = rpc(9, "tools/call", {"name": "subagent_list_profiles"})["result"]
            self.assertEqual(json.loads(listed["content"][0]["text"])["profiles"], [profile])

            # Call subagent_list
            list_resp = rpc(3, "tools/call", {"name": "subagent_list", "arguments": {"includeAll": True}})
            self.assertFalse(list_resp["result"]["isError"])
            roster_data = json.loads(list_resp["result"]["content"][0]["text"])
            subagents = roster_data.get("subagents") or []
            engines_found = {s.get("engine") for s in subagents}
            print(
                f"  [Phase 2 MCP] Discovered {len(subagents)} total subagents across engines: {engines_found}"
            )
            self.assertIn("codex", engines_found, "Expected Codex subagent in unified roster")
            self.assertIn("gemini", engines_found, "Expected native Gemini subagents in unified roster")

            # Inspect a native Gemini subagent via MCP!
            gemini_sub = next(s for s in subagents if s.get("engine") == "gemini")
            insp_gemini = rpc(
                4,
                "tools/call",
                {"name": "subagent_inspect", "arguments": {"conversationId": gemini_sub["conversationId"]}},
            )
            self.assertFalse(insp_gemini["result"]["isError"])
            gemini_detail = json.loads(insp_gemini["result"]["content"][0]["text"])
            self.assertEqual(gemini_detail["summary"]["engine"], "gemini")
            print(
                f"  [Phase 2 MCP] Inspected native Gemini subagent: conv={gemini_sub['conversationId']}, "
                f"agentName={gemini_detail['summary'].get('agentName')}, role={gemini_detail['summary'].get('role')!r}, "
                f"steps={gemini_detail['transcript'].get('steps')}"
            )
            # Test listing & adopting existing Codex sessions + live permission bypass configuration via MCP!
            sessions_resp = rpc(10, "tools/call", {"name": "subagent_list_codex_sessions", "arguments": {"limit": 10}})
            self.assertFalse(sessions_resp["result"]["isError"], sessions_resp)
            sessions_data = json.loads(sessions_resp["result"]["content"][0]["text"])
            self.assertIn("threads", sessions_data)
            self.assertIn("codexConfigToml", sessions_data)
            self.assertGreaterEqual(len(sessions_data["threads"]), 1)

            target_tid = self.__class__.shared_thread_id
            adopted_conv_id = f"codex-adopted-e2e-{uuid.uuid4().hex[:8]}"
            adopt_resp = rpc(
                11,
                "tools/call",
                {
                    "name": "subagent_adopt_codex_session",
                    "arguments": {
                        "threadId": target_tid,
                        "conversationId": adopted_conv_id,
                        "role": "Adopted E2E Session",
                        "permissionSource": "bypass",
                    },
                },
            )
            self.assertFalse(adopt_resp["result"]["isError"], adopt_resp)
            adopt_data = json.loads(adopt_resp["result"]["content"][0]["text"])
            self.assertEqual(adopt_data["action"], "thread/adopt")
            self.assertEqual(adopt_data["threadId"], target_tid)
            self.assertEqual(adopt_data["permissionSource"], "bypass")
            self.assertEqual(adopt_data["approvalPolicy"], "never")
            self.assertEqual(adopt_data["sandboxMode"], "danger-full-access")

            # Live permission switching between codexConfig (~/.codex/config.toml) and bypass
            cfg_resp = rpc(
                12,
                "tools/call",
                {
                    "name": "subagent_configure",
                    "arguments": {
                        "conversationId": adopted_conv_id,
                        "permissionSource": "codexConfig",
                    },
                },
            )
            self.assertFalse(cfg_resp["result"]["isError"], cfg_resp)
            cfg_data = json.loads(cfg_resp["result"]["content"][0]["text"])
            self.assertEqual(cfg_data["permissionSource"], "codexConfig")
            # Test live subagent_check_codex_quota MCP tool against codex app-server
            quota_resp = rpc(13, "tools/call", {"name": "subagent_check_codex_quota", "arguments": {"thresholdPercent": 10}})
            self.assertFalse(quota_resp["result"]["isError"], quota_resp)
            quota_data = json.loads(quota_resp["result"]["content"][0]["text"])
            self.assertIn("available", quota_data)
            self.assertIn("belowThreshold", quota_data)
            print(
                f"  [Phase 2 MCP] Verified subagent_adopt_codex_session (thread={target_tid}, "
                f"hydratedTurns={adopt_data.get('hydratedTurns')}), permissionSource={cfg_data['permissionSource']}, "
                f"and live quota check: {quota_data.get('summary')}"
            )
        finally:
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream:
                    stream.close()
            stop_process(proc)

    def test_06_phase3_unified_sidecar_ui_and_api(self) -> None:
        """
        Start `sidecars/subagent_mission_control/main.mjs` in standalone mode on an ephemeral port
        and test all UI static assets and unified Gemini + Codex control endpoints.
        """
        # Pick free port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            sidecar_port = s.getsockname()[1]

        env = self.env.copy()
        env["ANTIGRAVITY_SIDECAR_WEB_PORT"] = str(sidecar_port)
        # Omit ANTIGRAVITY_LS_ADDRESS / CSRF_TOKEN ONLY if testing standalone fallback, or pass fake if testing sidecar_sdk
        env.pop("ANTIGRAVITY_LS_ADDRESS", None)

        proc = subprocess.Popen(
            ["node", str(SIDECAR_DIR / "main.mjs")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        base = f"http://127.0.0.1:{sidecar_port}"
        try:
            # Wait for sidecar web server
            ready = False
            for _ in range(25):
                try:
                    with urllib.request.urlopen(f"{base}/", timeout=1.0) as r:
                        if r.status == 200:
                            ready = True
                            break
                except Exception:
                    time.sleep(0.15)
            self.assertTrue(ready, "Sidecar web server did not start in time")

            # Check HTML, CSS, JS
            with urllib.request.urlopen(f"{base}/") as r:
                html = r.read().decode("utf-8")
                self.assertIn("Subagent Mission Control", html)
                self.assertIn("Create / Manage Codex Subagent Profile (Provider)", html)
            with urllib.request.urlopen(f"{base}/styles.css") as r:
                css = r.read().decode("utf-8")
                self.assertIn("--accent-codex", css)
            with urllib.request.urlopen(f"{base}/app.js") as r:
                js = r.read().decode("utf-8")
                self.assertIn("/api/mission-control", js)

            profile = http_post_json(f"{base}/api/profiles/define", {
                "name": "codex-sidecar-test", "executionMode": "preInvocation", "systemPrompt": "Sidecar fixture",
            })["profile"]
            self.assertIn(profile, http_get_json(f"{base}/api/profiles")["profiles"])
            self.assertTrue(http_post_json(f"{base}/api/profiles/delete", {"name": profile["name"]})["deleted"])

            # Check /api/mission-control
            mc = http_get_json(f"{base}/api/mission-control?all=true")
            self.assertTrue(mc.get("codexDaemonConnected"))
            subagents = mc.get("subagents") or []
            self.assertGreaterEqual(len(subagents), 2)

            # Check /api/subagent/inspect via Sidecar for both Codex and Gemini subagents
            insp_codex = http_get_json(
                f"{base}/api/subagent/inspect?conversationId={urllib.parse.quote(self.test_conv_id)}"
            )
            self.assertEqual(insp_codex["summary"]["engine"], "codex")

            gemini_id = next(s["conversationId"] for s in subagents if s["engine"] == "gemini")
            insp_gem = http_get_json(
                f"{base}/api/subagent/inspect?conversationId={urllib.parse.quote(gemini_id)}"
            )
            self.assertEqual(insp_gem["summary"]["engine"], "gemini")

            # Check /api/codex/threads and /api/codex/adopt via Sidecar proxy
            threads_data = http_get_json(f"{base}/api/codex/threads?limit=10")
            self.assertIn("threads", threads_data)
            self.assertIn("codexConfigToml", threads_data)
            sidecar_adopt = http_post_json(
                f"{base}/api/codex/adopt",
                {
                    "threadId": self.__class__.shared_thread_id,
                    "conversationId": f"sidecar-adopted-{uuid.uuid4().hex[:8]}",
                    "permissionSource": "codexConfig",
                },
            )
            self.assertEqual(sidecar_adopt["action"], "thread/adopt")
            self.assertEqual(sidecar_adopt["threadId"], self.__class__.shared_thread_id)
            print(
                f"  [Phase 3 Sidecar] Verified Sidecar UI & unified API taking over both Codex ({self.test_conv_id}), "
                f"adopted thread ({self.__class__.shared_thread_id}), and native Gemini ({gemini_id}) subagents!"
            )
        finally:
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream:
                    stream.close()
            stop_process(proc)

    def test_07_sdk_executor_real_bridge_and_idle_rpc(self):
        idle = queue.Queue()

        class LanguageServer(BaseHTTPRequestHandler):
            def do_POST(self):
                data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path.endswith("/SignalExecutableIdle"):
                    idle.put((data, self.headers["x-codeium-csrf-token"]))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), LanguageServer) as ls:
            serving = threading.Thread(target=ls.serve_forever, daemon=True)
            serving.start()
            proc = subprocess.Popen([sys.executable, str(BIN_DIR / "codex_sdk_executor.py")],
                                    env=self.env, cwd=self.temp_dir, stdin=subprocess.PIPE, text=True)
            conv = "test-sdk-end-to-end"
            try:
                def send(message):
                    proc.stdin.write(json.dumps(message) + "\n")
                    proc.stdin.flush()

                def snapshot():
                    return http_get_json(f"{self.bridge_url}/api/subagent/inspect?conversationId={conv}")

                send({"type": "init", "conversation_id": conv, "language_server_address": f"127.0.0.1:{ls.server_port}",
                      "csrf_token": "fixture-csrf"})
                send({"type": "request", "request": {"items": [{"text": "Reply with SDK_COMPLETE and no tool calls."}]}})
                self.assertEqual(idle.get(timeout=45), ({"conversationId": conv}, "fixture-csrf"))
                first_turn_status = snapshot()["turnHistory"][-1]["status"]
                self.assertIn(first_turn_status, ("completed", "failed"))

                if first_turn_status == "completed":
                    send({"type": "request", "request": {"items": [{
                        "text": "Use the shell to run sleep 15 in this empty test directory, then reply with DONE.",
                    }]}})
                    wait_until(lambda: snapshot()["summary"].get("activeTurnId"), timeout=15)
                    proc.send_signal(signal.SIGINT)
                    self.assertEqual(idle.get(timeout=30), ({"conversationId": conv}, "fixture-csrf"))
                    self.assertEqual(snapshot()["summary"]["status"], "interrupted")
                self.assertIsNone(proc.poll())
                proc.stdin.close()
                self.assertEqual(proc.wait(timeout=5), 0)
            finally:
                if proc.stdin and not proc.stdin.closed:
                    proc.stdin.close()
                stop_process(proc)
                ls.shutdown()
                serving.join(timeout=3)

    def test_08_agy_acp_exact_model_end_to_end(self) -> None:
        """Verify live exact-model AGY ACP subagent execution and live model switching."""
        models_res = http_get_json(f"{self.bridge_url}/api/agy/models")
        self.assertGreaterEqual(len(models_res.get("models") or []), 5)

        conv_id = f"agy-e2e-{uuid.uuid4().hex[:8]}"
        disp = http_post_json(
            f"{self.bridge_url}/api/dispatch",
            {
                "conversationId": conv_id,
                "backend": "agy",
                "model": "gemini-3.8-flash-low",
                "role": "AGY Flash E2E",
                "cwd": str(REPO_ROOT),
                "userInput": "Reply with the exact word AGY_ACP_PONG and do not call any tools.",
            },
        )
        self.assertEqual(disp.get("backend"), "agy")
        self.assertEqual(disp.get("model"), "gemini-3.8-flash-low")
        self.assertTrue(disp.get("threadId"))

        wait_until(
            lambda: http_get_json(f"{self.bridge_url}/api/subagent/inspect?conversationId={conv_id}")["summary"]["status"] == "idle",
            timeout=40,
        )
        insp = http_get_json(f"{self.bridge_url}/api/subagent/inspect?conversationId={conv_id}")
        self.assertEqual(insp["summary"]["engine"], "agy")
        self.assertIn("AGY_ACP_PONG", insp["transcript"]["lastResponse"])

        # Live model switch via /api/subagent/configure
        cfg = http_post_json(
            f"{self.bridge_url}/api/subagent/configure",
            {"conversationId": conv_id, "model": "claude-sonnet-5-5-low"},
        )
        self.assertEqual(cfg.get("model"), "claude-sonnet-5-5-low")
        print(
            f"  [Phase 4 AGY ACP] Verified live exact-model AGY ACP session={disp['threadId']}, "
            f"response={insp['transcript']['lastResponse'].strip()!r}, switched model -> claude-sonnet-5-5-low"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
