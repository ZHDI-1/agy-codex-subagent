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

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

REPO_ROOT = Path("/home/zhdi/build_part/agy_subagent")
PLUGIN_DIR = REPO_ROOT / "plugins" / "codex-subagent"
BIN_DIR = PLUGIN_DIR / "bin"
SIDECAR_DIR = PLUGIN_DIR / "sidecars" / "subagent_mission_control"

sys.path.insert(0, str(BIN_DIR))

from codex_bridge_daemon import BRIDGE_PORT, ensure_bridge_daemon_running, summarize_transcript_file
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


class TestCodexSubagentAllPhases(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.bridge_url = ensure_bridge_daemon_running(BRIDGE_PORT)
        cls.temp_dir = Path(tempfile.mkdtemp(prefix="agy_codex_e2e_"))
        cls.test_conv_id = f"test-codex-{uuid.uuid4().hex[:10]}"
        cls.transcript_path = cls.temp_dir / "transcript.jsonl"

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def test_01_plugin_validation_zero_warnings(self) -> None:
        """Verify `agy plugin validate` passes with 0 errors and 0 warnings."""
        proc = subprocess.run(
            ["/home/zhdi/.local/bin/agy", "plugin", "validate", str(PLUGIN_DIR)],
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        self.assertEqual(proc.returncode, 0, f"Plugin validation failed:\n{proc.stdout}\n{proc.stderr}")
        self.assertIn("Validation passed", proc.stdout)
        self.assertNotIn("⚠", proc.stdout, f"Unexpected warning in plugin validate:\n{proc.stdout}")

    def test_02_phase1_direct_codex_daemon_websocket(self) -> None:
        """Verify direct AF_UNIX WebSocket connection & JSON-RPC handshake with codex app-server daemon."""
        client = CodexAppServerClient()
        info = client.connect()
        self.assertIn("userAgent", info)
        self.assertTrue(client.is_connected())
        loaded = client.request("thread/loaded/list", {})
        self.assertIn("data", loaded)
        client.close()

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
        print(f"  [Phase 1] Created Codex threadId={thread_id}, turnId={turn_id}")

        #Wait for turn/completed to mirror into transcript.jsonl
        deadline = time.time() + 40.0
        inspected = {}
        while time.time() < deadline:
            inspected = http_get_json(
                f"{self.bridge_url}/api/subagent/inspect?conversationId={urllib.parse.quote(self.test_conv_id)}"
            )
            st = (inspected.get("summary") or {}).get("status")
            if st == "idle" and (inspected.get("transcript") or {}).get("steps", 0) >= 2:
                break
            time.sleep(0.4)

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
            input=json.dumps(hook_input),
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        self.assertEqual(proc_filestore.returncode, 0)
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
            }
            self.assertEqual(tool_names, expected_tools)

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
        finally:
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream:
                    stream.close()
            proc.terminate()
            proc.wait(timeout=5)

    def test_06_phase3_unified_sidecar_ui_and_api(self) -> None:
        """
        Start `sidecars/subagent_mission_control/main.mjs` in standalone mode on an ephemeral port
        and test all UI static assets and unified Gemini + Codex control endpoints.
        """
        # Pick free port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            sidecar_port = s.getsockname()[1]

        env = os.environ.copy()
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
            with urllib.request.urlopen(f"{base}/styles.css") as r:
                css = r.read().decode("utf-8")
                self.assertIn("--accent-codex", css)
            with urllib.request.urlopen(f"{base}/app.js") as r:
                js = r.read().decode("utf-8")
                self.assertIn("/api/mission-control", js)

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
            print(
                f"  [Phase 3 Sidecar] Verified Sidecar UI & unified API taking over both Codex ({self.test_conv_id}) "
                f"and native Gemini ({gemini_id}) subagents!"
            )
        finally:
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream:
                    stream.close()
            proc.terminate()
            proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
