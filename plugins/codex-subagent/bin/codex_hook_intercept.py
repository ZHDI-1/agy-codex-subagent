#!/usr/bin/env python3
"""
Antigravity PreInvocation Lifecycle Hook for `codex-subagent`.

When `invoke_subagent(TypeName="codex", ...)` or `send_message(Recipient=<codex_subagent_id>, ...)`
triggers a turn in the `codex` subagent conversation:
1. Fast-checks whether the conversation is a `codex` subagent via:
   - `payload.get("agentName") == "codex"` (injected directly by Antigravity `HookArgsCommon`)
   - Bridge state file (`codex_subagent_state.json`)
   - SQLite `conversation_summaries.db`
2. If it is NOT a `codex` subagent (e.g., main conversation or Gemini `self`/`research` subagent),
   outputs `{}` immediately in < 2ms so native Gemini execution proceeds untouched.
3. If it IS a `codex` subagent:
   - Drains undelivered messages directly from Antigravity's `messages.FileStore`:
     `<appDataDir>/brain/<conversationId>/.system_generated/messages/undelivered/<msg_id>`
     and `<appDataDir>/brain/<conversationId>/.system_generated/messages/<msg_id>.json`,
     extracting the exact `content`, `sender` (`parentConversationId`), and `Role` (from
     `sourceMetadata.tool.toolCall.argumentsJson`), and removes the `undelivered/<msg_id>`
     marker file so `messages.FileStore` marks the message delivered!
   - Falls back to `lastUserInput` or `transcript.jsonl` if no undelivered message file exists.
   - Dispatches to `codex_bridge_daemon.py` (`/api/dispatch`), which executes `turn/start` (if idle)
     or `turn/steer` (if a turn is actively running on the Codex App-Server Daemon!).
   - Calls `LanguageServerService/CancelCascadeInvocation` (`notifyParent: False`) and outputs
     `{"terminationBehavior": "terminate"}` so Antigravity's `ordinaryExecutor` stops immediately
     before making any Gemini LLM call (0 Gemini tokens consumed!).
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from codex_bridge_daemon import BRIDGE_HOST, BRIDGE_PORT, ensure_bridge_daemon_running, find_app_data_dirs


SYSTEM_MSG_RE = re.compile(
    r"\[Message\]\s+timestamp=\S+\s+sender=([0-9a-fA-F-]{8,})\s+priority=\S+\s+content=(.*?)\s*(?:</SYSTEM_MESSAGE>|$)",
    re.DOTALL,
)


def hydrate_ls_env_from_state() -> Tuple[str, str, str]:
    ls_addr = os.environ.get("ANTIGRAVITY_LS_ADDRESS", "")
    csrf = os.environ.get("ANTIGRAVITY_CSRF_TOKEN", "")
    exe = os.environ.get("ANTIGRAVITY_AGENTAPI_EXE") or "/home/zhdi/.local/bin/agy"

    if not ls_addr or not csrf:
        for app_dir in find_app_data_dirs():
            state_path = app_dir / "codex_subagent_state.json"
            if state_path.exists():
                try:
                    st = json.loads(state_path.read_text(encoding="utf-8"))
                    if not ls_addr and st.get("lsAddress"):
                        ls_addr = str(st["lsAddress"])
                        os.environ["ANTIGRAVITY_LS_ADDRESS"] = ls_addr
                    if not csrf and st.get("csrfToken"):
                        csrf = str(st["csrfToken"])
                        os.environ["ANTIGRAVITY_CSRF_TOKEN"] = csrf
                    if ls_addr and csrf:
                        break
                except Exception:
                    pass
    return (ls_addr, csrf, exe)


def normalize_compact_transcript_path(raw_path: str, conv_id: str) -> str:
    if raw_path:
        p = Path(raw_path)
        if p.name == "transcript_full.jsonl":
            return str(p.parent / "transcript.jsonl")
        return str(p)
    app_dirs = find_app_data_dirs()
    return str(app_dirs[0] / "brain" / conv_id / ".system_generated" / "logs" / "transcript.jsonl")


def drain_undelivered_filestore_messages(conv_id: str, raw_transcript_path: str = "") -> Tuple[str, str, str]:
    """
    Drains undelivered messages from Antigravity's `messages.FileStore`:
      <appDataDir>/brain/<conv_id>/.system_generated/messages/undelivered/<msg_id>
      <appDataDir>/brain/<conv_id>/.system_generated/messages/<msg_id>.json
    Returns (combined_content, sender_parent_id, role).
    """
    if not conv_id:
        return ("", "", "")

    records: List[Tuple[str, str, str, str, Path]] = []  # (timestamp, content, sender, role, marker_path)
    msg_dirs: List[Path] = []
    if raw_transcript_path:
        tp = Path(raw_transcript_path)
        for cand in (tp.parent.parent / "messages", tp.parent / "messages"):
            if cand not in msg_dirs:
                msg_dirs.append(cand)
    for app_dir in find_app_data_dirs():
        cand = app_dir / "brain" / conv_id / ".system_generated" / "messages"
        if cand not in msg_dirs:
            msg_dirs.append(cand)

    for msg_dir in msg_dirs:
        undeliv_dir = msg_dir / "undelivered"
        if not undeliv_dir.is_dir():
            continue
        for marker in undeliv_dir.iterdir():
            if not marker.is_file():
                continue
            msg_json_path = msg_dir / f"{marker.name}.json"
            if not msg_json_path.is_file():
                continue
            try:
                msg_obj = json.loads(msg_json_path.read_text(encoding="utf-8", errors="replace"))
                ts = str(msg_obj.get("timestamp") or "")
                content = str(msg_obj.get("content") or "").strip()
                sender = str(msg_obj.get("sender") or "").strip()
                role = ""
                tool_call = ((msg_obj.get("sourceMetadata") or {}).get("tool") or {}).get("toolCall") or {}
                if tool_call.get("name") == "invoke_subagent" and tool_call.get("argumentsJson"):
                    try:
                        t_args = json.loads(tool_call["argumentsJson"])
                        role = str(t_args.get("Role") or "")
                        if not content and t_args.get("Prompt"):
                            content = str(t_args["Prompt"]).strip()
                    except Exception:
                        pass
                records.append((ts, content, sender, role, marker))
            except Exception:
                continue

    if not records:
        return ("", "", "")

    records.sort(key=lambda r: r[0])
    contents: List[str] = []
    sender_id = ""
    role_found = ""
    for _, content, sender, role, marker in records:
        if content:
            contents.append(content)
        if sender and not sender_id:
            sender_id = sender
        if role and not role_found:
            role_found = role
        try:
            marker.unlink(missing_ok=True)
        except Exception:
            pass

    return ("\n\n".join(contents), sender_id, role_found)


def extract_latest_input_from_transcripts(raw_transcript_path: str, conv_id: str) -> Tuple[str, str]:
    candidates: List[Path] = []
    if raw_transcript_path:
        p = Path(raw_transcript_path)
        candidates.append(p.parent / "transcript.jsonl")
        if p not in candidates:
            candidates.append(p)
    for app_dir in find_app_data_dirs():
        log_dir = app_dir / "brain" / conv_id / ".system_generated" / "logs"
        for fname in ("transcript.jsonl", "transcript_full.jsonl"):
            fp = log_dir / fname
            if fp not in candidates:
                candidates.append(fp)

    for fp in candidates:
        if not fp.exists():
            continue
        try:
            lines = [ln for ln in fp.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip()]
            for line in reversed(lines):
                try:
                    step = json.loads(line)
                except Exception:
                    continue
                s_type = step.get("type", "")
                content = str(step.get("content") or "").strip()
                if s_type == "USER_INPUT" and content:
                    return (content, "")
                if s_type == "SYSTEM_MESSAGE" and content:
                    m = SYSTEM_MSG_RE.search(content)
                    if m:
                        msg_body = m.group(2).strip()
                        sender_id = m.group(1).strip()
                        if msg_body:
                            return (msg_body, sender_id)
                if s_type == "PLANNER_RESPONSE":
                    break
        except Exception:
            continue
    return ("", "")


def check_is_codex_subagent(payload: Dict[str, Any], conversation_id: str, exe: str) -> Tuple[bool, str, str, str]:
    """
    Returns (is_codex, parent_conversation_id, role, initial_prompt).
    """
    if not conversation_id:
        return (False, "", "", "")

    # 0. Direct check from HookArgsCommon payload fields (`agentName`, `parentConversationId`)
    payload_agent = str(payload.get("agentName") or "").strip()
    payload_parent = str(payload.get("parentConversationId") or "").strip()
    if payload_agent == "codex":
        return (True, payload_parent, "", "")
    if payload_agent in ("research", "self", "image-generator"):
        return (False, payload_parent, "", "")

    app_dirs = find_app_data_dirs()

    # 1. Check Bridge state file
    for app_dir in app_dirs:
        state_path = app_dir / "codex_subagent_state.json"
        if state_path.exists():
            try:
                st = json.loads(state_path.read_text(encoding="utf-8"))
                sess = (st.get("sessions") or {}).get(conversation_id)
                if isinstance(sess, dict):
                    return (
                        True,
                        str(sess.get("parentConversationId") or payload_parent),
                        str(sess.get("role") or "Codex Subagent"),
                        "",
                    )
            except Exception:
                pass

    # 2. Check SQLite conversation_summaries.db
    for app_dir in app_dirs:
        db_path = app_dir / "conversation_summaries.db"
        if not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
            row = conn.execute(
                "SELECT agent_name, parent_conversation_id, title, raw_summary "
                "FROM conversation_summaries WHERE conversation_id = ?",
                (conversation_id,),
            ).fetchone()
            conn.close()
            if row is not None:
                agent_name = str(row[0] or "")
                parent_id = str(row[1] or payload_parent)
                title = str(row[2] or "")
                raw = row[3]
                if agent_name == "codex":
                    role = title
                    if isinstance(raw, (bytes, bytearray)):
                        strings = [s.decode("utf-8", "ignore") for s in re.findall(rb"[ -~]{3,80}", raw)]
                        if "codex" in strings:
                            idx = len(strings) - 1 - strings[::-1].index("codex")
                            if idx + 1 < len(strings):
                                cand = strings[idx + 1].rstrip("0123456789 ")
                                if cand and not cand.startswith("file://"):
                                    role = cand
                    return (True, parent_id, role or "Codex Subagent", "")
                if agent_name in ("research", "self", "image-generator") or (not parent_id and not agent_name):
                    return (False, parent_id, title, "")
        except Exception:
            pass

    # 3. Check undelivered messages in FileStore for invoke_subagent(TypeName="codex")
    for app_dir in app_dirs:
        msg_dir = app_dir / "brain" / conversation_id / ".system_generated" / "messages"
        if msg_dir.is_dir():
            for jf in sorted(msg_dir.glob("*.json")):
                try:
                    mo = json.loads(jf.read_text(encoding="utf-8", errors="replace"))
                    tc = ((mo.get("sourceMetadata") or {}).get("tool") or {}).get("toolCall") or {}
                    if tc.get("name") == "invoke_subagent" and tc.get("argumentsJson"):
                        t_args = json.loads(tc["argumentsJson"])
                        if t_args.get("TypeName") == "codex":
                            return (
                                True,
                                str(mo.get("sender") or payload_parent),
                                str(t_args.get("Role") or "Codex Subagent"),
                                str(t_args.get("Prompt") or ""),
                            )
                except Exception:
                    pass

    return (False, payload_parent, "", "")


def post_bridge_json(base_url: str, path: str, payload: Dict[str, Any], timeout: float = 15.0) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw) if raw.strip() else {}


def main() -> None:
    try:
        raw_stdin = sys.stdin.read()
        payload = json.loads(raw_stdin) if raw_stdin.strip() else {}
    except Exception:
        print("{}")
        return

    event_name = str(payload.get("hookEventName") or "PreInvocation")
    if event_name != "PreInvocation":
        print("{}")
        return

    conv_id = str(payload.get("conversationId") or os.environ.get("ANTIGRAVITY_CONVERSATION_ID") or "")
    ls_address, csrf_token, agentapi_exe = hydrate_ls_env_from_state()

    is_codex, parent_id, role, initial_prompt = check_is_codex_subagent(payload, conv_id, agentapi_exe)

    if not is_codex:
        if ls_address and csrf_token:
            try:
                post_bridge_json(
                    f"http://{BRIDGE_HOST}:{BRIDGE_PORT}",
                    "/api/ls-credentials",
                    {
                        "lsAddress": ls_address,
                        "csrfToken": csrf_token,
                        "agentapiExe": agentapi_exe,
                    },
                    timeout=0.3,
                )
            except Exception:
                pass
        print("{}")
        return

    try:
        base_url = ensure_bridge_daemon_running(BRIDGE_PORT)
        workspace_paths = payload.get("workspacePaths") or []
        cwd = payload.get("cwd") or (workspace_paths[0] if workspace_paths else os.getcwd())
        raw_transcript_path = payload.get("transcriptPath") or ""
        compact_transcript_path = normalize_compact_transcript_path(raw_transcript_path, conv_id)

        # 1. Primary input source: drain undelivered messages from `.system_generated/messages/undelivered/`
        fs_content, fs_sender, fs_role = drain_undelivered_filestore_messages(conv_id, raw_transcript_path)
        if fs_sender and not parent_id:
            parent_id = fs_sender
        if fs_role and (not role or role == "Codex Subagent"):
            role = fs_role

        last_user_input = fs_content or (payload.get("lastUserInput") or "").strip()
        if not last_user_input:
            tr_input, tr_sender = extract_latest_input_from_transcripts(raw_transcript_path, conv_id)
            if tr_input:
                last_user_input = tr_input
            if tr_sender and not parent_id:
                parent_id = tr_sender
        if not last_user_input and initial_prompt:
            last_user_input = initial_prompt

        dispatch_res = post_bridge_json(
            base_url,
            "/api/dispatch",
            {
                "conversationId": conv_id,
                "parentConversationId": parent_id,
                "role": role or "Codex Subagent",
                "cwd": cwd,
                "transcriptPath": compact_transcript_path,
                "userInput": last_user_input,
                "lsAddress": ls_address,
                "csrfToken": csrf_token,
                "agentapiExe": agentapi_exe,
            },
            timeout=20.0,
        )

        action = dispatch_res.get("action", "turn/start")
        thread_id = dispatch_res.get("threadId", "unknown")
        turn_id = dispatch_res.get("turnId", "unknown")

        # Cancel the Gemini ordinaryExecutor invocation context for this child conversation
        # (with notifyParent=False) so ordinaryExecutor exits immediately without calling Gemini LLM!
        if ls_address:
            try:
                cancel_req = urllib.request.Request(
                    f"http://{ls_address}/exa.language_server_pb.LanguageServerService/CancelCascadeInvocation",
                    data=json.dumps(
                        {
                            "cascadeId": conv_id,
                            "killBackgroundTasks": False,
                            "notifyParent": False,
                        }
                    ).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        **({"x-codeium-csrf-token": csrf_token} if csrf_token else {}),
                    },
                    method="POST",
                )
                urllib.request.urlopen(cancel_req, timeout=2.0).read()
            except Exception:
                pass

        hook_output = {
            "injectSteps": [
                {
                    "systemMessage": {
                        "systemMessage": "",
                        "metadata": {
                            "user_markdown": (
                                f"⚡ **Codex App-Server Daemon** accepted `{action}`\n"
                                f"- **Role**: `{role or 'Codex Subagent'}`\n"
                                f"- **Codex Thread**: `{thread_id}`\n"
                                f"- **Active Turn**: `{turn_id}`"
                            )
                        },
                    }
                }
            ],
            "terminationBehavior": "terminate",
        }
        print(json.dumps(hook_output))
    except Exception as e:
        err_output = {
            "injectSteps": [
                {
                    "errorMessage": {
                        "userMessage": f"Codex Subagent Bridge error: {e}",
                    }
                }
            ],
            "terminationBehavior": "terminate",
        }
        print(json.dumps(err_output))


if __name__ == "__main__":
    main()
