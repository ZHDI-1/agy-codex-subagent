#!/usr/bin/env python3
"""Route Codex-backed Markdown agents from Antigravity's PreInvocation hook."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import urllib.request
from contextlib import closing
from pathlib import Path
from typing import Any

from codex_bridge_daemon import (
    BRIDGE_HOST,
    BRIDGE_PORT,
    bridge_auth_headers,
    ensure_bridge_daemon_running,
    find_app_data_dirs,
)
from codex_profiles import active_sdk_definition, live_sdk_instance, load_config, load_profiles, read_object

SYSTEM_MSG_RE = re.compile(
    r"\[Message\]\s+timestamp=\S+\s+sender=([0-9a-fA-F-]{8,})\s+priority=\S+\s+content=(.*?)\s*(?:</SYSTEM_MESSAGE>|$)",
    re.DOTALL,
)


def normalize_transcript_path(raw_path: str, conversation: str) -> str:
    if raw_path:
        path = Path(raw_path)
        return str(path.with_name("transcript.jsonl") if path.name == "transcript_full.jsonl" else path)
    return str(find_app_data_dirs()[0] / "brain" / conversation / ".system_generated/logs/transcript.jsonl")


def message_dirs(conversation: str, transcript: str = "") -> list[Path]:
    dirs = [app / "brain" / conversation / ".system_generated/messages" for app in find_app_data_dirs()]
    if transcript:
        path = Path(transcript)
        dirs = [path.parent.parent / "messages", path.parent / "messages", *dirs]
    return list(dict.fromkeys(dirs))


def sender_id(message: dict[str, Any]) -> str:
    sender = message.get("sender")
    return str((sender.get("conversationId") if isinstance(sender, dict) else sender) or "")


def invoke_arguments(msg: dict[str, Any]) -> dict[str, Any]:
    metadata = (msg.get("sourceMetadata") or {}).get("tool") or {}
    tool = metadata.get("toolCall") or {}
    if tool.get("name") == "invoke_subagent":
        args = json.loads(tool.get("argumentsJson") or "{}")
        return args if isinstance(args, dict) else {}
    return {}


def pending_messages(conversation: str, transcript: str = "") -> tuple[str, str, str, list[Path]]:
    pending = []
    for directory in message_dirs(conversation, transcript):
        for marker in (directory / "undelivered").glob("*"):
            try:
                if not marker.is_file():
                    continue
                msg = read_object(directory / f"{marker.name}.json")
                args = invoke_arguments(msg)
                content = str(msg.get("content") or args.get("Prompt") or "").strip()
                if content:
                    pending.append((str(msg.get("timestamp") or msg.get("createdAt") or ""), marker, content,
                                    sender_id(msg), str(args.get("Role") or "")))
            except (OSError, ValueError, TypeError, AttributeError):
                continue
    pending.sort()
    return (
        "\n\n".join(item[2] for item in pending),
        next((item[3] for item in pending if item[3]), ""),
        next((item[4] for item in pending if item[4]), ""),
        [item[1] for item in pending],
    )


def acknowledge_messages(markers: list[Path]) -> None:
    for marker in markers:
        try:
            marker.unlink(missing_ok=True)
        except OSError as e:
            print(f"codex subagent: could not acknowledge FileStore message {marker.name}: {e}", file=sys.stderr)


def input_id(sources: list[Path | str]) -> str:
    return hashlib.sha256("\0".join(str(source) for source in sources).encode()).hexdigest()


def transcript_prompt(transcript: str, conversation: str) -> tuple[str, str, str]:
    candidates = [Path(normalize_transcript_path(transcript, conversation))]
    if transcript:
        candidates.append(Path(transcript))
    for app in find_app_data_dirs():
        folder = app / "brain" / conversation / ".system_generated/logs"
        candidates.extend((folder / "transcript.jsonl", folder / "transcript_full.jsonl"))
    for path in dict.fromkeys(candidates):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in reversed(lines):
            try:
                step = json.loads(line)
            except ValueError:
                continue
            content = str(step.get("content") or "").strip()
            source = input_id([path, line])
            if step.get("type") == "USER_INPUT" and content and step.get("source") != "USER_EXPLICIT":
                return content, "", source
            if step.get("type") == "SYSTEM_MESSAGE" and (match := SYSTEM_MSG_RE.search(content)):
                return match[2].strip(), match[1], source
            if step.get("type") == "PLANNER_RESPONSE":
                break
    return "", "", ""


def get_route(payload: dict[str, Any], conversation: str) -> tuple[str, str, str, str, dict, dict]:
    """Resolve the agent, parent, role, initial prompt and persisted state."""
    name = payload.get("agentName") or ""
    parent = payload.get("parentConversationId") or ""
    if name in ("self", "research", "image-generator"):
        return "", parent, "", "", {}, {}
    role = payload.get("role") or ""
    profiles = load_profiles()
    saved = {}
    session = {}
    for app in find_app_data_dirs():
        try:
            saved = read_object(app / "codex_subagent_state.json")
            profiles.update(saved.get("profiles", {}))
            session = saved.get("sessions", {}).get(conversation, {})
            if session:
                break
        except (OSError, ValueError):
            continue
    if session:
        return (session.get("agentName") or "codex", parent or session.get("parentConversationId") or "",
                role or session.get("role") or "", "", session, saved)
    names = {"codex", *profiles}
    if name:
        return (name if name in names else "", parent, role, "", session, saved)

    for app in find_app_data_dirs():
        db = app / "conversation_summaries.db"
        if not db.is_file():
            continue
        try:
            with closing(sqlite3.connect(f"{db.as_uri()}?mode=ro", uri=True, timeout=0.1)) as conn:
                row = conn.execute(
                    "SELECT agent_name, parent_conversation_id, title FROM conversation_summaries "
                    "WHERE conversation_id = ?", (conversation,),
                ).fetchone()
        except sqlite3.Error:
            continue
        if row and row[0]:
            return (row[0] if row[0] in names else "", parent or row[1], role or row[2], "", session, saved)

    for directory in message_dirs(conversation, payload.get("transcriptPath") or ""):
        for path in sorted(directory.glob("*.json")):
            try:
                msg = read_object(path)
                args = invoke_arguments(msg)
                if args.get("TypeName") in names:
                    return (args["TypeName"], parent or sender_id(msg), role or args.get("Role") or "",
                            args.get("Prompt") or "", session, saved)
            except (OSError, ValueError, AttributeError, TypeError):
                continue
    return "", parent, "", "", session, saved


def post_bridge_json(base_url: str, path: str, payload: dict[str, Any], timeout: float = 15.0) -> dict[str, Any]:
    req = urllib.request.Request(
        f"{base_url}{path}", data=json.dumps(payload).encode(),
        headers=bridge_auth_headers({"Content-Type": "application/json"}), method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read() or b"{}")


def language_server_post(address: str, token: str, method: str, payload: dict, timeout: float = 2.0) -> None:
    base = address if address.startswith(("http://", "https://")) else f"http://{address}"
    request = urllib.request.Request(
        f"{base.rstrip('/')}/exa.language_server_pb.LanguageServerService/{method}",
        data=json.dumps(payload).encode(), headers={"Content-Type": "application/json", "x-codeium-csrf-token": token},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()


def main() -> None:
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict) or payload.get("hookEventName", "PreInvocation") != "PreInvocation":
            print("{}")
            return
    except (ValueError, TypeError):
        print("{}")
        return

    conversation = payload.get("conversationId") or os.environ.get("ANTIGRAVITY_CONVERSATION_ID", "")
    if not isinstance(conversation, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", conversation):
        print("{}")
        return
    name, parent, role, initial, session, saved = get_route(payload, conversation)
    address = os.environ.get("ANTIGRAVITY_LS_ADDRESS", "")
    csrf = os.environ.get("ANTIGRAVITY_CSRF_TOKEN", "")
    exe = os.environ.get("ANTIGRAVITY_AGENTAPI_EXE") or shutil.which("agy") or "agy"
    if not name:
        if address:
            try:
                post_bridge_json(f"http://{BRIDGE_HOST}:{BRIDGE_PORT}", "/api/ls-credentials",
                                 {"lsAddress": address, "csrfToken": csrf, "agentapiExe": exe}, timeout=0.3)
            except OSError:
                pass
        print("{}")
        return

    cwd = payload.get("cwd") or next(iter(payload.get("workspacePaths") or []), os.getcwd())
    if live_sdk_instance(session):
        # Only pass through when a live sdkExecutor process is actually registered for THIS conversation.
        # Never pass through solely because a .json file exists on disk: if Antigravity loaded a .md
        # fallback (e.g., plugin agents/codex.md in another workspace) and fired PreInvocation, we MUST
        # intercept here so Gemini flash_lite never runs unintercepted!
        print("{}")
        return

    try:
        if not address:
            source = session if session.get("lsAddress") else saved
            address, csrf = source.get("lsAddress", ""), source.get("csrfToken", "")
        base = ensure_bridge_daemon_running(BRIDGE_PORT)
        transcript = payload.get("transcriptPath") or ""
        text, sender, message_role, markers = pending_messages(conversation, transcript)
        parent = parent or sender
        if message_role and (not role or role == "Codex Subagent"):
            role = message_role
        source_id = input_id(markers) if markers else ""
        if not text:
            text = (payload.get("lastUserInput") or "").strip()
        if not text:
            text, sender, source_id = transcript_prompt(transcript, conversation)
            parent = parent or sender
        if not text and initial:
            text, source_id = initial, input_id([conversation, "initial", initial])
        result = post_bridge_json(base, "/api/dispatch", {
            "conversationId": conversation, "parentConversationId": parent, "agentName": name,
            "executionMode": "preInvocation", "role": role or "Codex Subagent", "cwd": cwd,
            "transcriptPath": normalize_transcript_path(transcript, conversation), "userInput": text,
            "lsAddress": address, "csrfToken": csrf, "agentapiExe": exe, "inputId": source_id,
        }, timeout=20)
        if result.get("action") == "sdkExecutor":
            print("{}")
            return
        acknowledge_messages(markers)
        if address:
            try:
                language_server_post(address, csrf, "CancelCascadeInvocation",
                                     {"cascadeId": conversation, "killBackgroundTasks": False, "notifyParent": False})
            except OSError:
                pass
        print(json.dumps({
            "terminationBehavior": "terminate",
            "injectSteps": [{
                "systemMessage": {"systemMessage": "", "metadata": {
                    "user_markdown": f"Codex accepted `{result.get('action')}` for {role or 'Codex Subagent'}."
                }},
            }],
        }))
    except Exception as exc:
        if address:
            try:
                language_server_post(
                    address,
                    csrf,
                    "CancelCascadeInvocation",
                    {"cascadeId": conversation, "killBackgroundTasks": False, "notifyParent": False},
                )
            except OSError:
                pass
        print(json.dumps({
            "terminationBehavior": "terminate",
            "injectSteps": [{"errorMessage": {"userMessage": f"Codex Subagent Bridge error: {exc}"}}],
        }))


if __name__ == "__main__":
    main()
