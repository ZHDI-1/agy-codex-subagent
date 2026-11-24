#!/usr/bin/env python3
"""Persistent Antigravity commandSpec executor for Codex-backed agents."""

from __future__ import annotations

import argparse
import json
import os
import queue
import signal
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from typing import Any

from codex_bridge_daemon import BRIDGE_PORT, ensure_bridge_daemon_running
from codex_hook_intercept import (
    acknowledge_messages, input_id, language_server_post, normalize_transcript_path, pending_messages, post_bridge_json,
)

CANCEL = object()
EOF = object()
ACTIVE_STATES = {"starting", "working", "awaiting_approval", "interrupting"}


def text_parts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        return [part for item in value for part in text_parts(item)]
    if isinstance(value, dict):
        fields = (
            "items", "additionalSteps", "additional_steps", "messageText", "message_text",
            "userInput", "user_input", "userResponse", "user_response",
            "systemMessage", "system_message", "prompt", "message", "text", "content",
            "text", "content",
        )
        for field in fields:
            parts = text_parts(value.get(field))
            if parts:
                return parts
    return []


def request_prompt(request: Any) -> str:
    if isinstance(request, str) and request.lstrip().startswith(("{", "[")):
        try:
            request = json.loads(request)
        except ValueError:
            pass
    if isinstance(request, dict) and isinstance(request.get("items"), list):
        text = "".join(item.get("text", "") for item in request["items"] if isinstance(item, dict))
        if text.strip():
            return text.strip()
    return "\n".join(text_parts(request))


class SdkExecutor:
    def __init__(self, agent_name: str) -> None:
        self.agent_name = agent_name
        self.base = ""
        self.registration: dict[str, Any] = {}
        self.awaiting_idle = False
        self.turn_id: str | None = None
        self.cancel_requested = False
        self.status_failed_at: float | None = None
        self.pending_requests: list[Any] = []
        self.replaced = False

    def initialize(self, envelope: dict[str, Any]) -> None:
        conversation = envelope.get("conversation_id") or envelope.get("conversationId")
        if not conversation or self.registration:
            raise ValueError("Expected a single init message with conversation_id")
        self.registration = {
            "conversationId": conversation,
            "parentConversationId": envelope.get("parent_conversation_id") or envelope.get("parentConversationId") or "",
            "lsAddress": envelope.get("language_server_address") or envelope.get("languageServerAddress") or "",
            "csrfToken": envelope.get("csrf_token") or envelope.get("csrfToken") or "",
            "sdkInstanceId": uuid.uuid4().hex, "sdkPid": os.getpid(), "agentName": self.agent_name,
        }
        self.register()

    def register(self) -> dict[str, Any]:
        self.base = ensure_bridge_daemon_running(BRIDGE_PORT)
        return post_bridge_json(self.base, "/api/ls-credentials", self.registration, timeout=3)

    def request(self, payload: Any) -> None:
        if not self.registration:
            raise ValueError("Received a request before init")
        if self.cancel_requested:
            self.pending_requests.append(payload)
            return
        self.awaiting_idle = True
        conversation = self.registration["conversationId"]
        transcript = normalize_transcript_path("", conversation)
        message, sender, role, markers = pending_messages(conversation, transcript)
        # FileStore contains the authoritative incoming subagent messages. A wakeup line can refer
        # to the same text, so consume it only once when both transports carry it.
        prompt = message or request_prompt(payload)
        client_message_id = (payload.get("clientMessageId") or payload.get("client_message_id")) if isinstance(payload, dict) else ""
        result = post_bridge_json(self.base, "/api/dispatch", {
            **self.registration,
            "parentConversationId": self.registration["parentConversationId"] or sender,
            "executionMode": "sdkExecutor", "role": role, "cwd": os.getcwd(),
            "transcriptPath": transcript, "userInput": prompt,
            "inputId": input_id(markers) if markers else str(client_message_id or ""),
        }, timeout=30)
        self.turn_id = result.get("turnId") or result.get("interruptedTurnId") or self.turn_id
        acknowledge_messages(markers)

    def interrupt(self) -> None:
        if not self.registration:
            return
        self.awaiting_idle = True
        if self.cancel_requested:
            self.pending_requests.clear()
            return
        self.cancel_requested = True
        result = post_bridge_json(self.base, "/api/subagent/interrupt", {
            "conversationId": self.registration["conversationId"],
            "sdkInstanceId": self.registration["sdkInstanceId"],
        }, timeout=15)
        self.turn_id = result.get("interruptedTurnId") or self.turn_id

    def status(self) -> dict[str, Any]:
        query = urllib.parse.urlencode({key: self.registration[key] for key in ("conversationId", "sdkInstanceId")})
        with urllib.request.urlopen(f"{self.base}/api/subagent/sdk-status?{query}", timeout=1.0) as response:
            return json.load(response)

    def poll(self) -> None:
        if not self.awaiting_idle:
            return
        try:
            status = self.status()
            if not status.get("registered"):
                if status.get("replaced"):
                    self.replaced = True
                    self.awaiting_idle = False
                    return
                status = self.register()
            self.status_failed_at = None
        except Exception as e:
            if self.status_failed_at is None:
                self.status_failed_at = time.monotonic()
                print(f"codex sdk executor: bridge status unavailable: {e}", file=sys.stderr)
            if time.monotonic() - self.status_failed_at >= 6:
                self.signal_idle()
            return

        if status.get("status") in ACTIVE_STATES or status.get("activeTurnId"):
            return
        if self.turn_id and status.get("idleSignalTurnId"):
            if status.get("idleSignal") == "pending":
                return
            if status.get("idleSignal") == "sent":
                self.finish_invocation()
                return
        self.signal_idle()

    def signal_idle(self) -> None:
        address = self.registration["lsAddress"]
        if address:
            try:
                language_server_post(address, self.registration["csrfToken"], "SignalExecutableIdle",
                                     {"conversationId": self.registration["conversationId"]})
            except OSError as e:
                print(f"codex sdk executor: SignalExecutableIdle failed: {e}", file=sys.stderr)
                return
        self.finish_invocation()

    def finish_invocation(self) -> None:
        self.turn_id = None
        self.awaiting_idle = False
        self.cancel_requested = False

    def unregister(self) -> None:
        if self.base and self.registration and not self.replaced:
            try:
                post_bridge_json(self.base, "/api/subagent/sdk-unregister", {
                    "conversationId": self.registration["conversationId"],
                    "sdkInstanceId": self.registration["sdkInstanceId"],
                }, timeout=2)
            except OSError:
                pass


def read_input(inbox: queue.SimpleQueue) -> None:
    try:
        for line in sys.stdin:
            try:
                message = json.loads(line)
                if isinstance(message, dict):
                    inbox.put(message)
            except ValueError:
                print("codex sdk executor: invalid NDJSON message", file=sys.stderr)
    finally:
        inbox.put(EOF)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-name", default="codex")
    args = parser.parse_args()
    executor = SdkExecutor(args.agent_name)
    inbox: queue.SimpleQueue = queue.SimpleQueue()
    signal.signal(signal.SIGINT, lambda _signum, _frame: inbox.put(CANCEL))
    signal.signal(signal.SIGTERM, lambda _signum, _frame: inbox.put(EOF))
    threading.Thread(target=read_input, args=(inbox,), daemon=True, name="codex-sdk-stdin").start()
    stop_deadline: float | None = None
    try:
        while not executor.replaced:
            try:
                if not executor.cancel_requested and executor.pending_requests:
                    executor.request(executor.pending_requests.pop(0))
                elif stop_deadline is None:
                    event = inbox.get(timeout=0.25)
                    if event is EOF:
                        stop_deadline = time.monotonic() + 10
                        executor.pending_requests.clear()
                        if executor.awaiting_idle:
                            executor.interrupt()
                    elif event is CANCEL:
                        executor.interrupt()
                    elif event.get("type") == "init":
                        executor.initialize(event)
                    elif event.get("type") == "request":
                        executor.request(event.get("request"))
                else:
                    time.sleep(0.25)
            except queue.Empty:
                pass
            except Exception as e:
                print(f"codex sdk executor: {e}", file=sys.stderr)
            executor.poll()
            if stop_deadline is not None and (
                not executor.awaiting_idle or time.monotonic() >= stop_deadline
            ):
                break
    finally:
        executor.unregister()


if __name__ == "__main__":
    main()
