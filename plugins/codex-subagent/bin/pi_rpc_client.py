#!/usr/bin/env python3
"""
Native Pi Coding Agent (`pi --mode rpc`) Persistent Session Client (`pi_rpc_client.py`).

Speaks Pi's native NDJSON RPC protocol over stdin/stdout:
  - `get_state` / `get_available_models`
  - `prompt` (start turn)
  - `steer` (native mid-turn steering!)
  - `follow_up` (native queue for next turn!)
  - `abort` (native turn interrupt!)
  - `set_model` / `set_thinking_level` (live mid-session model & thinking switching!)
  - Streaming events: `message_update`, `tool_execution_start`, `tool_execution_end`, `agent_end`
"""

from __future__ import annotations

import json
import logging
import os
import queue
import shutil
import subprocess
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("pi_rpc_client")


class PiRpcError(RuntimeError):
    pass


class PiSessionProcess:
    """One persistent `pi --mode rpc` subprocess backing a single subagent conversation."""

    def __init__(
        self,
        session_key: str,
        *,
        pi_exe: Optional[str] = None,
        cwd: Optional[str] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        thinking: Optional[str] = None,
        system_prompt: Optional[str] = None,
        on_event: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None:
        self.session_key = session_key
        self.pi_exe = pi_exe or shutil.which("pi") or "pi"
        self.cwd = cwd or os.getcwd()
        self.provider = provider or ""
        self.model = model or ""
        self.thinking = thinking or ""
        self.system_prompt = system_prompt or ""
        self._on_event = on_event

        self._proc: Optional[subprocess.Popen] = None
        self._generation = 0
        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._pending: Dict[str, Tuple[subprocess.Popen, queue.Queue]] = {}
        self._pending_lock = threading.Lock()

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def start(self) -> None:
        with self._lock:
            if self.is_running():
                return
            resolved = shutil.which(self.pi_exe) or (self.pi_exe if os.path.isfile(self.pi_exe) else None)
            if not resolved:
                raise FileNotFoundError(f"Could not find `pi` executable at {self.pi_exe!r}")
            cmd = [resolved, "--mode", "rpc", "--session-id", self.session_key]
            if self.provider:
                cmd.extend(["--provider", self.provider])
            if self.model:
                cmd.extend(["--model", self.model])
            if self.thinking:
                cmd.extend(["--thinking", self.thinking])
            if self.system_prompt:
                cmd.extend(["--append-system-prompt", self.system_prompt])

            proc = subprocess.Popen(
                cmd,
                cwd=self.cwd if os.path.isdir(self.cwd) else None,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            self._proc = proc
            self._generation += 1
            gen = self._generation
            threading.Thread(
                target=self._reader_loop,
                args=(proc, gen),
                name=f"pi-rpc-reader-{self.session_key[:10]}",
                daemon=True,
            ).start()

    def close(self) -> None:
        with self._lock:
            proc, self._proc = self._proc, None
            if proc is not None:
                try:
                    if proc.stdin and not proc.stdin.closed:
                        proc.stdin.close()
                except OSError:
                    pass
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        try:
                            proc.wait(timeout=2.0)
                        except subprocess.TimeoutExpired:
                            pass

    def command(self, cmd_type: str, payload: Optional[Dict[str, Any]] = None, timeout: float = 30.0) -> Dict[str, Any]:
        self.start()
        proc = self._proc
        if proc is None or proc.poll() is not None or not proc.stdin:
            raise ConnectionError("Pi RPC process is not running")
        req_id = f"pi-{uuid.uuid4().hex[:10]}"
        q: queue.Queue = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[req_id] = (proc, q)
        msg = {"id": req_id, "type": cmd_type, **(payload or {})}
        try:
            with self._write_lock:
                proc.stdin.write(json.dumps(msg, ensure_ascii=False) + "\n")
                proc.stdin.flush()
            resp = q.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(f"Pi RPC '{cmd_type}' timed out after {timeout}s") from None
        finally:
            with self._pending_lock:
                self._pending.pop(req_id, None)
        if isinstance(resp, Exception):
            raise resp
        if not resp.get("success", True):
            raise PiRpcError(str(resp.get("error") or f"Pi RPC '{cmd_type}' failed"))
        return resp.get("data") if isinstance(resp.get("data"), dict) else resp

    def _reader_loop(self, proc: subprocess.Popen, generation: int) -> None:
        try:
            assert proc.stdout is not None
            for raw in proc.stdout:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(msg, dict):
                    continue
                mtype = msg.get("type", "")
                if mtype == "response" and msg.get("id"):
                    with self._pending_lock:
                        entry = self._pending.get(str(msg["id"]))
                    if entry is not None and entry[0] is proc:
                        try:
                            entry[1].put_nowait(msg)
                        except queue.Full:
                            pass
                elif mtype == "extension_ui_request":
                    # Reply with cancellation for interactive dialogs that expect a response
                    if msg.get("id") and msg.get("method") in ("select", "confirm", "input", "editor"):
                        try:
                            with self._write_lock:
                                if proc.stdin and not proc.stdin.closed:
                                    proc.stdin.write(json.dumps({
                                        "type": "extension_ui_response", "id": msg["id"], "cancelled": True,
                                    }) + "\n")
                                    proc.stdin.flush()
                        except OSError:
                            pass
                    continue
                elif self._on_event and proc is self._proc:
                    try:
                        self._on_event(self.session_key, {**msg, "_piGeneration": generation})
                    except Exception:
                        logger.exception("Error in Pi RPC event handler")
        except Exception as e:
            logger.debug("Pi RPC reader exited: %s", e)
        finally:
            with self._pending_lock:
                for rid, (owner, q) in list(self._pending.items()):
                    if owner is proc:
                        self._pending.pop(rid, None)
                        try:
                            q.put_nowait(ConnectionError("Pi RPC process disconnected"))
                        except queue.Full:
                            pass
            if self._on_event and proc is self._proc:
                try:
                    self._on_event(self.session_key, {"type": "pi/disconnected", "_piGeneration": generation})
                except Exception:
                    pass


class PiRpcManager:
    """Manages active `PiSessionProcess` instances and model catalog discovery."""

    def __init__(self, pi_exe: Optional[str] = None) -> None:
        self.pi_exe = pi_exe or shutil.which("pi") or "pi"
        self._lock = threading.RLock()
        self._sessions: Dict[str, PiSessionProcess] = {}
        self._event_handlers: List[Callable[[str, Dict[str, Any]], None]] = []
        self._cached_models: List[Dict[str, str]] = []

    def add_event_handler(self, handler: Callable[[str, Dict[str, Any]], None]) -> None:
        self._event_handlers.append(handler)

    def _dispatch_event(self, session_key: str, event: Dict[str, Any]) -> None:
        for h in list(self._event_handlers):
            try:
                h(session_key, event)
            except Exception:
                logger.exception("Pi event handler failed")

    def get_or_create(
        self,
        session_key: str,
        *,
        cwd: Optional[str] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        thinking: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ) -> PiSessionProcess:
        with self._lock:
            proc = self._sessions.get(session_key)
            if proc is None or not proc.is_running():
                proc = PiSessionProcess(
                    session_key,
                    pi_exe=self.pi_exe,
                    cwd=cwd,
                    provider=provider,
                    model=model,
                    thinking=thinking,
                    system_prompt=system_prompt,
                    on_event=self._dispatch_event,
                )
                proc.start()
                self._sessions[session_key] = proc
            return proc

    def get(self, session_key: str) -> Optional[PiSessionProcess]:
        with self._lock:
            return self._sessions.get(session_key)

    def close_session(self, session_key: str) -> None:
        with self._lock:
            proc = self._sessions.pop(session_key, None)
        if proc:
            proc.close()

    def close_all(self) -> None:
        with self._lock:
            procs = list(self._sessions.values())
            self._sessions.clear()
        for p in procs:
            p.close()

    def list_models(self) -> List[Dict[str, str]]:
        if self._cached_models:
            return list(self._cached_models)
        resolved = shutil.which(self.pi_exe) or (self.pi_exe if os.path.isfile(self.pi_exe) else None)
        if not resolved:
            return []
        probe = PiSessionProcess(f"probe-{uuid.uuid4().hex[:8]}", pi_exe=resolved)
        try:
            probe.start()
            data = probe.command("get_available_models", timeout=10.0)
            raw_models = data.get("models") or []
            parsed = [
                {
                    "modelId": f"{m.get('provider')}/{m.get('id')}" if m.get("provider") else str(m.get("id")),
                    "name": str(m.get("name") or m.get("id")),
                    "provider": str(m.get("provider") or ""),
                }
                for m in raw_models
                if isinstance(m, dict) and m.get("id")
            ]
            if parsed:
                self._cached_models = parsed
            return parsed
        except Exception as e:
            logger.debug("Pi get_available_models failed: %s", e)
            return []
        finally:
            probe.close()
