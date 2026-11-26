#!/usr/bin/env python3
"""
Persistent Antigravity ACP (`agy acp --stdio`) JSON-RPC 2.0 Client.

Mirrors `CodexAppServerClient` so `UnifiedSubagentBridge` can orchestrate exact-model
Antigravity subagents (`claude-opus-5-5-high`, `claude-sonnet-5-5-high`,
`gemini-4-neon-xhigh`, `gemini-4-argon-high`, `gpt-oss-120b-medium`, etc.)
over a single shared persistent `agy acp` process with:
  - `session/new` (including `_meta.antigravity.systemPrompt.append`)
  - `session/set_model` (live mid-session model switching!)
  - `session/set_mode` (`default`, `accept-edits`, `auto`, `plan`)
  - `session/prompt` (multi-turn execution + streaming `session/update` notifications)
  - `session/cancel` (immediate turn cancellation)
  - `session/resume` / `session/load` / `session/list` / `session/close`
"""

from __future__ import annotations

import json
import logging
import os
import queue
import shutil
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("agy_acp_client")


class AgyAcpRpcError(RuntimeError):
    def __init__(self, code: int, message: str, data: Any = None) -> None:
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"AGY ACP RPC Error [{code}]: {message}")


class AgyAcpClient:
    """Thread-safe persistent JSON-RPC 2.0 client over `agy acp --stdio`."""

    def __init__(
        self,
        agy_exe: Optional[str] = None,
        auto_start: bool = True,
        dangerously_skip_permissions: bool = True,
    ) -> None:
        self.agy_exe = agy_exe or shutil.which("agy") or os.path.expanduser("~/.local/bin/agy")
        self.auto_start = auto_start
        self.dangerously_skip_permissions = dangerously_skip_permissions

        self._proc: Optional[subprocess.Popen] = None
        self._conn_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._id_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._next_id = 1
        self._pending: Dict[int, Tuple[subprocess.Popen, queue.Queue]] = {}
        self._callbacks: queue.SimpleQueue = queue.SimpleQueue()
        self._callback_worker: Optional[threading.Thread] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._notification_handlers: List[Callable[[str, Dict[str, Any]], None]] = []
        self._initialized = False
        self.agent_info: Dict[str, Any] = {}
        self.capabilities: Dict[str, Any] = {}
        self.available_models: List[Dict[str, str]] = []

    def add_notification_handler(self, handler: Callable[[str, Dict[str, Any]], None]) -> None:
        self._notification_handlers.append(handler)

    def is_connected(self) -> bool:
        proc = self._proc
        return proc is not None and proc.poll() is None and self._initialized

    @property
    def connection(self) -> Optional[subprocess.Popen]:
        return self._proc if self.is_connected() else None

    def connect(self) -> Dict[str, Any]:
        with self._conn_lock:
            if self.is_connected():
                return {"agentInfo": self.agent_info, "capabilities": self.capabilities}
            if not self.auto_start:
                raise ConnectionError("AGY ACP process is not running and auto_start is disabled")

            exe = self.agy_exe
            if not exe or not (shutil.which(exe) or os.path.isfile(exe)):
                raise FileNotFoundError(f"Could not find `agy` executable at {exe!r}")

            cmd = [exe, "acp", "--stdio"]
            if self.dangerously_skip_permissions:
                cmd.append("--dangerously-skip-permissions")

            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            self._proc = proc
            self._initialized = False

            if self._callback_worker is None or not self._callback_worker.is_alive():
                self._callback_worker = threading.Thread(
                    target=self._run_callbacks,
                    args=(self._callbacks,),
                    name="agy-acp-callbacks",
                    daemon=True,
                )
                self._callback_worker.start()

            self._reader_thread = threading.Thread(
                target=self._reader_loop,
                args=(proc, self._callbacks),
                name="agy-acp-reader",
                daemon=True,
            )
            self._reader_thread.start()

            try:
                init_res = self._raw_request(
                    proc,
                    "initialize",
                    {"protocolVersion": 1, "clientCapabilities": {}},
                    timeout=15.0,
                )
                self.agent_info = init_res.get("agentInfo") or {}
                self.capabilities = init_res.get("agentCapabilities") or {}
                self._initialized = True
            except BaseException:
                self._terminate_proc(proc)
                if self._proc is proc:
                    self._proc = None
                raise

            logger.info("Connected to Antigravity ACP Server (%s)", self.agent_info)
            return init_res

    def close(self) -> None:
        with self._conn_lock:
            self._initialized = False
            proc, self._proc = self._proc, None
            if proc is not None:
                self._terminate_proc(proc)
            self._callbacks.put(None)
            self._callbacks = queue.SimpleQueue()
            self._callback_worker = None

    @staticmethod
    def _terminate_proc(proc: subprocess.Popen) -> None:
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

    def _next_request_id(self) -> int:
        with self._id_lock:
            rid = self._next_id
            self._next_id += 1
            return rid

    def _send_json(self, proc: subprocess.Popen, obj: Dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=False) + "\n"
        with self._write_lock:
            if proc.poll() is not None or not proc.stdin or proc.stdin.closed:
                raise ConnectionError("AGY ACP process stdin is closed")
            proc.stdin.write(line)
            proc.stdin.flush()

    def _raw_request(
        self,
        proc: subprocess.Popen,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 60.0,
    ) -> Any:
        req_id = self._next_request_id()
        reply: queue.Queue = queue.Queue(maxsize=1)
        with self._pending_lock:
            if proc.poll() is not None:
                raise ConnectionError("AGY ACP process is not running")
            self._pending[req_id] = (proc, reply)

        msg: Dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params is not None:
            msg["params"] = params

        try:
            self._send_json(proc, msg)
            response = reply.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(f"AGY ACP RPC '{method}' timed out after {timeout}s") from None
        finally:
            with self._pending_lock:
                self._pending.pop(req_id, None)

        if isinstance(response, Exception):
            raise response
        if response.get("error") is not None:
            err = response["error"]
            raise AgyAcpRpcError(err.get("code", -1), err.get("message", "Unknown ACP error"), err.get("data"))
        return response.get("result")

    def request(self, method: str, params: Optional[Dict[str, Any]] = None, timeout: float = 60.0) -> Any:
        self.connect()
        proc = self.connection
        if proc is None:
            raise ConnectionError("AGY ACP process closed before request")
        return self._raw_request(proc, method, params, timeout=timeout)

    def notify(self, method: str, params: Optional[Dict[str, Any]] = None) -> None:
        self.connect()
        proc = self.connection
        if proc is None:
            raise ConnectionError("AGY ACP process closed before notification")
        msg: Dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        self._send_json(proc, msg)

    @staticmethod
    def _run_callbacks(callbacks: queue.SimpleQueue) -> None:
        while (work := callbacks.get()) is not None:
            fn, args = work
            try:
                fn(*args)
            except Exception:
                logger.exception("Error in AGY ACP callback")

    def _deliver_notification(self, proc: subprocess.Popen, method: str, params: Dict[str, Any]) -> None:
        if proc is not self._proc and method != "acp/disconnected":
            return
        for handler in list(self._notification_handlers):
            try:
                handler(method, params)
            except Exception:
                logger.exception("Error in AGY ACP notification handler for %s", method)

    def _handle_server_request(self, proc: subprocess.Popen, req_id: Any, method: str, params: Dict[str, Any]) -> None:
        # Handle ACP permission prompts cleanly (auto-allow when running autonomously)
        if method == "session/request_permission":
            options = params.get("options") or []
            chosen_id = None
            for kind in ("allow_always", "allow_once"):
                for opt in options:
                    if isinstance(opt, dict) and opt.get("kind") == kind:
                        chosen_id = opt.get("optionId")
                        break
                if chosen_id:
                    break
            if not chosen_id and options and isinstance(options[0], dict):
                chosen_id = options[0].get("optionId")
            result: Dict[str, Any] = (
                {"outcome": {"outcome": "selected", "optionId": chosen_id}}
                if chosen_id
                else {"outcome": {"outcome": "cancelled"}}
            )
            try:
                self._send_json(proc, {"jsonrpc": "2.0", "id": req_id, "result": result})
            except Exception as e:
                logger.warning("Failed replying to ACP permission request: %s", e)
            return

        try:
            self._send_json(
                proc,
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Unsupported client method: {method}"},
                },
            )
        except Exception:
            pass

    def _reader_loop(self, proc: subprocess.Popen, callbacks: queue.SimpleQueue) -> None:
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

                req_id = msg.get("id")
                method = msg.get("method")
                params = msg.get("params") or {}

                if req_id is not None and method is None:
                    with self._pending_lock:
                        pending = self._pending.get(req_id)
                        if pending and pending[0] is proc:
                            try:
                                pending[1].put_nowait(msg)
                            except queue.Full:
                                pass
                elif req_id is not None and method is not None:
                    callbacks.put((self._handle_server_request, (proc, req_id, method, params)))
                elif method is not None:
                    callbacks.put((self._deliver_notification, (proc, method, params)))
        except Exception as e:
            logger.debug("AGY ACP reader exited: %s", e)
        finally:
            with self._conn_lock:
                if self._proc is proc:
                    self._initialized = False
                    self._proc = None
            with self._pending_lock:
                for rid, (owner, q) in list(self._pending.items()):
                    if owner is proc:
                        self._pending.pop(rid, None)
                        try:
                            q.put_nowait(ConnectionError("AGY ACP process disconnected"))
                        except queue.Full:
                            pass
            callbacks.put((self._deliver_notification, (proc, "acp/disconnected", {"connection": proc})))

    # -------------------------------------------------------------------------
    # High-Level ACP Session APIs
    # -------------------------------------------------------------------------

    def session_new(
        self,
        cwd: str,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "cwd": cwd or os.getcwd(),
            "mcpServers": [],
        }
        if system_prompt:
            params["_meta"] = {"antigravity": {"systemPrompt": {"append": system_prompt}}}
        res = self.request("session/new", params, timeout=30.0)
        models_block = res.get("models") or {}
        if isinstance(models_block.get("availableModels"), list) and models_block["availableModels"]:
            self.available_models = list(models_block["availableModels"])
        sid = res.get("sessionId")
        if sid and model and model != models_block.get("currentModelId"):
            self.session_set_model(sid, model)
            res["selectedModelId"] = model
        else:
            res["selectedModelId"] = models_block.get("currentModelId") or model or ""
        if sid and mode:
            try:
                self.session_set_mode(sid, mode)
            except Exception as e:
                logger.debug("session/set_mode skipped: %s", e)
        return res

    def session_set_model(self, session_id: str, model_id: str) -> Dict[str, Any]:
        return self.request("session/set_model", {"sessionId": session_id, "modelId": model_id}, timeout=15.0)

    def session_set_mode(self, session_id: str, mode_id: str) -> Dict[str, Any]:
        return self.request("session/set_mode", {"sessionId": session_id, "modeId": mode_id}, timeout=15.0)

    def session_prompt(self, session_id: str, text: str, timeout: float = 3600.0) -> Dict[str, Any]:
        return self.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": text}]},
            timeout=timeout,
        )

    def session_cancel(self, session_id: str) -> None:
        self.notify("session/cancel", {"sessionId": session_id})

    def session_resume(self, session_id: str, cwd: Optional[str] = None) -> Dict[str, Any]:
        return self.request(
            "session/resume",
            {"sessionId": session_id, "cwd": cwd or os.getcwd(), "mcpServers": []},
            timeout=20.0,
        )

    def session_list(self, cwd: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if cwd:
            params["cwd"] = cwd
        return self.request("session/list", params, timeout=15.0)

    def session_close(self, session_id: str) -> Dict[str, Any]:
        return self.request("session/close", {"sessionId": session_id}, timeout=10.0)

    def list_models(self) -> List[Dict[str, str]]:
        """Returns available exact `agy` model IDs (cached from ACP or queried via `agy models`)."""
        if self.available_models:
            return list(self.available_models)
        try:
            proc = subprocess.run([self.agy_exe, "models"], capture_output=True, text=True, timeout=15.0)
            if proc.returncode == 0:
                parsed: List[Dict[str, str]] = []
                for line in proc.stdout.splitlines():
                    clean = line.strip()
                    if not clean or clean.startswith("Fetching"):
                        continue
                    parts = clean.split(None, 1)
                    if len(parts) == 2 and "-" in parts[0]:
                        parsed.append({"modelId": parts[0].strip(), "name": parts[1].strip()})
                if parsed:
                    self.available_models = parsed
                    return list(parsed)
        except Exception as e:
            logger.debug("agy models CLI fallback failed: %s", e)
        return []
