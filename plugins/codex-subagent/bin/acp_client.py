#!/usr/bin/env python3
"""
Generic Agent Client Protocol (ACP) Stdio JSON-RPC 2.0 Client (`acp_client.py`).

Works with ANY ACP-compliant agent server over stdio JSON-RPC 2.0, including:
  - Antigravity ACP:      ["agy", "acp", "--stdio", "--dangerously-skip-permissions"]
  - Pi ACP adapter:       ["pi-acp"] / ["npx", "-y", "pi-acp"]
  - Claude Code ACP:      ["claude-code-acp"]
  - OpenCode ACP:         ["opencode", "acp"]
  - Any custom ACP binary specified via `acpCommand` + `acpEnv` in a profile.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import shutil
import subprocess
import threading
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("acp_client")


class AcpRpcError(RuntimeError):
    def __init__(self, code: int, message: str, data: Any = None) -> None:
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"ACP RPC Error [{code}]: {message}")


class GenericAcpClient:
    """Thread-safe persistent JSON-RPC 2.0 client over any ACP stdio server command."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        env: Optional[Dict[str, str]] = None,
        auto_start: bool = True,
        default_mode: Optional[str] = "auto",
        auto_approve: bool = False,
        name: str = "acp",
        system_prompt_meta_builder: Optional[Callable[[str], Dict[str, Any]]] = None,
    ) -> None:
        if not command:
            raise ValueError("ACP command must be a non-empty list of arguments")
        self.command: List[str] = [str(part) for part in command]
        self.env_overrides: Dict[str, str] = dict(env or {})
        self.auto_start = auto_start
        self.default_mode = default_mode
        self.auto_approve = auto_approve
        self.name = name
        self._system_prompt_meta_builder = system_prompt_meta_builder

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
        self.available_modes: List[Dict[str, Any]] = []

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
                raise ConnectionError(f"ACP process ({self.name}) is not running and auto_start is disabled")

            exe = self.command[0]
            resolved = shutil.which(exe) or (exe if os.path.isfile(exe) else None)
            if not resolved:
                raise FileNotFoundError(f"Could not find ACP executable {exe!r} for backend {self.name!r}")

            cmd = [resolved, *self.command[1:]]
            proc_env = {**os.environ, **self.env_overrides}
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                env=proc_env,
                start_new_session=True,
            )
            self._proc = proc
            self._initialized = False

            if self._callback_worker is None or not self._callback_worker.is_alive():
                self._callback_worker = threading.Thread(
                    target=self._run_callbacks,
                    args=(self._callbacks,),
                    name=f"{self.name}-acp-callbacks",
                    daemon=True,
                )
                self._callback_worker.start()

            self._reader_thread = threading.Thread(
                target=self._reader_loop,
                args=(proc, self._callbacks),
                name=f"{self.name}-acp-reader",
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

            logger.info("Connected to ACP server [%s]: %s", self.name, self.agent_info)
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
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    pass

    def _next_request_id(self) -> int:
        with self._id_lock:
            rid = self._next_id
            self._next_id += 1
            return rid

    def _send_json(self, proc: subprocess.Popen, obj: Dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=False) + "\n"
        with self._write_lock:
            if proc.poll() is not None or not proc.stdin or proc.stdin.closed:
                raise ConnectionError(f"ACP process ({self.name}) stdin is closed")
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
                raise ConnectionError(f"ACP process ({self.name}) is not running")
            self._pending[req_id] = (proc, reply)

        msg: Dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params is not None:
            msg["params"] = params

        try:
            self._send_json(proc, msg)
            response = reply.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(f"ACP RPC '{method}' ({self.name}) timed out after {timeout}s") from None
        finally:
            with self._pending_lock:
                self._pending.pop(req_id, None)

        if isinstance(response, Exception):
            raise response
        if response.get("error") is not None:
            err = response["error"]
            raise AcpRpcError(err.get("code", -1), err.get("message", "Unknown ACP error"), err.get("data"))
        return response.get("result")

    def request(self, method: str, params: Optional[Dict[str, Any]] = None, timeout: float = 60.0) -> Any:
        self.connect()
        proc = self.connection
        if proc is None:
            raise ConnectionError(f"ACP process ({self.name}) closed before request")
        return self._raw_request(proc, method, params, timeout=timeout)

    def notify(self, method: str, params: Optional[Dict[str, Any]] = None) -> None:
        self.connect()
        proc = self.connection
        if proc is None:
            raise ConnectionError(f"ACP process ({self.name}) closed before notification")
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
                logger.exception("Error in ACP callback")

    def flush_callbacks(self, timeout: float = 5.0) -> bool:
        """Waits until all notifications queued before this barrier have been delivered."""
        worker = self._callback_worker
        if worker is None or not worker.is_alive():
            return True
        done = threading.Event()
        self._callbacks.put((done.set, ()))
        return done.wait(timeout=timeout)

    def _deliver_notification(self, proc: subprocess.Popen, method: str, params: Dict[str, Any]) -> None:
        if proc is not self._proc and method != "acp/disconnected":
            return
        enriched = {**params, "_acpClientId": id(self)} if isinstance(params, dict) else params
        for handler in list(self._notification_handlers):
            try:
                handler(method, enriched)
            except Exception:
                logger.exception("Error in ACP notification handler for %s", method)

    def _handle_server_request(self, proc: subprocess.Popen, req_id: Any, method: str, params: Dict[str, Any]) -> None:
        if method == "session/request_permission":
            options = params.get("options") or []
            chosen_id = None
            if self.auto_approve:
                for kind in ("allow_always", "allow_once"):
                    for opt in options:
                        if isinstance(opt, dict) and opt.get("kind") == kind:
                            chosen_id = opt.get("optionId")
                            break
                    if chosen_id:
                        break
            else:
                for kind in ("reject_once", "reject_always"):
                    for opt in options:
                        if isinstance(opt, dict) and opt.get("kind") == kind:
                            chosen_id = opt.get("optionId")
                            break
                    if chosen_id:
                        break
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
            logger.debug("ACP reader (%s) exited: %s", self.name, e)
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
                            q.put_nowait(ConnectionError(f"ACP process ({self.name}) disconnected"))
                        except queue.Full:
                            pass
            callbacks.put((self._deliver_notification, (proc, "acp/disconnected", {"connection": proc, "clientName": self.name})))

    # -------------------------------------------------------------------------
    # Standard ACP Session Operations
    # -------------------------------------------------------------------------

    def supports_native_system_prompt(self) -> bool:
        if self._system_prompt_meta_builder is not None:
            return True
        meta = self.capabilities.get("_meta") or {}
        ag = meta.get("antigravity") or {}
        return bool((ag.get("systemPrompt") or {}).get("append"))

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
            if self._system_prompt_meta_builder is not None:
                params["_meta"] = self._system_prompt_meta_builder(system_prompt)
            elif self.supports_native_system_prompt():
                params["_meta"] = {"antigravity": {"systemPrompt": {"append": system_prompt}}}

        res = self.request("session/new", params, timeout=30.0)
        models_block = res.get("models") or {}
        if isinstance(models_block.get("availableModels"), list) and models_block["availableModels"]:
            self.available_models = list(models_block["availableModels"])
        modes_block = res.get("modes") or {}
        if isinstance(modes_block.get("availableModes"), list) and modes_block["availableModes"]:
            self.available_modes = list(modes_block["availableModes"])

        sid = res.get("sessionId")
        if sid and model and model != models_block.get("currentModelId"):
            self.session_set_model(sid, model)
            res["selectedModelId"] = model
        else:
            res["selectedModelId"] = models_block.get("currentModelId") or model or ""

        eff_mode = mode if mode is not None else self.default_mode
        if sid and eff_mode:
            try:
                self.session_set_mode(sid, eff_mode)
            except Exception as e:
                logger.debug("ACP session/set_mode(%s) skipped: %s", eff_mode, e)
        return res

    def session_set_model(self, session_id: str, model_id: str) -> Dict[str, Any]:
        return self.request("session/set_model", {"sessionId": session_id, "modelId": model_id}, timeout=15.0)

    def session_set_mode(self, session_id: str, mode_id: str) -> Dict[str, Any]:
        return self.request("session/set_mode", {"sessionId": session_id, "modeId": mode_id}, timeout=15.0)

    def session_set_config_option(self, session_id: str, config_id: str, value: Any) -> Dict[str, Any]:
        return self.request(
            "session/set_config_option",
            {"sessionId": session_id, "configId": config_id, "value": value},
            timeout=15.0,
        )

    def session_prompt(self, session_id: str, text: str, timeout: float = 3600.0) -> Dict[str, Any]:
        return self.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": text}]},
            timeout=timeout,
        )

    def session_cancel(self, session_id: str) -> None:
        self.notify("session/cancel", {"sessionId": session_id})

    def session_resume(self, session_id: str, cwd: Optional[str] = None) -> Dict[str, Any]:
        payload = {"sessionId": session_id, "cwd": cwd or os.getcwd(), "mcpServers": []}
        try:
            return self.request("session/resume", payload, timeout=20.0)
        except AcpRpcError as e:
            if e.code == -32601 or "not supported" in e.message.lower():
                return self.request("session/load", payload, timeout=20.0)
            raise

    def session_list(self, cwd: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if cwd:
            params["cwd"] = cwd
        return self.request("session/list", params, timeout=15.0)

    def session_close(self, session_id: str) -> Dict[str, Any]:
        return self.request("session/close", {"sessionId": session_id}, timeout=10.0)

    def list_models(self) -> List[Dict[str, str]]:
        return list(self.available_models)
