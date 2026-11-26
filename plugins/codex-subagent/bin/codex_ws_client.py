#!/usr/bin/env python3
"""
Pure-Python WebSocket-over-AF_UNIX JSON-RPC 2.0 Client for OpenAI Codex App-Server Daemon.

Connects directly to ~/.codex/app-server-control/app-server-control.sock
(HTTP/1.1 Upgrade: websocket over AF_UNIX), supporting full-duplex JSON-RPC 2.0:
  - Client -> Server requests (thread/start, thread/resume, thread/fork, thread/revert,
    thread/read, thread/compact/start, thread/queue/add, turn/start, turn/steer,
    turn/interrupt, turn/settings/update, etc.)
  - Server -> Client notifications (turn/started, turn/completed, turn/diff/updated,
    turn/plan/updated, item/started, item/completed, item/agentMessage/delta,
    item/reasoning/summaryTextDelta, item/commandExecution/outputDelta, etc.)
  - Server -> Client bidirectional requests (item/commandExecution/requestApproval,
    item/fileChange/requestApproval, item/permissions/requestApproval,
    item/tool/call, item/tool/requestUserInput)
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import queue
import socket
import struct
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("codex_ws_client")

DEFAULT_CONTROL_SOCK = Path.home() / ".codex" / "app-server-control" / "app-server-control.sock"
DEFAULT_DAEMON_PID = Path.home() / ".codex" / "app-server-daemon" / "daemon.pid"


class CodexRpcError(Exception):
    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(f"Codex RPC Error [{code}]: {message}")
        self.code = code
        self.message = message
        self.data = data


class UnixWebSocketConnection:
    """Minimal, dependency-free RFC 6455 WebSocket client over AF_UNIX."""

    def __init__(self, sock_path: str | Path, timeout: float = 15.0):
        self.sock_path = str(sock_path)
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self._write_lock = threading.Lock()
        self._closed = False
        self._leftover = b""

    def connect(self) -> None:
        resolved = os.path.realpath(self.sock_path)
        if not os.path.exists(resolved):
            raise FileNotFoundError(f"Codex control socket not found: {self.sock_path} (-> {resolved})")

        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        try:
            s.settimeout(self.timeout)
            s.connect(resolved)
            s.sendall((
                "GET / HTTP/1.1\r\n"
                "Host: localhost\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\n"
                "Sec-WebSocket-Version: 13\r\n\r\n"
            ).encode("ascii"))

            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = s.recv(4096)
                if not chunk or len(resp) > 65536:
                    raise ConnectionError("Invalid WebSocket upgrade response from Codex")
                resp += chunk

            header, self._leftover = resp.split(b"\r\n\r\n", 1)
            status, *fields = header.decode("latin1").split("\r\n")
            headers = dict(field.split(":", 1) for field in fields if ":" in field)
            headers = {name.lower(): value.strip() for name, value in headers.items()}
            expected = base64.b64encode(hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
            ).digest()).decode("ascii")
            if status.split()[1:2] != ["101"] or headers.get("sec-websocket-accept") != expected:
                raise ConnectionError(f"WebSocket upgrade rejected by Codex daemon: {status}")
            s.settimeout(None)
        except BaseException:
            s.close()
            raise
        self.sock = s
        self._closed = False

    def close(self) -> None:
        self._closed = True
        sock, self.sock = self.sock, None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def send_text(self, text: str) -> None:
        payload = text.encode("utf-8")
        self._send_frame(0x1, payload)

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self._closed or self.sock is None:
            raise ConnectionError("WebSocket is closed")
        mask_key = os.urandom(4)
        header = bytearray()
        header.append(0x80 | (opcode & 0x0F))
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        header.extend(mask_key)

        # Fast XOR masking in 4-byte blocks / int conversion
        if length > 0:
            masked = bytearray(payload)
            m0, m1, m2, m3 = mask_key[0], mask_key[1], mask_key[2], mask_key[3]
            for i in range(length):
                masked[i] ^= (m0 if (i & 3) == 0 else m1 if (i & 3) == 1 else m2 if (i & 3) == 2 else m3)
            frame = bytes(header) + bytes(masked)
        else:
            frame = bytes(header)

        with self._write_lock:
            sock = self.sock
            if self._closed or sock is None:
                raise ConnectionError("WebSocket is closed")
            sock.sendall(frame)

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        if self._leftover:
            take = min(len(self._leftover), n)
            buf.extend(self._leftover[:take])
            self._leftover = self._leftover[take:]
        while len(buf) < n:
            sock = self.sock
            if sock is None:
                raise EOFError("Socket closed")
            chunk = sock.recv(max(4096, n - len(buf)))
            if not chunk:
                raise EOFError("WebSocket EOF from Codex daemon")
            need = n - len(buf)
            if len(chunk) <= need:
                buf.extend(chunk)
            else:
                buf.extend(chunk[:need])
                self._leftover += chunk[need:]
        return bytes(buf)

    def recv_message(self) -> Optional[str]:
        """Reads a complete WebSocket text message, handling fragmentation and ping/pong."""
        fragments: List[bytes] = []
        while not self._closed:
            hdr = self._recv_exact(2)
            fin = (hdr[0] & 0x80) != 0
            opcode = hdr[0] & 0x0F
            masked = (hdr[1] & 0x80) != 0
            length = hdr[1] & 0x7F

            if length == 126:
                length = struct.unpack("!H", self._recv_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._recv_exact(8))[0]

            mask_key = self._recv_exact(4) if masked else b""
            payload = self._recv_exact(length) if length > 0 else b""

            if masked and payload:
                arr = bytearray(payload)
                for i in range(len(arr)):
                    arr[i] ^= mask_key[i & 3]
                payload = bytes(arr)

            if opcode == 0x8:  # Close
                self._closed = True
                return None
            elif opcode == 0x9:  # Ping
                try:
                    self._send_frame(0xA, payload)
                except Exception:
                    pass
                continue
            elif opcode == 0xA:  # Pong
                continue
            elif opcode in (0x1, 0x2, 0x0):  # Text, Binary, Continuation
                fragments.append(payload)
                if fin:
                    raw = b"".join(fragments)
                    return raw.decode("utf-8", errors="replace")
            else:
                continue
        return None


class CodexAppServerClient:
    """
    Full-featured JSON-RPC 2.0 client for `codex app-server` daemon.
    Automatically connects, initializes experimentalApi=true, and dispatches
    notifications & server requests.
    """

    def __init__(
        self,
        sock_path: str | Path = DEFAULT_CONTROL_SOCK,
        client_name: str = "antigravity-codex-subagent",
        client_version: str = "1.0.0",
        auto_start_daemon: bool = True,
    ):
        self.sock_path = Path(sock_path)
        self.client_name = client_name
        self.client_version = client_version
        self.auto_start_daemon = auto_start_daemon

        self._ws: Optional[UnixWebSocketConnection] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._next_id = 1
        self._id_lock = threading.Lock()
        self._pending: Dict[int | str, tuple[UnixWebSocketConnection, queue.Queue]] = {}
        self._callbacks: queue.SimpleQueue = queue.SimpleQueue()
        self._callback_worker: Optional[threading.Thread] = None
        self._pending_lock = threading.Lock()
        self._conn_lock = threading.RLock()
        self._initialized = False
        self.user_agent: str = ""

        # Notification subscribers: fn(method: str, params: dict) -> None
        self._notification_handlers: List[Callable[[str, Dict[str, Any]], None]] = []
        # Server request handler: fn(req_id: int|str, method: str, params: dict) -> Optional[dict]
        # If it returns a dict, that dict is sent back immediately as the JSON-RPC result.
        # If it returns None, the handler is holding the request asynchronously (e.g., UI approval)
        # and will call client.respond_to_server_request(req_id, result) later.
        self._server_request_handler: Optional[
            Callable[[int | str, str, Dict[str, Any], Callable[..., None]], Optional[Dict[str, Any]]]
        ] = None

    def add_notification_handler(self, handler: Callable[[str, Dict[str, Any]], None]) -> None:
        self._notification_handlers.append(handler)

    def set_server_request_handler(
        self,
        handler: Callable[[int | str, str, Dict[str, Any], Callable[..., None]], Optional[Dict[str, Any]]],
    ) -> None:
        self._server_request_handler = handler

    def is_connected(self) -> bool:
        ws = self._ws
        return ws is not None and not ws._closed and self._initialized

    @property
    def connection(self) -> Optional[UnixWebSocketConnection]:
        return self._ws if self.is_connected() else None

    def ensure_daemon_running(self) -> None:
        """Verify the control socket is reachable, starting Codex if needed."""
        def available() -> bool:
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                    probe.settimeout(1.0)
                    probe.connect(os.path.realpath(self.sock_path))
                return True
            except OSError:
                return False

        if available():
            return
        if not self.auto_start_daemon:
            raise ConnectionError(f"Codex app-server socket is unavailable at {self.sock_path}")
        process = subprocess.Popen(
            ["codex", "app-server", "--remote-control", "--listen", "unix://", "--managed-daemon"],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        threading.Thread(target=process.wait, name="codex-daemon-reaper", daemon=True).start()
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if available():
                return
            time.sleep(0.2)
        raise TimeoutError(f"Timed out waiting for Codex app-server socket at {self.sock_path}")

    def connect(self) -> Dict[str, Any]:
        with self._conn_lock:
            if self.is_connected():
                return {"userAgent": self.user_agent}

            self.ensure_daemon_running()
            ws = UnixWebSocketConnection(self.sock_path)
            ws.connect()
            self._ws = ws
            self._initialized = False
            if self._callback_worker is None:
                self._callback_worker = threading.Thread(
                    target=self._run_callbacks, args=(self._callbacks,),
                    name="codex-ws-callbacks", daemon=True,
                )
                self._callback_worker.start()
            self._reader_thread = threading.Thread(
                target=self._reader_loop, args=(ws, self._callbacks), name="codex-ws-reader", daemon=True,
            )
            self._reader_thread.start()
            try:
                init_res = self._raw_request(
                    ws, "initialize",
                    {
                        "clientInfo": {"name": self.client_name, "title": "Antigravity Codex Subagent Bridge",
                                       "version": self.client_version},
                        "capabilities": {"experimentalApi": True},
                    },
                    timeout=10.0,
                )
                self.user_agent = init_res.get("userAgent", "")
                self._initialized = True
            except BaseException:
                ws.close()
                raise
            logger.info("Connected to Codex App-Server Daemon (%s)", self.user_agent)
            return init_res

    def close(self) -> None:
        with self._conn_lock:
            self._initialized = False
            ws, self._ws = self._ws, None
            if ws is not None:
                ws.close()
            self._callbacks.put(None)
            self._callbacks = queue.SimpleQueue()
            self._callback_worker = None

    def _next_request_id(self) -> int:
        with self._id_lock:
            rid = self._next_id
            self._next_id += 1
            return rid

    def _raw_request(self, ws: UnixWebSocketConnection, method: str, params: Any = None, timeout: float = 60.0) -> Any:
        req_id = self._next_request_id()
        reply: queue.Queue = queue.Queue(maxsize=1)
        with self._pending_lock:
            if ws._closed:
                raise ConnectionError("WebSocket is not connected")
            self._pending[req_id] = (ws, reply)

        msg = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params is not None:
            msg["params"] = params

        try:
            ws.send_text(json.dumps(msg))
            response = reply.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(f"Codex RPC '{method}' timed out after {timeout}s") from None
        finally:
            with self._pending_lock:
                self._pending.pop(req_id, None)

        if isinstance(response, Exception):
            raise response
        if response.get("error") is not None:
            err = response["error"]
            raise CodexRpcError(err.get("code", -1), err.get("message", "Unknown RPC error"), err.get("data"))
        return response.get("result")

    def request(self, method: str, params: Any = None, timeout: float = 60.0) -> Any:
        """Reconnect before sending. A lost response cannot safely be replayed."""
        self.connect()
        ws = self.connection
        if ws is None:
            raise ConnectionError("Codex closed the connection before the request")
        return self._raw_request(ws, method, params, timeout)

    def respond_to_server_request(
        self,
        req_id: int | str,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
        *,
        connection: Optional[UnixWebSocketConnection] = None,
    ) -> None:
        ws = connection or self.connection
        if ws is None:
            raise ConnectionError("WebSocket is not connected")
        payload: Dict[str, Any] = {"jsonrpc": "2.0", "id": req_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result if result is not None else {}
        ws.send_text(json.dumps(payload))

    @staticmethod
    def _run_callbacks(callbacks: queue.SimpleQueue) -> None:
        while (work := callbacks.get()) is not None:
            fn, args = work
            try:
                fn(*args)
            except Exception:
                logger.exception("Error in Codex callback")

    def _deliver_notification(self, ws: UnixWebSocketConnection, method: str, params: Dict[str, Any]) -> None:
        # An old reader can drain its last frames while a new connection initializes.
        if ws is not self._ws and method != "bridge/disconnected":
            return
        for handler in list(self._notification_handlers):
            try:
                handler(method, params)
            except Exception:
                logger.exception("Error in notification handler for %s", method)

    def _reader_loop(self, ws: UnixWebSocketConnection, callbacks: queue.SimpleQueue) -> None:
        try:
            while not ws._closed:
                raw = ws.recv_message()
                if raw is None:
                    break
                try:
                    msg = json.loads(raw)
                except ValueError:
                    logger.warning("Invalid JSON from Codex daemon")
                    continue
                if not isinstance(msg, dict):
                    continue

                req_id = msg.get("id")
                method = msg.get("method")
                params = msg.get("params") or {}
                if req_id is not None and method is None:
                    with self._pending_lock:
                        pending = self._pending.get(req_id)
                        if pending and pending[0] is ws:
                            try:
                                pending[1].put_nowait(msg)
                            except queue.Full:
                                pass
                elif method is not None and req_id is None:
                    callbacks.put((self._deliver_notification, (ws, method, params)))
                elif method is not None:
                    callbacks.put((self._schedule_server_request, (ws, req_id, method, params)))
        except (EOFError, OSError) as e:
            if not ws._closed:
                logger.info("Codex WebSocket reader disconnected: %s", e)
        finally:
            ws.close()
            with self._pending_lock:
                for connection, reply in self._pending.values():
                    if connection is ws:
                        try:
                            reply.put_nowait(ConnectionError("Connection closed by Codex app-server"))
                        except queue.Full:
                            pass
            callbacks.put((self._deliver_notification, (ws, "bridge/disconnected", {"connection": ws})))

    def _schedule_server_request(self, ws: UnixWebSocketConnection, req_id: int | str,
                                 method: str, params: Dict[str, Any]) -> None:
        if ws is not self._ws or ws._closed:
            return
        if method == "item/tool/call":
            threading.Thread(
                target=self._handle_server_request, args=(ws, req_id, method, params),
                name="codex-dynamic-tool", daemon=True,
            ).start()
        else:
            self._handle_server_request(ws, req_id, method, params)

    def _handle_server_request(self, ws: UnixWebSocketConnection, req_id: int | str,
                               method: str, params: Dict[str, Any]) -> None:
        def reply(result=None, error=None):
            self.respond_to_server_request(req_id, result=result, error=error, connection=ws)

        try:
            if self._server_request_handler is not None:
                result = self._server_request_handler(req_id, method, params, reply)
            elif method in ("item/commandExecution/requestApproval", "item/fileChange/requestApproval"):
                result = {"decision": "decline"}
            elif method == "item/permissions/requestApproval":
                result = {"permissions": {}, "scope": "turn"}
            elif method == "item/tool/call":
                result = {"success": False, "contentItems": [
                    {"type": "inputText", "text": "Dynamic tool handler not configured"}
                ]}
            else:
                result = {}
            if result is not None:
                reply(result=result)
        except (OSError, ConnectionError):
            logger.info("Connection closed responding to Codex request %s", req_id)
        except Exception as e:
            logger.exception("Server request handler failed for %s", method)
            try:
                reply(error={"code": -32603, "message": str(e)})
            except OSError:
                pass

    # -------------------------------------------------------------------------
    # High-level Codex App-Server v2 Convenience Methods
    # -------------------------------------------------------------------------

    def thread_start(
        self,
        cwd: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        approval_policy: str = "never",
        sandbox: str = "workspace-write",
        base_instructions: Optional[str] = None,
        developer_instructions: Optional[str] = None,
        dynamic_tools: Optional[List[Dict[str, Any]]] = None,
        ephemeral: bool = False,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "approvalPolicy": approval_policy,
            "sandbox": sandbox,
            "ephemeral": ephemeral,
            "historyMode": "paginated",
        }
        if cwd:
            params["cwd"] = cwd
            params["runtimeWorkspaceRoots"] = [cwd]
        if model:
            params["model"] = model
        if effort:
            params["config"] = {"model_reasoning_effort": effort}
        if base_instructions:
            params["baseInstructions"] = base_instructions
        if developer_instructions:
            params["developerInstructions"] = developer_instructions
        if dynamic_tools:
            params["dynamicTools"] = dynamic_tools
        return self.request("thread/start", params)

    def thread_resume(self, thread_id: str, cwd: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"threadId": thread_id, "excludeTurns": True}
        if cwd:
            params["cwd"] = cwd
        return self.request("thread/resume", params)

    def thread_fork(
        self,
        thread_id: str,
        last_turn_id: Optional[str] = None,
        before_turn_id: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"threadId": thread_id, "excludeTurns": True}
        if last_turn_id:
            params["lastTurnId"] = last_turn_id
        if before_turn_id:
            params["beforeTurnId"] = before_turn_id
        if cwd:
            params["cwd"] = cwd
        return self.request("thread/fork", params)

    def thread_read(self, thread_id: str, include_turns: bool = True) -> Dict[str, Any]:
        return self.request("thread/read", {"threadId": thread_id, "includeTurns": include_turns})

    def thread_revert(self, thread_id: str, before_turn_id: str) -> Dict[str, Any]:
        return self.request("thread/revert", {"threadId": thread_id, "beforeTurnId": before_turn_id})

    def thread_compact_start(self, thread_id: str) -> Dict[str, Any]:
        return self.request("thread/compact/start", {"threadId": thread_id})

    def thread_unsubscribe(self, thread_id: str) -> Dict[str, Any]:
        return self.request("thread/unsubscribe", {"threadId": thread_id})

    def thread_queue_add(self, thread_id: str, text: str) -> Dict[str, Any]:
        return self.request(
            "thread/queue/add",
            {
                "threadId": thread_id,
                "clientUserMessageId": str(uuid.uuid4()),
                "input": [{"type": "text", "text": text, "text_elements": []}],
            },
        )

    def turn_start(
        self,
        thread_id: str,
        prompt_text: str,
        cwd: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        output_schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "threadId": thread_id,
            "input": [{"type": "text", "text": prompt_text, "text_elements": []}],
        }
        if cwd:
            params["cwd"] = cwd
        if model:
            params["model"] = model
        if effort:
            params["effort"] = effort
        if output_schema:
            params["outputSchema"] = output_schema
        return self.request("turn/start", params)

    def turn_steer(
        self,
        thread_id: str,
        expected_turn_id: str,
        steer_text: str,
        additional_context: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "threadId": thread_id,
            "expectedTurnId": expected_turn_id,
            "input": [{"type": "text", "text": steer_text, "text_elements": []}],
        }
        if additional_context:
            params["additionalContext"] = {"agy": {"kind": "untrusted", "value": additional_context}}
        return self.request("turn/steer", params)

    def turn_interrupt(self, thread_id: str, turn_id: str) -> Dict[str, Any]:
        return self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})

    def thread_list(
        self,
        limit: int = 30,
        cursor: Optional[str] = None,
        search_term: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        if search_term:
            params["searchTerm"] = search_term
        if cwd:
            params["cwd"] = cwd
        return self.request("thread/list", params)

    def thread_loaded_list(self) -> Dict[str, Any]:
        return self.request("thread/loaded/list", {})

    def thread_turns_list(
        self,
        thread_id: str,
        limit: int = 20,
        sort_direction: str = "desc",
        items_view: str = "full",
    ) -> Dict[str, Any]:
        return self.request(
            "thread/turns/list",
            {
                "threadId": thread_id,
                "limit": limit,
                "sortDirection": sort_direction,
                "itemsView": items_view,
            },
        )

    @staticmethod
    def sandbox_mode_to_policy(sandbox_mode: str, cwd: Optional[str] = None, network_access: bool = False) -> Dict[str, Any]:
        if sandbox_mode == "danger-full-access":
            return {"type": "dangerFullAccess"}
        if sandbox_mode == "read-only":
            return {"type": "readOnly", "networkAccess": bool(network_access)}
        roots = [cwd] if cwd and os.path.isabs(cwd) else []
        return {
            "type": "workspaceWrite",
            "writableRoots": roots,
            "networkAccess": bool(network_access),
            "excludeTmpdirEnvVar": False,
            "excludeSlashTmp": False,
        }

    def thread_settings_update(
        self,
        thread_id: str,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        approval_policy: Optional[str] = None,
        sandbox_mode: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"threadId": thread_id}
        if model is not None:
            params["model"] = model
        if effort is not None:
            params["effort"] = effort
        if approval_policy in ("never", "on-request", "untrusted"):
            params["approvalPolicy"] = approval_policy
        if sandbox_mode in ("read-only", "workspace-write", "danger-full-access"):
            params["sandboxPolicy"] = self.sandbox_mode_to_policy(sandbox_mode, cwd)
        if cwd:
            params["cwd"] = cwd
        return self.request("thread/settings/update", params)

    def turn_settings_update(
        self,
        thread_id: str,
        turn_id: str,
        model: Optional[str] = None,
        effort: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"threadId": thread_id, "turnId": turn_id}
        if model is not None:
            params["model"] = model
        if effort is not None:
            params["effort"] = effort
        return self.request("turn/settings/update", params)

    def account_rate_limits_read(self) -> Dict[str, Any]:
        """Read current Codex account quota and rate limit windows from the daemon."""
        return self.request("account/rateLimits/read", {}, timeout=10.0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = CodexAppServerClient()
    info = client.connect()
    print("Connected successfully:", json.dumps(info, indent=2))
    loaded = client.request("thread/loaded/list", {})
    print("Loaded threads:", json.dumps(loaded, indent=2))
    client.close()
