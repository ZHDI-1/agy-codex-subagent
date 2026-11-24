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
import json
import logging
import os
import queue
import socket
import struct
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

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
        s.settimeout(self.timeout)
        s.connect(resolved)

        key = base64.b64encode(os.urandom(16)).decode("ascii")
        handshake = (
            "GET / HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ).encode("ascii")
        s.sendall(handshake)

        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = s.recv(4096)
            if not chunk:
                s.close()
                raise ConnectionError("EOF during WebSocket upgrade handshake with Codex daemon")
            resp += chunk

        header_end = resp.index(b"\r\n\r\n") + 4
        header_text = resp[:header_end].decode("latin1", errors="replace")
        self._leftover = resp[header_end:]

        first_line = header_text.splitlines()[0] if header_text else ""
        if "101" not in first_line:
            s.close()
            raise ConnectionError(f"WebSocket upgrade rejected by Codex daemon: {first_line}")

        s.settimeout(None)  # Blocking mode for background reader thread
        self.sock = s
        self._closed = False

    def close(self) -> None:
        self._closed = True
        if self.sock is not None:
            try:
                self._send_frame(0x8, b"")
            except Exception:
                pass
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

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
            if self.sock is None:
                raise ConnectionError("WebSocket is closed")
            self.sock.sendall(frame)

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        if self._leftover:
            take = min(len(self._leftover), n)
            buf.extend(self._leftover[:take])
            self._leftover = self._leftover[take:]
        while len(buf) < n:
            if self.sock is None:
                raise EOFError("Socket closed")
            chunk = self.sock.recv(max(4096, n - len(buf)))
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
        self._pending: Dict[int | str, "queue.Queue[Dict[str, Any]]"] = {}
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
            Callable[[int | str, str, Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None

    def add_notification_handler(self, handler: Callable[[str, Dict[str, Any]], None]) -> None:
        self._notification_handlers.append(handler)

    def set_server_request_handler(
        self,
        handler: Callable[[int | str, str, Dict[str, Any]], Optional[Dict[str, Any]]],
    ) -> None:
        self._server_request_handler = handler

    def is_connected(self) -> bool:
        return self._ws is not None and not self._ws._closed and self._initialized

    def ensure_daemon_running(self) -> None:
        """Verifies control socket exists and is reachable; starts daemon if needed."""
        resolved = os.path.realpath(str(self.sock_path))
        if os.path.exists(resolved):
            try:
                test_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                test_sock.settimeout(2.0)
                test_sock.connect(resolved)
                test_sock.close()
                return
            except Exception as e:
                logger.warning("Existing Codex control socket not connectable (%s), attempting restart...", e)

        if not self.auto_start_daemon:
            raise ConnectionError(f"Codex app-server daemon socket is not running at {self.sock_path}")

        # Try launching codex app-server daemon via codex CLI
        logger.info("Auto-starting codex app-server daemon...")
        try:
            subprocess.Popen(
                ["codex", "app-server", "--remote-control", "--listen", "unix://", "--managed-daemon"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except FileNotFoundError:
            raise RuntimeError("`codex` binary not found in PATH")

        deadline = time.time() + 10.0
        while time.time() < deadline:
            resolved = os.path.realpath(str(self.sock_path))
            if os.path.exists(resolved):
                try:
                    test_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    test_sock.settimeout(1.0)
                    test_sock.connect(resolved)
                    test_sock.close()
                    return
                except Exception:
                    pass
            time.sleep(0.2)
        raise TimeoutError(f"Timed out waiting for codex app-server daemon socket at {self.sock_path}")

    def connect(self) -> Dict[str, Any]:
        with self._conn_lock:
            if self.is_connected():
                return {"userAgent": self.user_agent}

            self.ensure_daemon_running()
            ws = UnixWebSocketConnection(self.sock_path)
            ws.connect()
            self._ws = ws
            self._initialized = False

            self._reader_thread = threading.Thread(
                target=self._reader_loop,
                name="codex-ws-reader",
                daemon=True,
            )
            self._reader_thread.start()

            init_res = self._raw_request(
                "initialize",
                {
                    "clientInfo": {
                        "name": self.client_name,
                        "title": "Antigravity Codex Subagent Bridge",
                        "version": self.client_version,
                    },
                    "capabilities": {
                        "experimentalApi": True,
                    },
                },
                timeout=10.0,
            )
            self.user_agent = init_res.get("userAgent", "")
            self._initialized = True
            logger.info("Connected to Codex App-Server Daemon (%s)", self.user_agent)
            return init_res

    def close(self) -> None:
        with self._conn_lock:
            self._initialized = False
            if self._ws is not None:
                self._ws.close()
                self._ws = None

    def _next_request_id(self) -> int:
        with self._id_lock:
            rid = self._next_id
            self._next_id += 1
            return rid

    def _raw_request(self, method: str, params: Any = None, timeout: float = 60.0) -> Any:
        if self._ws is None or self._ws._closed:
            raise ConnectionError("WebSocket is not connected")

        req_id = self._next_request_id()
        q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[req_id] = q

        msg: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
        }
        if params is not None:
            msg["params"] = params

        try:
            self._ws.send_text(json.dumps(msg))
            resp = q.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(f"Codex RPC method '{method}' (id={req_id}) timed out after {timeout}s")
        finally:
            with self._pending_lock:
                self._pending.pop(req_id, None)

        if "error" in resp and resp["error"] is not None:
            err = resp["error"]
            raise CodexRpcError(
                code=err.get("code", -1),
                message=err.get("message", "Unknown RPC error"),
                data=err.get("data"),
            )
        return resp.get("result")

    def request(self, method: str, params: Any = None, timeout: float = 60.0) -> Any:
        """Sends a JSON-RPC request, auto-connecting/reconnecting if necessary."""
        if not self.is_connected():
            self.connect()
        try:
            return self._raw_request(method, params, timeout=timeout)
        except (ConnectionError, EOFError, BrokenPipeError):
            logger.warning("Connection lost during '%s'; reconnecting once...", method)
            self.close()
            self.connect()
            return self._raw_request(method, params, timeout=timeout)

    def respond_to_server_request(
        self,
        req_id: int | str,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Sends a JSON-RPC 2.0 response back to a ServerRequest initiated by the daemon."""
        if self._ws is None or self._ws._closed:
            raise ConnectionError("WebSocket is not connected")
        payload: Dict[str, Any] = {"jsonrpc": "2.0", "id": req_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result if result is not None else {}
        self._ws.send_text(json.dumps(payload))

    def _reader_loop(self) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            while not ws._closed:
                raw = ws.recv_message()
                if raw is None:
                    break
                try:
                    msg = json.loads(raw)
                except Exception as e:
                    logger.warning("Failed to parse JSON from Codex daemon: %s", e)
                    continue

                if not isinstance(msg, dict):
                    continue

                has_id = "id" in msg and msg["id"] is not None
                has_method = "method" in msg and msg["method"] is not None

                if has_id and not has_method:
                    # Response to our ClientRequest
                    req_id = msg["id"]
                    with self._pending_lock:
                        q = self._pending.get(req_id)
                    if q is not None:
                        try:
                            q.put_nowait(msg)
                        except queue.Full:
                            pass
                elif has_method and not has_id:
                    # ServerNotification
                    method = str(msg["method"])
                    params = msg.get("params") or {}
                    for handler in list(self._notification_handlers):
                        try:
                            handler(method, params)
                        except Exception as e:
                            logger.exception("Error in notification handler for %s: %s", method, e)
                elif has_method and has_id:
                    # ServerRequest (e.g. requestApproval, item/tool/call)
                    req_id = msg["id"]
                    method = str(msg["method"])
                    params = msg.get("params") or {}
                    threading.Thread(
                        target=self._handle_server_request,
                        args=(req_id, method, params),
                        daemon=True,
                    ).start()
        except EOFError:
            logger.info("Codex WebSocket EOF reached")
        except Exception as e:
            if not ws._closed:
                logger.warning("Codex WebSocket reader loop exited: %s", e)
        finally:
            self._initialized = False
            # Wake up any pending callers with a connection error
            with self._pending_lock:
                for rid, q in list(self._pending.items()):
                    try:
                        q.put_nowait(
                            {
                                "id": rid,
                                "error": {"code": -32000, "message": "Connection closed by Codex daemon"},
                            }
                        )
                    except Exception:
                        pass

    def _handle_server_request(self, req_id: int | str, method: str, params: Dict[str, Any]) -> None:
        if self._server_request_handler is not None:
            try:
                res = self._server_request_handler(req_id, method, params)
                if res is not None:
                    self.respond_to_server_request(req_id, result=res)
                return
            except Exception as e:
                logger.exception("Server request handler failed for %s: %s", method, e)
                self.respond_to_server_request(
                    req_id,
                    error={"code": -32603, "message": str(e)},
                )
                return

        # Default safe fallback if no custom handler is registered
        if method in ("item/commandExecution/requestApproval", "item/fileChange/requestApproval"):
            self.respond_to_server_request(req_id, result={"decision": "accept"})
        elif method == "item/permissions/requestApproval":
            self.respond_to_server_request(
                req_id,
                result={"permissions": params.get("permissions", {}), "scope": "turn"},
            )
        elif method == "item/tool/call":
            self.respond_to_server_request(
                req_id,
                result={
                    "success": False,
                    "contentItems": [{"type": "inputText", "text": "Dynamic tool handler not configured"}],
                },
            )
        else:
            self.respond_to_server_request(req_id, result={})

    # -------------------------------------------------------------------------
    # High-level Codex App-Server v2 Convenience Methods
    # -------------------------------------------------------------------------

    def thread_start(
        self,
        cwd: Optional[str] = None,
        model: Optional[str] = None,
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
            "experimentalRawEvents": False,
            "persistExtendedHistory": True,
        }
        if cwd:
            params["cwd"] = cwd
            params["runtimeWorkspaceRoots"] = [cwd]
        if model:
            params["model"] = model
        if base_instructions:
            params["baseInstructions"] = base_instructions
        if developer_instructions:
            params["developerInstructions"] = developer_instructions
        if dynamic_tools:
            params["dynamicTools"] = dynamic_tools
        return self.request("thread/start", params)

    def thread_resume(self, thread_id: str, cwd: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"threadId": thread_id, "persistExtendedHistory": True}
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
        params: Dict[str, Any] = {"threadId": thread_id, "persistExtendedHistory": True}
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
            params["additionalContext"] = additional_context
        return self.request("turn/steer", params)

    def turn_interrupt(self, thread_id: str, turn_id: str) -> Dict[str, Any]:
        return self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = CodexAppServerClient()
    info = client.connect()
    print("Connected successfully:", json.dumps(info, indent=2))
    loaded = client.request("thread/loaded/list", {})
    print("Loaded threads:", json.dumps(loaded, indent=2))
    client.close()
