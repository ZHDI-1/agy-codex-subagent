#!/usr/bin/env python3
"""
Pluggable Subagent Backend Registry (`backend_registry.py`).

Unifies multiple heterogeneous agent protocols behind one extensible registry:
  1. `codex` — OpenAI Codex App-Server Daemon (AF_UNIX WebSocket JSON-RPC 2.0)
  2. `agy`   — Antigravity ACP (`agy acp --stdio` via `GenericAcpClient` preset)
  3. `acp`   — Generic Agent Client Protocol (ANY ACP-compliant agent command:
               `pi-acp`, `claude-code-acp`, `opencode acp`, or custom `acpCommand`)
  4. `pi`    — Native Pi Coding Agent (`pi --mode rpc` with native `steer`/`follow_up`/`abort`)

Adding a future agent backend only requires registering a descriptor + client pool here
(or simply defining a profile with `backend: "acp"` and `acpCommand: [...]` with zero Python changes!).
"""

from __future__ import annotations

import shutil
import threading
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from acp_client import GenericAcpClient
from agy_acp_client import AgyAcpClient
from pi_rpc_client import PiRpcManager


BUILTIN_BACKEND_SPECS: Dict[str, Dict[str, Any]] = {
    "codex": {
        "name": "codex",
        "displayName": "OpenAI Codex App-Server Daemon",
        "protocol": "ws-af-unix-jsonrpc2",
        "capabilities": {
            "midTurnSteer": True,
            "queue": True,
            "interrupt": True,
            "fork": True,
            "revert": True,
            "adoptExisting": True,
            "liveModelSwitch": True,
            "livePermissionSwitch": True,
            "quotaGuard": True,
        },
    },
    "agy": {
        "name": "agy",
        "displayName": "Antigravity ACP (Exact Model Selection)",
        "protocol": "acp-stdio-jsonrpc2",
        "capabilities": {
            "midTurnSteer": True,
            "queue": True,
            "interrupt": True,
            "fork": False,
            "revert": False,
            "adoptExisting": False,
            "liveModelSwitch": True,
            "livePermissionSwitch": False,
            "quotaGuard": False,
            "nativeSystemPrompt": True,
        },
    },
    "acp": {
        "name": "acp",
        "displayName": "Generic ACP Server (pi-acp, claude-code-acp, opencode, custom)",
        "protocol": "acp-stdio-jsonrpc2",
        "capabilities": {
            "midTurnSteer": True,
            "queue": True,
            "interrupt": True,
            "fork": False,
            "revert": False,
            "adoptExisting": False,
            "liveModelSwitch": True,
            "livePermissionSwitch": False,
            "quotaGuard": False,
            "customCommand": True,
        },
    },
    "pi": {
        "name": "pi",
        "displayName": "Pi Coding Agent (pi --mode rpc)",
        "protocol": "pi-stdio-ndjson-rpc",
        "capabilities": {
            "midTurnSteer": True,
            "queue": True,
            "interrupt": True,
            "fork": False,
            "revert": False,
            "adoptExisting": False,
            "liveModelSwitch": True,
            "livePermissionSwitch": False,
            "quotaGuard": False,
            "nativeSystemPrompt": True,
        },
    },
}


class BackendRegistry:
    """Manages reusable client pools for `agy`, generic `acp`, `pi`, and future custom backends."""

    def __init__(
        self,
        *,
        on_acp_notification: Callable[[str, Dict[str, Any]], None],
        on_pi_event: Callable[[str, Dict[str, Any]], None],
    ) -> None:
        self._lock = threading.RLock()
        self._on_acp_notification = on_acp_notification
        self._on_pi_event = on_pi_event
        self._specs: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in BUILTIN_BACKEND_SPECS.items()}

        self.agy: GenericAcpClient = AgyAcpClient()
        self.agy.add_notification_handler(on_acp_notification)

        self._acp_clients: Dict[Tuple[Tuple[str, ...], Tuple[Tuple[str, str], ...]], GenericAcpClient] = {}
        self.pi = PiRpcManager()
        self.pi.add_event_handler(on_pi_event)

    def register_backend_spec(self, name: str, spec: Dict[str, Any]) -> None:
        with self._lock:
            self._specs[name] = dict(spec)

    def get_acp_client(
        self,
        backend: str,
        *,
        acp_command: Optional[Sequence[str]] = None,
        acp_env: Optional[Dict[str, str]] = None,
        acp_mode: Optional[str] = None,
        auto_approve: bool = False,
    ) -> GenericAcpClient:
        if backend == "agy" and not acp_command:
            self.agy.auto_approve = bool(auto_approve)
            return self.agy
        if not acp_command:
            raise ValueError(f"Backend {backend!r} requires a non-empty `acpCommand` (e.g. ['pi-acp'] or ['claude-code-acp'])")
        cmd_tuple = tuple(str(x) for x in acp_command)
        env_tuple = tuple(sorted((str(k), str(v)) for k, v in (acp_env or {}).items()))
        key = (cmd_tuple, env_tuple)
        with self._lock:
            client = self._acp_clients.get(key)
            if client is None:
                client = GenericAcpClient(
                    list(cmd_tuple),
                    env=dict(env_tuple),
                    default_mode=acp_mode,
                    auto_approve=auto_approve,
                    name=f"acp:{cmd_tuple[0]}",
                )
                client.add_notification_handler(self._on_acp_notification)
                self._acp_clients[key] = client
            else:
                client.auto_approve = bool(auto_approve)
            return client

    def list_backends(self, *, codex_connected: bool = False) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        with self._lock:
            specs = list(self._specs.values())
        for spec in specs:
            name = spec["name"]
            if name == "codex":
                available = True
                connected = codex_connected
            elif name == "agy":
                available = bool(shutil.which("agy"))
                connected = self.agy.is_connected()
            elif name == "pi":
                available = bool(shutil.which("pi"))
                connected = bool(self.pi._sessions)
            elif name == "acp":
                available = True
                connected = any(c.is_connected() for c in self._acp_clients.values())
            else:
                available = True
                connected = False
            out.append({**spec, "available": available, "connected": connected})
        return out

    def list_models(self, backend: str) -> List[Dict[str, str]]:
        if backend == "agy":
            return self.agy.list_models()
        if backend == "pi":
            return self.pi.list_models()
        if backend == "acp":
            models: List[Dict[str, str]] = []
            with self._lock:
                clients = list(self._acp_clients.values())
            for c in clients:
                models.extend(c.list_models())
            return models
        return []

    def close_all(self) -> None:
        self.agy.close()
        with self._lock:
            clients = list(self._acp_clients.values())
            self._acp_clients.clear()
        for c in clients:
            c.close()
        self.pi.close_all()
