# Codex & Unified Subagent Mission Control (`codex-subagent`)

Integrates **OpenAI Codex** (`codex app-server` daemon over `~/.codex/app-server-control/app-server-control.sock`) into **Google Antigravity (`agy`)** as a first-class native Subagent, while providing a **Unified Subagent Mission Control** (MCP Server + `tmux` Split-View TUI + Sidecar `AUX_PANE` Web UI) for both **OpenAI Codex** and **native Google Gemini** subagents (`self`, `research`, custom `.md` agents).

---

## 1. Architecture & Protocol Flow

Regardless of whether you install with `--mode=sdkExecutor` or `--mode=preInvocation`, **100% of Codex tasks run over the persistent `codex app-server` Unix Domain WebSocket (JSON-RPC 2.0)**:

```text
┌───────────────────────────── Left Side: Antigravity Host ─────────────────────────────┐   ┌──────────────────── Right Side: Codex App-Server Daemon ────────────────────┐
│                                                                                       │   │                                                                             │
│  Antigravity LanguageServer                                                           │   │  codex_bridge_daemon.py (:18765)     codex app-server                       │
│  (invoke_subagent / send_message)                                                     │   │  (UnifiedSubagentBridge)             (~/.codex/.../app-server-control.sock) │
│         │                                                                             │   │         │                                         │                         │
│         ├─► [Mode A: sdkExecutor]   codex_sdk_executor.py   ─┐                        │   │         │   WebSocket over AF_UNIX (JSON-RPC 2.0) │                         │
│         │   • .agents/agents/<name>.json (commandSpec)       │                        │   │         │ ◄─────────────────────────────────────► │                         │
│         │   • stdin NDJSON (init / request)                  ├─► POST /api/dispatch ─►│   │         │   • thread/start, thread/resume         │                         │
│         │   • SIGINT ──► POST /api/subagent/interrupt        │                        │   │         │   • turn/start                          │                         │
│         │   • SignalExecutableIdle on turn/completed         │                        │   │         │   • turn/steer (live mid-turn injection)│                         │
│         │                                                    │                        │   │         │   • turn/interrupt                      │                         │
│         └─► [Mode B: preInvocation] codex_hook_intercept.py ─┘                        │   │         │   • thread/fork, thread/revert          │                         │
│             • .agents/agents/<name>.md + PreInvocation hook                           │   │         │   • thread/settings/update (permissions)│                         │
│             • CancelCascadeInvocation + terminate                                     │   │         │   • thread/loaded/list, thread/list     │                         │
└───────────────────────────────────────────────────────────────────────────────────────┘   └─────────┴─────────────────────────────────────────┴─────────────────────────┘
```

### Execution Backends

Run the root installer with `--mode=sdkExecutor` to select Antigravity's persistent executable adapter, or `--mode=preInvocation` (default) to select the `PreInvocation` hook adapter. The local `config.json` overrides `config.example.json` and is kept out of Git.

- **`sdkExecutor`**: Uses Antigravity's `AgentScriptItem.commandSpec` JSON definition (`codex_sdk_executor.py`) and `SignalExecutableIdle` language-server RPC. The adapter process stays alive across turns, maps `stdin` NDJSON requests to `codex app-server` `turn/start` or `turn/steer`, maps `SIGINT` to Codex `turn/interrupt`, and releases the execution lease when `turn/completed` arrives from the Codex daemon.
- **`preInvocation`**: Uses a Markdown agent definition (`.md`) and the `PreInvocation` hook (`codex_hook_intercept.py`). Before any Gemini LLM generation occurs, the hook forwards the prompt or undelivered `send_message` payload to `codex_bridge_daemon.py`, cancels the Gemini invocation (`CancelCascadeInvocation`), and returns `terminationBehavior: "terminate"`.

---

## 2. Taking Over Existing Standalone Codex Sessions & Managing Permissions

### A. Take Over / Adopt Existing Codex Sessions (`thread/adopt`)
- **MCP**: `subagent_list_codex_sessions` + `subagent_adopt_codex_session(threadId="...", permissionSource="codexConfig" | "bypass", prompt="...")`
- **TUI Split-View**: Press **`t`** (`[t]TakeOver`) to pick from loaded/recent daemon threads by number or `threadId`.
- **Web Sidecar UI**: Click **`📥 Take Over Session`** in the top header.
- **Inline Directive**: Send `[ADOPT:<threadId>] <optional prompt>` via `invoke_subagent` or `send_message`.
- **How it works**: Calls `thread/resume` (`excludeTurns: true`) on `codex app-server` to attach a live WebSocket event listener, backfills historical turns via `thread/turns/list` (`itemsView: "full"`) into `<appDataDir>/brain/<convId>/.system_generated/logs/transcript.jsonl`, and enables full mid-turn steering (`turn/steer`), interruption, fork, and revert.

### B. Flexible Permission Control (`~/.codex/config.toml` & One-Click Bypass)
- **`permissionSource: "codexConfig"` (default)**: Automatically inherits `approval_policy` and `sandbox_mode` directly from `~/.codex/config.toml` (falling back to `never` + `danger-full-access`).
- **`permissionSource: "bypass"`**: Forces `approvalPolicy = "never"` and `sandboxMode = "danger-full-access"` (`{"type": "dangerFullAccess"}`).
- **`permissionSource: "custom"`**: Explicit per-session or per-profile `approvalPolicy` (`never`, `on-request`, `untrusted`, `inherit`) and `sandboxMode` (`danger-full-access`, `workspace-write`, `read-only`, `inherit`).
- **Live Mid-Session Switching**: Updates active threads in-place via `thread/settings/update` using MCP `subagent_configure`, TUI hotkey **`P`** (`Shift+P`), Web UI **`⚡ One-Click Bypass Permissions`**, or `[PERMISSION:bypass]`.

---

## 3. Codex Subagent Profile Provider

Create or update custom Codex subagent types with `subagent_define_profile` in MCP, **Handoff & Config** in the Web UI, or **`p`** in the TUI. The profile name becomes the `TypeName` passed to `invoke_subagent`.

```json
{
  "name": "codex-reviewer",
  "description": "Reviews changes for correctness and test gaps",
  "model": "gpt-6.2-sol",
  "effort": "xhigh",
  "permissionSource": "codexConfig",
  "sandboxMode": "read-only",
  "approvalPolicy": "never",
  "executionMode": "sdkExecutor",
  "systemPrompt": "Review the proposed change for regressions and missing tests."
}
```
