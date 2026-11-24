# Codex & Unified Subagent Mission Control (`codex-subagent`)

Integrates **OpenAI Codex** (`codex app-server` daemon) into **Google Antigravity (`agy`)** as a first-class native Subagent with:
- **Two execution backends**. The Markdown `PreInvocation` hook and native persistent `sdkExecutor` send work to the same Codex app-server Unix socket. Both support steering and persistent threads.
- **True Mid-Turn Steering (`turn/steer`)**: `send_message` or `subagent_steer` injects live corrections into an active Codex turn (`expectedTurnId`) or active Gemini subagent without restarting the session.
- **Native `agy` UI & `transcript.jsonl` Mirroring**: Real-time Codex notifications (`reasoning`, `commandExecution`, `fileChange`, `turn/plan/updated`, `turn/diff/updated`) are synthesized into `<appDataDir>/brain/<convId>/.system_generated/logs/transcript.jsonl` and `conversation_summaries.db`.
- **Unified Subagent Mission Control (`tmux` Split-View TUI + Sidecar `AUX_PANE` Web UI)**: Inspect, steer, interrupt, revert, fork, and hand off **both** OpenAI Codex subagents and native Gemini subagents (`self`, `research`, custom `.md` agents).


## Execution backends

Run the root installer with `--mode=sdkExecutor` to select Antigravity's persistent executable, or run it with `--mode=preInvocation` to select the hook. The latter is also the default without a flag. The local `config.json` overrides `config.example.json` and is kept out of Git.

- `sdkExecutor` requires Antigravity support for the `AgentScriptItem.commandSpec` JSON definition and `SignalExecutableIdle` language-server RPC. The process stays available for follow-up requests. Native SIGINT maps to Codex `turn/interrupt`. The bridge normally releases the execution lease at terminal completion, with the executor covering idle invocations and HTTP failures.
- `preInvocation` uses a Markdown agent with wider version compatibility. The hook checks the effective definition before claiming input. For a Markdown invocation, it forwards the input and terminates the Gemini invocation before generation. The bridge tracks the Codex turn and sends the parent its report when the turn finishes.

Workspace and global agent files are generated at `.agents/agents/<name>.*` and `~/.gemini/config/agents/<name>.*`. The bundled `agents/codex.md` is the packaged fallback and participates in `agy plugin validate`.

## Codex profile provider

Create or update a profile with `subagent_define_profile` in MCP, **Handoff & Config** in the web UI, or **p** in the TUI. The profile name is the `TypeName` supplied to `invoke_subagent`. Use `subagent_list_profiles` to discover saved types.

```json
{
  "name": "codex-reviewer",
  "description": "Reviews changes for correctness and test gaps",
  "model": "gpt-6.2-sol",
  "effort": "high",
  "systemPrompt": "Review the proposed change for regressions and missing tests.",
  "sandboxMode": "read-only",
  "approvalPolicy": "never",
  "executionMode": "sdkExecutor"
}
```

Omitted settings inherit the local defaults for new profiles and the saved settings when updating profiles. The bridge saves profiles in `codex_subagent_state.json` and `profiles/<name>.json`. It injects the system prompt as Codex developer instructions when starting the thread and prepends it to each new turn's task context. Existing conversations keep their thread settings after a profile edit. New conversations use the saved profile.

The local bridge exposes `GET /api/profiles`, `POST /api/profiles/define`, and `POST /api/profiles/delete`. Delete takes `{"name": "<profile>" }` and removes the generated definitions. Agent definitions owned by another tool or the user cause a conflict during registration.

## Taking Over Existing Standalone Codex Sessions & Managing Permissions

- **Take Over / Adopt Existing Codex Sessions**:
  - Use MCP tools `subagent_list_codex_sessions` and `subagent_adopt_codex_session(threadId="...")` (or `GET /api/codex/threads` + `POST /api/codex/adopt`, press **`t`** in the `tmux` Split-View TUI, click **📥 Take Over Session** in the Sidecar Web UI, or use `[ADOPT:<threadId>]` in `invoke_subagent` / `send_message`).
  - The bridge attaches to the running or persisted thread via `thread/resume`, imports historical turns via `thread/turns/list` into `transcript.jsonl`, and enables full mid-turn steering (`turn/steer`), interruption, fork, and revert.
- **Permission Inheritance (`~/.codex/config.toml`) & One-Click Bypass**:
  - `permissionSource: "codexConfig"` (default): automatically reads `approval_policy` and `sandbox_mode` from `~/.codex/config.toml`.
  - `permissionSource: "bypass"`: forces `approvalPolicy="never"` and `sandboxMode="danger-full-access"` (zero prompts, full access).
  - `permissionSource: "custom"`: allows explicit per-session or per-profile `approvalPolicy` (`never`, `on-request`, `untrusted`, `inherit`) and `sandboxMode` (`danger-full-access`, `workspace-write`, `read-only`, `inherit`), dynamically hot-swappable mid-session via `thread/settings/update` (`subagent_configure`, `[PERMISSION:bypass]`, TUI key **`P`**, or Web UI **⚡ One-Click Bypass Permissions**).
