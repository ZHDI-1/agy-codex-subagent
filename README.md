# Codex & Unified Subagent Mission Control for Google Antigravity (`agy`)

Integrates **OpenAI Codex (`codex app-server` daemon)** into **Google Antigravity (`agy`)** as a first-class native Subagent (`invoke_subagent(TypeName="codex")`), while providing a **Unified Subagent Mission Control** (MCP Control Plane + Terminal TUI Split-View + Sidecar Auxiliary Pane Web UI) that simultaneously takes over **both** OpenAI Codex subagents and native Gemini subagents (`self`, `research`, custom `.md` agents).

---

## Core Highlights

1. **Two Antigravity execution backends**. Both send work over the Codex app-server Unix WebSocket. The persistent `sdkExecutor` uses Antigravity's `commandSpec` lifecycle. The `preInvocation` backend routes a Markdown agent through the hook and stops its Gemini invocation before generation.
2. **Full Session Intervention & Mid-Turn Steering (`turn/steer`)**:
   - Because tasks run on persistent `codex app-server` threads rather than one-shot CLI processes, you can steer an actively running turn mid-flight (`turn/steer` with `expectedTurnId`), queue follow-up turns (`thread/queue/add`), interrupt (`turn/interrupt`), fork threads (`thread/fork`), revert turns (`thread/revert`), or dynamically hot-swap `model` and `effort` (`low` / `medium` / `high` / `xhigh` / `ultra`).
3. **100% Native `agy` Subagent Experience**:
   - Live Codex events (`item/reasoning/*`, `commandExecution`, `fileChange`, `turn/plan/updated`, `turn/diff/updated`, `item/agentMessage/*`) are mirrored in real time into `<appDataDir>/brain/<convId>/.system_generated/logs/transcript.jsonl` and `conversation_summaries.db`.
   - When a Codex turn finishes (`turn/completed`), [`codex_bridge_daemon.py`](plugins/codex-subagent/bin/codex_bridge_daemon.py) automatically wakes up the parent `agy` conversation via `LanguageServerService/SendAgentMessage`.
4. **Unified Gemini + Codex Subagent Mission Control (3 Surfaces)**:
   - **Terminal `tmux` Split-View TUI (`curses`)**: [`codex_mission_control_tui.py`](plugins/codex-subagent/bin/codex_mission_control_tui.py) opens a `pi`-style right-hand split pane directly inside `tmux` alongside `agy`.
   - **Web / Sidecar Auxiliary Pane UI (`SIDECAR_UI_ENTRYPOINT_AUX_PANE`)**: [`sidecars/subagent_mission_control/`](plugins/codex-subagent/sidecars/subagent_mission_control/) works both inside the Antigravity 2.0 Desktop Auxiliary Pane and as a standalone browser dashboard (`http://127.0.0.1:18766`).
   - **Unified MCP Server (`codex-subagent-control`)**: [`codex_mcp_server.py`](plugins/codex-subagent/bin/codex_mcp_server.py) exposes unified session tools (`subagent_list`, `subagent_inspect`, `subagent_steer`, `subagent_interrupt`, `subagent_revert`, `subagent_fork`, `subagent_configure`, `subagent_handoff`) across both Codex and Gemini subagents.

---

## Installation

```bash
git clone git@github.com:ZHDI-1/agy-codex-subagent.git
cd agy-codex-subagent
./install.sh                     # repository default, preInvocation
./install.sh --mode=sdkExecutor  # select the native persistent executor
./install.sh --mode=preInvocation
```

The installer writes the selected mode to `plugins/codex-subagent/config.json` and updates the workspace and global agent definitions. The checked-in [example configuration](plugins/codex-subagent/config.example.json) supplies the defaults. This checkout's local configuration selects `sdkExecutor`. The installer keeps the Markdown definition in the plugin bundle for `agy plugin validate`.

| Mode | How Antigravity runs Codex | When to choose it |
| --- | --- | --- |
| `preInvocation` | Starts a Markdown agent. The hook forwards the input, then terminates the Gemini invocation before generation. | Default for wider compatibility. Requires the PreInvocation hook and uses a handoff outside Antigravity's native execution lease. |
| `sdkExecutor` | Starts a long-lived executable from a JSON agent definition. NDJSON requests reach the same Codex thread, SIGINT interrupts the active turn, and the language server releases the lease on terminal completion. | Choose when your Antigravity build supports `commandSpec` and `SignalExecutableIdle`. Native BUSY/IDLE status and cancellation follow the executor lifecycle. |

A Markdown fallback still uses the hook in SDK mode. When switching modes, start a fresh Antigravity session to load the generated definition. An already-running bridge from the previous plugin version must be restarted to load the new API.

The default Codex settings are `gpt-6.2-sol`, `xhigh`, and `permissionSource: "codexConfig"` (which automatically inherits `approval_policy` and `sandbox_mode` directly from `~/.codex/config.toml`, falling back to `never` approvals and `danger-full-access` sandbox mode). You can also switch any session or profile to `permissionSource: "bypass"` (one-click `never` + `danger-full-access`) or `permissionSource: "custom"` (`read-only` / `workspace-write` / `danger-full-access` and `never` / `on-request` / `untrusted`) live via `thread/settings/update`.

## Usage

### 1. Natural Language Subagent Delegation & Taking Over Existing Codex Sessions
Ask the main agent naturally:
- *"Spawn a `codex` subagent to audit and fix the race condition in `src/`."*
- *"List my existing Codex sessions and take over thread `019d...` with bypass permissions."*
- *"Steer the running `codex` subagent: do not modify public headers, only internal implementation."*
- *"Hand off the findings from the `research` subagent to a `codex` subagent to implement the fix."*

### 2. Define a Codex profile

Call the MCP tool `subagent_define_profile`, open **Handoff & Config** in Mission Control, or press **p** in the TUI. For example, a profile named `codex-reviewer` can use model `gpt-6.2-sol`, effort `high`, `permissionSource: "codexConfig"` (or `"bypass"` / `"custom"`), and the system prompt “Review changes for correctness and test gaps.” Invoke it with `invoke_subagent(TypeName="codex-reviewer")`. The bridge writes native definitions in the workspace and global config. `subagent_list_profiles` returns the saved profiles and the detected `~/.codex/config.toml` settings.

### 3. `send_message` Control Prefixes
When messaging a `codex` subagent via `send_message(Recipient=<conversationId>, Message=...)`:
- Plain text: Automatically runs `turn/steer` if a turn is active, or `turn/start` on the same persistent `threadId` if idle.
- `[STEER] <instruction>`: Explicitly mid-turn steer the active turn.
- `[QUEUE] <instruction>`: Queue for the next turn (`thread/queue/add`).
- `[INTERRUPT]` or `[INTERRUPT] <new_task>`: Abort current turn (`turn/interrupt`).
- `[REVERT:N]`: Drop and revert the last `N` turns (`thread/revert`).
- `[EFFORT:low|medium|high|xhigh|ultra]` / `[MODEL:<model>]`: Update reasoning effort or model (`turn/settings/update`).
- `[PERMISSION:bypass|codexConfig|never:danger-full-access|on-request:workspace-write]`: Dynamically update Codex approval policy and sandbox mode (`thread/settings/update`).
- `[ADOPT:<threadId>] <optional prompt>`: Directly take over an existing standalone Codex session (`thread/resume` + `thread/turns/list`), importing its historical turns into `transcript.jsonl`.
- `[COMPACT]`: Trigger context compaction (`thread/compact/start`).

### 4. Opening the Mission Control UI

#### A. Right-Hand `tmux` Split-View TUI (Recommended for CLI / TUI users)
```bash
python3 plugins/codex-subagent/bin/codex_mission_control_tui.py --split --all
```
- **Hotkeys**:
  - `↑` / `↓`: Select subagent (`[CODEX]` or `[GEMINI]`)
  - `1` / `2` / `3`: Switch tabs (`1:Stream & Steps`, `2:Live Diff`, `3:Plan & Events`)
  - `a`: Toggle scope (`Current Session` vs `All History`)
  - `f`: Cycle engine filter (`ALL` / `CODEX` / `GEMINI`)
  - `s`: Mid-Turn Steer (`turn/steer`)
  - `q`: Queue Next (`thread/queue/add`)
  - `i`: Interrupt active turn
  - `t`: Take Over / Adopt an existing standalone Codex session (`thread/list` + `thread/resume`)
  - `P` (`Shift+p`): Adjust Codex permissions live (`bypass` / `codexConfig` from `~/.codex/config.toml` / custom `approval:sandbox`)
  - `F` (`Shift+f`): Fork Codex thread (`thread/fork`)
  - `r`: Revert last `N` turns/steps
  - `m`: Change reasoning effort
  - `p`: Define or update a Codex profile
  - `h`: Cross-engine handoff (`Gemini research -> Codex coding`)
  - `Q` / `Esc`: Close split view

#### B. Standalone Browser Web UI / Desktop Auxiliary Pane
```bash
ANTIGRAVITY_SIDECAR_WEB_PORT=18766 node plugins/codex-subagent/sidecars/subagent_mission_control/main.mjs
```
Then open **`http://127.0.0.1:18766`** in your browser (or open the **Subagent Mission Control** icon in the Antigravity 2.0 Desktop Auxiliary Pane).

---

## Testing & Verification

```bash
agy plugin validate ./plugins/codex-subagent
python3 ./tests/test_e2e_all_phases.py
```


The suite includes deterministic installer, protocol, and state-machine tests. The live Phase 1–3 and SDK tests use an isolated bridge and fixture database. They require loopback sockets, the local authenticated Codex app-server, and Node.js. The [architecture review](ARCHITECTURE_REVIEW.md) describes the turn and lease boundaries.
