# Codex & Unified Subagent Mission Control for Google Antigravity (`agy`)

Integrates **OpenAI Codex (`codex app-server` daemon)** into **Google Antigravity (`agy`)** as a first-class native Subagent (`invoke_subagent(TypeName="codex")`), while providing a **Unified Subagent Mission Control** (MCP Control Plane + Terminal TUI Split-View + Sidecar Auxiliary Pane Web UI) that simultaneously takes over **both** OpenAI Codex subagents and native Gemini subagents (`self`, `research`, custom `.md` agents).

---

## Core Highlights

1. **True Zero-Gemini-Token Hook Interception (`PreInvocation`)**:
   - Subagents invoked via `invoke_subagent(TypeName="codex")` or followed up via `send_message` are intercepted by [`codex_hook_intercept.py`](plugins/codex-subagent/bin/codex_hook_intercept.py) before `ordinaryExecutor` makes a Gemini LLM call.
   - Messages are drained directly from `<appDataDir>/brain/<convId>/.system_generated/messages/undelivered/` and dispatched over WebSocket-over-AF_UNIX (`~/.codex/app-server-control/app-server-control.sock`) to the persistent `codex app-server` daemon, followed by `LanguageServerService/CancelCascadeInvocation(notifyParent=False)` so **0 Gemini tokens** are consumed.
2. **Full Session Intervention & Mid-Turn Steering (`turn/steer`)**:
   - Because tasks run on persistent `codex app-server` threads rather than one-shot CLI processes, you can steer an actively running turn mid-flight (`turn/steer` with `expectedTurnId`), queue follow-up turns (`thread/queue/add`), interrupt (`turn/interrupt`), fork threads (`thread/fork`), revert turns (`thread/revert`), or dynamically hot-swap `model` and `effort` (`low` / `medium` / `high` / `xhigh` / `ultra`).
3. **100% Native `agy` Subagent Experience**:
   - Live Codex events (`item/reasoning/*`, `commandExecution`, `fileChange`, `turn/plan/updated`, `turn/diff/updated`, `item/agentMessage/*`) are mirrored in real time into `<appDataDir>/brain/<convId>/.system_generated/logs/transcript.jsonl` and `conversation_summaries.db`.
   - When a Codex turn finishes (`turn/completed`), [`codex_bridge_daemon.py`](plugins/codex-subagent/bin/codex_bridge_daemon.py) automatically wakes up the parent `agy` conversation via `LanguageServerService/SendAgentMessage`.
4. **Unified Gemini + Codex Subagent Mission Control (3 Surfaces)**:
   - **Terminal `tmux` Split-View TUI (`curses`)**: [`codex_mission_control_tui.py`](plugins/codex-subagent/bin/codex_mission_control_tui.py) opens a `pi`-style right-hand split pane directly inside `tmux` alongside `agy`.
   - **Web / Sidecar Auxiliary Pane UI (`SIDECAR_UI_ENTRYPOINT_AUX_PANE`)**: [`sidecars/subagent_mission_control/`](plugins/codex-subagent/sidecars/subagent_mission_control/) works both inside the Antigravity 2.0 Desktop Auxiliary Pane and as a standalone browser dashboard (`http://127.0.0.1:18766`).
   - **Unified MCP Server (`codex-subagent-control`)**: [`codex_mcp_server.py`](plugins/codex-subagent/bin/codex_mcp_server.py) exposes 8 unified tools (`subagent_list`, `subagent_inspect`, `subagent_steer`, `subagent_interrupt`, `subagent_revert`, `subagent_fork`, `subagent_configure`, `subagent_handoff`) across both Codex and Gemini subagents.

---

## Installation

```bash
git clone git@github.com:ZHDI-1/agy-codex-subagent.git
cd agy-codex-subagent
chmod +x install.sh
./install.sh
```

---

## Usage

### 1. Natural Language Subagent Delegation in `agy`
Ask the main agent naturally:
- *"Spawn a `codex` subagent to audit and fix the race condition in `src/`."*
- *"Steer the running `codex` subagent: do not modify public headers, only internal implementation."*
- *"Hand off the findings from the `research` subagent to a `codex` subagent to implement the fix."*

### 2. `send_message` Control Prefixes
When messaging a `codex` subagent via `send_message(Recipient=<conversationId>, Message=...)`:
- Plain text: Automatically runs `turn/steer` if a turn is active, or `turn/start` on the same persistent `threadId` if idle.
- `[STEER] <instruction>`: Explicitly mid-turn steer the active turn.
- `[QUEUE] <instruction>`: Queue for the next turn (`thread/queue/add`).
- `[INTERRUPT]` or `[INTERRUPT] <new_task>`: Abort current turn (`turn/interrupt`).
- `[REVERT:N]`: Drop and revert the last `N` turns (`thread/revert`).
- `[EFFORT:low|medium|high|xhigh|ultra]` / `[MODEL:<model>]`: Update reasoning effort or model (`turn/settings/update`).
- `[COMPACT]`: Trigger context compaction (`thread/compact/start`).

### 3. Opening the Mission Control UI

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
  - `F` (`Shift+f`): Fork Codex thread (`thread/fork`)
  - `r`: Revert last `N` turns/steps
  - `m`: Change reasoning effort
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
