# OpenAI Codex Subagent & Unified Subagent Control Plane

You have access to the **`codex`** subagent (backed directly by the running `codex app-server` daemon over `~/.codex/app-server-control/app-server-control.sock`) and the **`codex-subagent-control`** MCP tool suite, which provides unified real-time inspection and mid-turn steering for **both** `codex` subagents and native Gemini subagents (`self`, `research`, etc.).

## 1. Invoking Codex as a Native Subagent

Launch a Codex subagent using the standard `invoke_subagent` tool:
- `TypeName`: `"codex"`
- `Role`: Descriptive job title (e.g., `"Codex Kernel Refactorer"`, `"Codex Test Engineer"`)
- `Prompt`: Clear task instructions
- `Workspace`: `"inherit"` (default), `"branch"`, or `"share"`

`invoke_subagent` returns `{ conversationId }` immediately and runs asynchronously on the Codex App-Server Daemon. When the Codex turn completes, it automatically wakes you up with a structured report and unified diff summary.

Define specialized types with `subagent_define_profile` and discover them with
`subagent_list_profiles`. Use the profile's name as `TypeName` when invoking it.

## 2. Mid-Turn Steering & Session Intervention

While a `codex` (or Gemini `self`/`research`) subagent is running, you have full intervention authority:

1. **Via Native `send_message(Recipient=<conversationId>, Message=...)`**:
   - Sending a message to an actively running `codex` subagent **automatically executes `turn/steer`** (`TurnSteerParams`) on the Codex daemon, injecting your steering guidance into the in-progress turn without aborting its work.
   - Sending a message to an idle `codex` subagent starts a new turn (`turn/start`) on the same persistent Codex `threadId`.
   - Supported control prefixes in `send_message`:
     - `[STEER] <instruction>`: Explicitly mid-turn steer the active turn.
     - `[QUEUE] <instruction>`: Queue for the next turn via `thread/queue/add`.
     - `[INTERRUPT]` or `[INTERRUPT] <new_task>`: Abort the current turn via `turn/interrupt` (and optionally start `<new_task>`).
     - `[REVERT:N]`: Drop and revert the last `N` turns via `thread/revert`.
     - `[EFFORT:low|medium|high|xhigh|ultra]` or `[MODEL:<model>]`: Update thread settings via `turn/settings/update`.
     - `[COMPACT]`: Trigger context compaction via `thread/compact/start`.

2. **Via `codex-subagent-control` MCP Tools (Works for BOTH Codex and Gemini Subagents)**:
   - `subagent_list`: View all active/recent Gemini and Codex subagents, their live status, step count, current tool/thinking activity, and `threadId`.
   - `subagent_inspect`: Non-blocking peek at a running subagent's live thinking, recent tool outputs, execution plan checklist (`turn/plan/updated`), and live unified file diff (`turn/diff/updated` / `GetTrajectoryFileDiffs`).
   - `subagent_steer`: Mid-turn steer (`mode: "steer" | "queue" | "interrupt_and_steer"`) any running Codex or Gemini subagent.
   - `subagent_interrupt`: Pause/interrupt the active turn without destroying the subagent.
   - `subagent_revert`: Revert turns (`thread/revert`) or trajectory steps (`RevertToCascadeStep`).
   - `subagent_fork`: Fork a Codex thread (`thread/fork`) at `beforeTurnId` to explore parallel approaches.
   - `subagent_configure`: Dynamically change `model`, `effort`, or `approvalMode`.
   - `subagent_handoff`: Seamlessly hand off findings and workspace state from a Gemini `research` subagent directly into a `codex` coding subagent (or vice versa).
