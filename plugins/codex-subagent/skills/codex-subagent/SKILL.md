---
name: codex-subagent
description: How to orchestrate OpenAI Codex subagents (backed by codex app-server daemon) alongside native Gemini subagents (`self`, `research`), perform mid-turn steering (`turn/steer`), inspect live plans and diffs, fork threads, and execute cross-engine handoffs.
---

# Orchestrating Codex & Unified Subagents

The `codex-subagent` plugin connects Antigravity (`agy`) directly to the local **Codex App-Server Daemon** (`~/.codex/app-server-control/app-server-control.sock`) over a full-duplex Unix domain WebSocket, while unifying both **OpenAI Codex** and **Google Gemini** subagents under a single control plane and Auxiliary Pane UI (**Subagents & Codex**).

## Recommended Workflows

### Workflow 1: Parallel Research (Gemini) -> Steerable Implementation (Codex)
1. Launch a Gemini `research` subagent via `invoke_subagent(TypeName="research", ...)` for fast read-only codebase auditing, OR launch a `codex` subagent directly via `invoke_subagent(TypeName="codex", Workspace="share", ...)`.
2. Once the research subagent reports back (or mid-run), call `subagent_handoff(sourceConversationId=<research_id>, targetEngine="codex", prompt="...")` to spin up a Codex thread pre-loaded with the research findings and workspace root.
3. While Codex works, use `subagent_inspect(conversationId=<codex_id>)` to review its live `plan` checklist and unified `diff`.
4. If you spot an architectural deviation before the turn finishes, inject a zero-downtime correction via `subagent_steer(conversationId=<codex_id>, message="...", mode="steer")` (or `send_message(Recipient=<codex_id>, Message="[STEER] ...")`).

### Workflow 2: Branch Exploration via `subagent_fork`
When a Codex subagent reaches a design fork after Turn 1:
1. Call `subagent_fork(conversationId=<codex_id>, newRole="Codex Approach B", prompt="Try lockless ring buffer approach")`.
2. Compare the diffs of both branches using `subagent_inspect` and revert or kill the losing branch.
