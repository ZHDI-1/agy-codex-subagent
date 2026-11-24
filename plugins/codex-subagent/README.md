# Codex & Unified Subagent Mission Control (`codex-subagent`)

Integrates **OpenAI Codex** (`codex app-server` daemon) into **Google Antigravity (`agy`)** as a first-class native Subagent with:
- **Zero-LLM Proxy Overhead**: `PreInvocation` hook intercepts `codex` subagent turns, drains `<appDataDir>/brain/<convId>/.system_generated/messages/undelivered/`, routes directly over WebSocket-over-AF_UNIX (`~/.codex/app-server-control/app-server-control.sock`) to the Codex App-Server Daemon, and cancels the redundant Gemini step (`0 Gemini tokens`).
- **True Mid-Turn Steering (`turn/steer`)**: `send_message` or `subagent_steer` injects live corrections into an active Codex turn (`expectedTurnId`) or active Gemini subagent without restarting the session.
- **Native `agy` UI & `transcript.jsonl` Mirroring**: Real-time Codex notifications (`reasoning`, `commandExecution`, `fileChange`, `turn/plan/updated`, `turn/diff/updated`) are synthesized into `<appDataDir>/brain/<convId>/.system_generated/logs/transcript.jsonl` and `conversation_summaries.db`.
- **Unified Subagent Mission Control (`tmux` Split-View TUI + Sidecar `AUX_PANE` Web UI)**: Inspect, steer, interrupt, revert, fork, and hand off **both** OpenAI Codex subagents and native Gemini subagents (`self`, `research`, custom `.md` agents).
