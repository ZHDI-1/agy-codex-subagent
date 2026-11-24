# Codex subagent architecture and state-machine review

The Antigravity conversation owns the persistent Codex thread binding. The bridge owns execution state and turn history. In native SDK mode, Antigravity owns the execution lease and the bridge releases it through the language server after Codex reaches a terminal state.

```text
Antigravity commandSpec -> persistent NDJSON executor --+
                                                       +-> bridge -> Codex app-server
Antigravity Markdown -> PreInvocation hook ------------+

Codex app-server -> WebSocket reader -> waiting RPC caller
                                     -> ordered lifecycle callbacks -> bridge state
                                                                    -> language server idle
                                                                    -> parent report
```

The native request wraps a protojson `SendUserCascadeMessageRequest`. The installed Antigravity 1.5.16 binary confirms that the local SDK process marshals that message and that `SignalExecutableIdleRequest` carries `conversationId`. The executor reads the prompt from text items or FileStore messages. The hook checks the registered SDK process and effective agent definition before reading messages. It cancels the ordinary invocation only on the Markdown path. FileStore markers are acknowledged after the bridge accepts their input. A bounded cache of accepted input IDs handles repeated delivery of the same batch.

Profile definition updates publish an agent in the workspace and global config, with the bridge's persisted profile and matching JSON file. Writes replace individual files atomically. The bridge rolls back changes if a later filesystem operation fails. A new turn uses its conversation's captured profile, and a new conversation reads the current profile. System prompts augment the Codex developer instructions and each new turn's task context.

## State ownership

| State | Owning event and allowed actions |
| --- | --- |
| `idle` | Executor registration can create an idle binding before a thread exists. Dispatch creates or resumes the thread and starts a turn. |
| `starting` | Dispatch publishes and resets live per-turn state before sending `turn/start`. It leaves a terminal state intact if completion arrives before the RPC response. |
| `working` | Either `turn/started` or the start response installs the active turn ID. Follow-ups use that ID as the `turn/steer` precondition. |
| `awaiting_approval` | A server request belongs to the current thread and turn. The turn ID remains active while approvals are pending. |
| `interrupting` | The bridge submitted `turn/interrupt`. It retains the active turn ID while waiting for the matching terminal notification. An interrupt-and-redirect waits before starting a replacement turn. |
| `idle`, `interrupted`, `error` | A matching completion clears the turn, records history, closes turn resources, and mirrors the final step. Failure and restart also expose an error state when the outcome cannot be recovered. |
| `killed` | The bridge clears turn resources and removes the thread routing entry. Later dispatches to the killed conversation are rejected. |

## Concurrency and resource ownership

- The bridge's per-conversation operation lock gives thread publication and turn dispatch one owner. The state lock protects the session and reverse thread maps, persisted snapshots, transcript step allocation, and turn-local approvals. HTTP callers release the state lock before Codex RPC. The callback worker takes the state lock without the operation lock, so it can process a terminal notification while an interrupt-and-redirect waits. The condition variable releases the state lock during that wait.
- The original WebSocket reader executed notification callbacks synchronously. A turn report could then block both unrelated RPC replies and cancellation while waiting for the language server or CLI fallback. The reader now resolves RPC replies directly and queues lifecycle callbacks in wire order. Dynamic tools, terminal reports, and lease release use separate work. Pending RPCs and server-request replies are tied to the socket that issued them. On disconnect, only that socket's waiters fail, its descriptor closes, and the next dispatch resumes the thread. A call whose reply was lost is ambiguous and is never automatically replayed.
- The original bridge published a running turn after every `turn/start` response. A fast completion could therefore be overwritten. Dispatch now reserves state first and commits the returned turn ID only if the reservation is still starting. The active ID and last terminal ID protect the next turn against late and duplicate terminal notifications. A rejected steer falls back to start only when the server reports a lost turn precondition.
- Turn buffers are keyed by thread, turn, and item. Approvals are scoped to the originating conversation and turn, then removed on server resolution, terminal completion, kill, or disconnect. A steering action can still target a turn that is waiting on approval. Reversion prunes durable turn history in the bridge, and forks receive independent transcript, history, approval, and execution state.
- The native executor continues reading stdin during an active turn. SIGINT queues cancellation work in the persistent process. Normal completion schedules idle signaling from the bridge with the credentials for that conversation. The executor checks whether the bridge sent the signal and provides the direct fallback when the call fails or a request produces no turn. Lease release is guarded against an active successor turn. Parent delivery runs independently of lease signaling.
- Bridge startup checks HTTP readiness separately from the Codex handshake. SQLite connections, WebSocket upgrades, child log descriptors, and subprocesses now close on failure as well as success. MCP tool requests run on a bounded worker pool and serialize writes to stdout, allowing protocol replies to proceed during a tool call.

## Verification and limits

`python3 tests/test_e2e_all_phases.py` passed nine deterministic cases covering installer switching, zero-warning plugin validation, native-definition bypass and Markdown fallback, profile routing and persistence, early completion, concurrent steer and interrupt, subprocess NDJSON and SIGINT, WebSocket reader progress, and sidecar JavaScript syntax.

The live class retains the direct Codex WebSocket, Phase 1 hook, Phase 2 control and MCP, and Phase 3 sidecar tests. It also exercises the SDK executable against a real Codex bridge with a fixture language server. This sandbox returned `EPERM` when opening a loopback socket, so the live class skipped. The installer was verified against a temporary checkout and global config. The active checkout's local config now selects SDK mode. Both the workspace and global `codex.json` files point to `codex_sdk_executor.py`.

The language-server idle RPC identifies the conversation only. If a response is lost after the server accepted it, a retry remains ambiguous. Bridge and executor coordinate the normal path, but the protocol has no turn or lease token for strict exactly-once signaling across a disconnect. FileStore acknowledgment and the Codex RPC also span processes. A daemon crash at the boundary can leave a message eligible for redelivery.

The next reading path starts at `dispatch_subagent_input` and `_on_turn_completed` in [the bridge](plugins/codex-subagent/bin/codex_bridge_daemon.py), follows `_reader_loop` in [the WebSocket client](plugins/codex-subagent/bin/codex_ws_client.py), and ends at `SdkExecutor.poll` in [the executable](plugins/codex-subagent/bin/codex_sdk_executor.py).
