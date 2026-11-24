#!/usr/bin/env python3
"""
Unified Subagent Mission Control — Native Terminal TUI Split-View (`curses`).
Takes over BOTH OpenAI Codex (`codex`) and native Gemini (`self`, `research`, etc.) subagents
in a right-hand tmux split pane alongside the Antigravity (`agy`) TUI.
"""

from __future__ import annotations

import argparse
import curses
import json
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BIN_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN_DIR))

from codex_bridge_daemon import BRIDGE_PORT, ensure_bridge_daemon_running


def fetch_json(url: str, timeout: float = 2.0) -> Dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def post_json(url: str, payload: Dict[str, Any], timeout: float = 10.0) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


class MissionControlTUI:
    def __init__(self, base_url: str, show_all: bool = False) -> None:
        self.base_url = base_url
        self.show_all = show_all
        self.engine_filter = "all"  # "all" | "codex" | "gemini"
        self.active_tab = "stream"  # "stream" | "diff" | "plan"
        self.subagents: List[Dict[str, Any]] = []
        self.selected_idx = 0
        self.selected_conv_id: str = ""
        self.detail: Optional[Dict[str, Any]] = None
        self.scroll_offset = 0
        self.status_banner = "Ready — [s] Steer  [q] Queue  [i] Interrupt  [f] Fork  [r] Revert  [h] Handoff  [p] Profiles"
        self.codex_connected = False
        self.codex_ua = ""
        self.ls_addr = ""
        self.last_refresh = 0.0

    def filtered_subagents(self) -> List[Dict[str, Any]]:
        if self.engine_filter == "all":
            return self.subagents
        return [s for s in self.subagents if s.get("engine") == self.engine_filter]

    def refresh_data(self, force_detail: bool = False) -> None:
        now = time.time()
        try:
            q = "?all=true" if self.show_all else ""
            mc = fetch_json(f"{self.base_url}/api/subagents{q}", timeout=1.5)
            self.codex_connected = bool(mc.get("codexDaemonConnected"))
            self.codex_ua = str(mc.get("codexUserAgent") or "")
            self.ls_addr = str(mc.get("lsAddress") or "")
            self.subagents = mc.get("subagents") or []

            visible = self.filtered_subagents()
            if not visible and not self.show_all and self.subagents == []:
                # Auto fallback to all history if current session has none
                mc_all = fetch_json(f"{self.base_url}/api/subagents?all=true", timeout=1.5)
                if mc_all.get("subagents"):
                    self.show_all = True
                    self.subagents = mc_all.get("subagents") or []
                    visible = self.filtered_subagents()

            if visible:
                if self.selected_conv_id:
                    found_idx = next(
                        (i for i, s in enumerate(visible) if s.get("conversationId") == self.selected_conv_id),
                        -1,
                    )
                    if found_idx >= 0:
                        self.selected_idx = found_idx
                    else:
                        self.selected_idx = min(self.selected_idx, len(visible) - 1)
                        self.selected_conv_id = visible[self.selected_idx]["conversationId"]
                else:
                    self.selected_idx = 0
                    self.selected_conv_id = visible[0]["conversationId"]

                if force_detail or (now - self.last_refresh >= 1.2) or self.detail is None:
                    self.detail = fetch_json(
                        f"{self.base_url}/api/subagent/inspect?conversationId={urllib.parse.quote(self.selected_conv_id)}",
                        timeout=2.0,
                    )
            else:
                self.selected_idx = 0
                self.selected_conv_id = ""
                self.detail = None
            self.last_refresh = now
        except Exception as e:
            self.status_banner = f"Refresh error: {e}"

    def safe_addstr(self, stdscr: Any, y: int, x: int, text: str, attr: int = 0) -> None:
        h, w = stdscr.getmaxyx()
        if y < 0 or y >= h or x >= w:
            return
        max_len = w - x - 1
        if max_len <= 0:
            return
        try:
            stdscr.addstr(y, x, text[:max_len], attr)
        except curses.error:
            pass

    def prompt_input(self, stdscr: Any, prompt_label: str) -> str:
        h, w = stdscr.getmaxyx()
        curses.echo()
        curses.curs_set(1)
        try:
            stdscr.move(h - 1, 0)
            stdscr.clrtoeol()
            self.safe_addstr(stdscr, h - 1, 0, prompt_label, curses.color_pair(3) | curses.A_BOLD)
            stdscr.refresh()
            win = curses.newwin(1, max(10, w - len(prompt_label) - 2), h - 1, len(prompt_label))
            raw = win.getstr(0, 0, 512)
            return raw.decode("utf-8", errors="replace").strip()
        except Exception:
            return ""
        finally:
            curses.noecho()
            curses.curs_set(0)

    def manage_profile(self, stdscr: Any) -> None:
        try:
            result = fetch_json(f"{self.base_url}/api/profiles")
            profiles = {profile["name"]: profile for profile in result["profiles"]}
            names = ", ".join(profiles) or "none"
            name = self.prompt_input(stdscr, f"Profile ({names[:22]}): ")
            if not name:
                return
            current = profiles.get(name, result["defaults"])
            body = {"name": name}
            for key, label in (
                ("description", "Description"), ("model", "Model"), ("effort", "Effort"),
                ("executionMode", "Mode"), ("sandboxMode", "Sandbox"), ("approvalPolicy", "Approval"),
                ("systemPrompt", "System prompt"),
            ):
                value = current.get(key, "")
                answer = self.prompt_input(stdscr, f"{label} [{value[:16]}]: ")
                if answer:
                    body[key] = answer.replace("\\n", "\n") if key == "systemPrompt" else answer
            result = post_json(f"{self.base_url}/api/profiles/define", body)
            self.status_banner = f"Saved profile {result['profile']['name']}"
        except urllib.error.HTTPError as e:
            with e:
                self.status_banner = json.load(e).get("error", str(e))
        except (OSError, KeyError, ValueError) as e:
            self.status_banner = f"Profile update failed: {e}"

    def build_detail_lines(self, width: int) -> List[Tuple[str, int]]:
        lines: List[Tuple[str, int]] = []
        if not self.detail:
            lines.append(("No subagent selected.", curses.A_DIM))
            return lines

        summary = self.detail.get("summary") or {}
        transcript = self.detail.get("transcript") or {}
        engine = str(summary.get("engine") or "gemini").upper()
        eng_color = curses.color_pair(2) if engine == "CODEX" else curses.color_pair(4)

        lines.append(
            (
                f"[{engine}] {summary.get('role') or summary.get('agentName')}  "
                f"status={summary.get('status')}  steps={summary.get('steps')}",
                eng_color | curses.A_BOLD,
            )
        )
        lines.append(
            (
                f"ConvID: {summary.get('conversationId')}  Model: {summary.get('model') or 'default'} "
                f"({summary.get('effort') or 'high'})",
                curses.A_DIM,
            )
        )
        if summary.get("threadId"):
            lines.append(
                (
                    f"Codex Thread: {summary.get('threadId')}  ActiveTurn: {summary.get('activeTurnId') or 'idle'}",
                    curses.color_pair(2),
                )
            )
        lines.append(("─" * max(10, width - 2), curses.A_DIM))

        if self.active_tab == "stream":
            thinking = str(summary.get("lastThinking") or transcript.get("lastThinking") or "").strip()
            if thinking:
                lines.append(("▶ LIVE THINKING / REASONING:", curses.color_pair(3) | curses.A_BOLD))
                for ln in thinking.splitlines()[-12:]:
                    for wrapped in textwrap.wrap(ln, max(20, width - 4)) or [""]:
                        lines.append((f"  {wrapped}", curses.color_pair(3)))
                lines.append(("", 0))

            recent_steps = transcript.get("recentSteps") or []
            lines.append((f"▶ TRAJECTORY STEPS ({len(recent_steps)} recent):", curses.A_BOLD))
            for st in recent_steps[-25:]:
                idx = st.get("stepIndex", st.get("index", "?"))
                stype = str(st.get("type") or "").replace("CORTEX_STEP_TYPE_", "")
                status = str(st.get("status") or "").replace("CORTEX_STEP_STATUS_", "")
                header = f"  #{idx} [{stype}] ({status})"
                hdr_color = curses.color_pair(5) if "ERROR" in stype or "ERROR" in status else curses.color_pair(4)
                lines.append((header, hdr_color | curses.A_BOLD))
                text_body = str(
                    st.get("response")
                    or st.get("content")
                    or st.get("summary")
                    or st.get("error")
                    or st.get("thinking")
                    or ""
                ).strip()
                if text_body:
                    for ln in text_body.splitlines()[:8]:
                        for wrapped in textwrap.wrap(ln, max(20, width - 6)) or [""]:
                            lines.append((f"    {wrapped}", 0))
                for tc in (st.get("tools") or st.get("toolCalls") or []):
                    lines.append((f"    ⚙ tool: {tc}", curses.color_pair(2)))

        elif self.active_tab == "diff":
            diff_text = str(self.detail.get("liveDiff") or "").strip()
            if not diff_text:
                lines.append(("No live file diffs recorded for this subagent.", curses.A_DIM))
            else:
                for ln in diff_text.splitlines():
                    attr = 0
                    if ln.startswith("+") and not ln.startswith("+++"):
                        attr = curses.color_pair(2)
                    elif ln.startswith("-") and not ln.startswith("---"):
                        attr = curses.color_pair(5)
                    elif ln.startswith("@@") or ln.startswith("diff "):
                        attr = curses.color_pair(3) | curses.A_BOLD
                    lines.append((ln[: max(10, width - 2)], attr))

        elif self.active_tab == "plan":
            plan_items = self.detail.get("livePlan") or []
            lines.append(("▶ EXECUTION PLAN CHECKLIST:", curses.A_BOLD))
            if not plan_items:
                lines.append(("  (No structured plan steps emitted yet)", curses.A_DIM))
            for item in plan_items:
                st = item.get("status", "pending")
                mark = "[✓]" if st == "completed" else ("[~]" if st == "inProgress" else "[ ]")
                lines.append((f"  {mark} {item.get('step', '')}", curses.color_pair(2) if st == "completed" else 0))

            lines.append(("", 0))
            lines.append(("▶ RECENT DAEMON / SUBAGENT EVENTS:", curses.A_BOLD))
            for ev in (self.detail.get("recentEvents") or [])[-20:]:
                raw_ts = ev.get("timestamp", "")
                if isinstance(raw_ts, (int, float)):
                    ts = time.strftime("%H:%M:%S", time.localtime(raw_ts))
                else:
                    ts = str(raw_ts)[11:19] or str(raw_ts)
                ev_type = ev.get("type") or ev.get("method") or "event"
                ev_sum = ev.get("summary") or ev.get("command") or ev.get("turnId") or ev.get("itemType") or ""
                lines.append((f"  {ts}  {ev_type}: {ev_sum}", curses.A_DIM))

        return lines

    def run(self, stdscr: Any) -> None:
        curses.curs_set(0)
        stdscr.nodelay(True)
        stdscr.keypad(True)
        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_WHITE, -1)
            curses.init_pair(2, curses.COLOR_GREEN, -1)   # Codex / Success
            curses.init_pair(3, curses.COLOR_YELLOW, -1)  # Running / Thinking / Warning
            curses.init_pair(4, curses.COLOR_CYAN, -1)    # Gemini / Info
            curses.init_pair(5, curses.COLOR_RED, -1)     # Error / Delete

        self.refresh_data(force_detail=True)

        while True:
            now = time.time()
            if now - self.last_refresh >= 1.2:
                self.refresh_data(force_detail=False)

            stdscr.erase()
            h, w = stdscr.getmaxyx()

            # 1. Top Header Bar
            codex_badge = "CODEX:ONLINE" if self.codex_connected else "CODEX:OFFLINE"
            scope_badge = "SCOPE:ALL" if self.show_all else "SCOPE:SESSION"
            header = f" ◈ SUBAGENT MISSION CONTROL (TUI SPLIT) │ {codex_badge} │ {scope_badge} │ Filter:{self.engine_filter.upper()} "
            self.safe_addstr(stdscr, 0, 0, header.ljust(max(0, w - 1)), curses.color_pair(4) | curses.A_REVERSE | curses.A_BOLD)

            # 2. Roster Section (Top pane of split)
            visible = self.filtered_subagents()
            roster_max_rows = min(max(4, h // 4), 10)
            self.safe_addstr(
                stdscr,
                1,
                0,
                f" SUBAGENTS ({len(visible)})  [↑/↓] Select  [a] Toggle All/Session  [f] Filter Engine ",
                curses.A_BOLD,
            )

            if not visible:
                self.safe_addstr(stdscr, 2, 2, "No subagents in current filter. Press [a] to toggle All History.", curses.A_DIM)
            else:
                start_idx = max(0, self.selected_idx - roster_max_rows + 2)
                for row_offset, idx in enumerate(range(start_idx, min(len(visible), start_idx + roster_max_rows - 1))):
                    s = visible[idx]
                    y = 2 + row_offset
                    is_sel = idx == self.selected_idx
                    eng = "CODEX " if s.get("engine") == "codex" else "GEMINI"
                    st = str(s.get("status") or "idle").upper()
                    role = str(s.get("role") or s.get("agentName") or s.get("conversationId"))[:28]
                    steps = s.get("steps", 0)
                    act = str(s.get("activity") or "")[: max(10, w - 58)]
                    prefix = "▸" if is_sel else " "
                    line = f" {prefix} [{eng}] {st:<7} {role:<28} ({steps:>2}st) {act}"
                    attr = curses.A_REVERSE | curses.A_BOLD if is_sel else (
                        curses.color_pair(2) if s.get("engine") == "codex" else curses.color_pair(4)
                    )
                    if st == "RUNNING" and not is_sel:
                        attr = curses.color_pair(3) | curses.A_BOLD
                    self.safe_addstr(stdscr, y, 0, line.ljust(max(0, w - 1)), attr)

            # 3. Tabs Bar
            tab_y = 2 + roster_max_rows
            t1 = "[1:Stream & Steps]" if self.active_tab == "stream" else " 1:Stream & Steps "
            t2 = "[2:Live Diff]" if self.active_tab == "diff" else " 2:Live Diff "
            t3 = "[3:Plan & Events]" if self.active_tab == "plan" else " 3:Plan & Events "
            tab_bar = f" {t1} │ {t2} │ {t3}   (PgUp/PgDn or j/k to scroll) "
            self.safe_addstr(stdscr, tab_y, 0, tab_bar.ljust(max(0, w - 1)), curses.A_UNDERLINE | curses.A_BOLD)

            # 4. Detail Viewport
            detail_start_y = tab_y + 1
            detail_end_y = h - 3
            avail_rows = max(1, detail_end_y - detail_start_y)
            detail_lines = self.build_detail_lines(w)
            max_scroll = max(0, len(detail_lines) - avail_rows)
            self.scroll_offset = max(0, min(self.scroll_offset, max_scroll))

            for i in range(avail_rows):
                line_idx = self.scroll_offset + i
                if line_idx >= len(detail_lines):
                    break
                text, attr = detail_lines[line_idx]
                self.safe_addstr(stdscr, detail_start_y + i, 1, text, attr)

            # 5. Bottom Control & Status Bars
            ctrl_bar = " [s]Steer [q]Queue [i]Interrupt [F]Fork [r]Revert [m]Effort [p]Profiles [h]Handoff [Q]Quit "
            self.safe_addstr(stdscr, h - 2, 0, ctrl_bar.ljust(max(0, w - 1)), curses.color_pair(1) | curses.A_REVERSE)
            self.safe_addstr(stdscr, h - 1, 0, f" {self.status_banner}", curses.color_pair(3))

            stdscr.refresh()

            # 6. Handle Key Input
            ch = stdscr.getch()
            if ch == -1:
                time.sleep(0.06)
                continue

            if ch in (ord("Q"), 27):  # Shift+Q or Esc
                break
            elif ch == curses.KEY_UP:
                if visible:
                    self.selected_idx = max(0, self.selected_idx - 1)
                    self.selected_conv_id = visible[self.selected_idx]["conversationId"]
                    self.scroll_offset = 0
                    self.refresh_data(force_detail=True)
            elif ch == curses.KEY_DOWN:
                if visible:
                    self.selected_idx = min(len(visible) - 1, self.selected_idx + 1)
                    self.selected_conv_id = visible[self.selected_idx]["conversationId"]
                    self.scroll_offset = 0
                    self.refresh_data(force_detail=True)
            elif ch == ord("j"):
                self.scroll_offset += 2
            elif ch == ord("k"):
                self.scroll_offset = max(0, self.scroll_offset - 2)
            elif ch == curses.KEY_NPAGE:
                self.scroll_offset += avail_rows // 2
            elif ch == curses.KEY_PPAGE:
                self.scroll_offset = max(0, self.scroll_offset - avail_rows // 2)
            elif ch == ord("1"):
                self.active_tab = "stream"
                self.scroll_offset = 0
            elif ch == ord("2"):
                self.active_tab = "diff"
                self.scroll_offset = 0
            elif ch == ord("3"):
                self.active_tab = "plan"
                self.scroll_offset = 0
            elif ch == ord("a"):
                self.show_all = not self.show_all
                self.refresh_data(force_detail=True)
                self.status_banner = f"Switched scope to {'All History' if self.show_all else 'Current Session'}"
            elif ch == ord("f"):
                order = ["all", "codex", "gemini"]
                self.engine_filter = order[(order.index(self.engine_filter) + 1) % len(order)]
                self.selected_idx = 0
                self.selected_conv_id = ""
                self.refresh_data(force_detail=True)
                self.status_banner = f"Engine filter: {self.engine_filter.upper()}"
            elif ch in (ord("s"), ord("q")) and self.selected_conv_id:
                mode = "steer" if ch == ord("s") else "queue"
                msg = self.prompt_input(stdscr, f"[{mode.upper()} -> {self.selected_conv_id[:8]}]: ")
                if msg:
                    try:
                        res = post_json(
                            f"{self.base_url}/api/subagent/steer",
                            {"conversationId": self.selected_conv_id, "message": msg, "mode": mode},
                        )
                        self.status_banner = f"Dispatched {res.get('action')} ({res.get('status', 'ok')})"
                        self.refresh_data(force_detail=True)
                    except Exception as e:
                        self.status_banner = f"Steer failed: {e}"
            elif ch == ord("i") and self.selected_conv_id:
                try:
                    res = post_json(
                        f"{self.base_url}/api/subagent/interrupt",
                        {"conversationId": self.selected_conv_id},
                    )
                    self.status_banner = f"Interrupted: {res.get('action')}"
                    self.refresh_data(force_detail=True)
                except Exception as e:
                    self.status_banner = f"Interrupt failed: {e}"
            elif ch == ord("F") and self.selected_conv_id:
                role = self.prompt_input(stdscr, "Fork new role (Enter for default): ")
                try:
                    res = post_json(
                        f"{self.base_url}/api/subagent/fork",
                        {"conversationId": self.selected_conv_id, "newRole": role or "Forked Codex Subagent"},
                    )
                    self.status_banner = f"Forked -> {res.get('newConversationId', '')[:12]}"
                    self.refresh_data(force_detail=True)
                except Exception as e:
                    self.status_banner = f"Fork failed: {e}"
            elif ch == ord("r") and self.selected_conv_id:
                raw_n = self.prompt_input(stdscr, "Revert turns/steps count [default 1]: ")
                n = int(raw_n) if raw_n.isdigit() else 1
                try:
                    res = post_json(
                        f"{self.base_url}/api/subagent/revert",
                        {"conversationId": self.selected_conv_id, "stepOrTurns": n},
                    )
                    self.status_banner = f"Reverted {n} turn(s): {res.get('action')}"
                    self.refresh_data(force_detail=True)
                except Exception as e:
                    self.status_banner = f"Revert failed: {e}"
            elif ch == ord("m") and self.selected_conv_id:
                effort = self.prompt_input(stdscr, "New effort (low/medium/high/xhigh/ultra): ")
                if effort:
                    try:
                        res = post_json(
                            f"{self.base_url}/api/subagent/configure",
                            {"conversationId": self.selected_conv_id, "effort": effort},
                        )
                        self.status_banner = f"Updated effort={effort} ({res.get('action')})"
                        self.refresh_data(force_detail=True)
                    except Exception as e:
                        self.status_banner = f"Config failed: {e}"
            elif ch == ord("p"):
                self.manage_profile(stdscr)
            elif ch == ord("h") and self.selected_conv_id:
                task = self.prompt_input(stdscr, "Handoff task instruction to Codex: ")
                if task:
                    try:
                        res = post_json(
                            f"{self.base_url}/api/subagent/handoff",
                            {
                                "sourceConversationId": self.selected_conv_id,
                                "targetEngine": "codex",
                                "prompt": task,
                            },
                        )
                        self.status_banner = f"Handed off -> Codex conv {res.get('newConversationId', '')[:12]}"
                        self.refresh_data(force_detail=True)
                    except Exception as e:
                        self.status_banner = f"Handoff failed: {e}"


def spawn_tmux_split(script_path: Path, target_pane: Optional[str], width_pct: int, show_all: bool) -> int:
    cmd = ["tmux", "split-window", "-h", "-d", "-p", str(width_pct)]
    if target_pane:
        cmd.extend(["-t", target_pane])
    py_cmd = f"{sys.executable} {script_path}"
    if show_all:
        py_cmd += " --all"
    cmd.append(py_cmd)
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        sys.stderr.write(f"tmux split-window failed: {res.stderr}\n")
    return res.returncode


def main() -> None:
    parser = argparse.ArgumentParser(description="Subagent Mission Control Terminal TUI (Split View)")
    parser.add_argument("--split", action="store_true", help="Open in a right-hand tmux split pane")
    parser.add_argument("--pane", type=str, default="", help="Target tmux pane ID (e.g. %%42)")
    parser.add_argument("--width", type=int, default=45, help="Split pane width percentage (default: 45)")
    parser.add_argument("--all", action="store_true", help="Show all historical Gemini + Codex subagents initially")
    args = parser.parse_args()

    base_url = ensure_bridge_daemon_running(BRIDGE_PORT)

    if args.split:
        sys.exit(spawn_tmux_split(Path(__file__).resolve(), args.pane or None, args.width, args.all))

    tui = MissionControlTUI(base_url=base_url, show_all=args.all)
    curses.wrapper(tui.run)


if __name__ == "__main__":
    main()
