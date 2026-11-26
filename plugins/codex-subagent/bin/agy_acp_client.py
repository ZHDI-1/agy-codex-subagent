#!/usr/bin/env python3
"""
Antigravity ACP (`agy acp --stdio`) Client — specialized preset of `GenericAcpClient`.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from typing import Dict, List, Optional

from acp_client import AcpRpcError, GenericAcpClient

logger = logging.getLogger("agy_acp_client")

# Backwards-compatible alias
AgyAcpRpcError = AcpRpcError


class AgyAcpClient(GenericAcpClient):
    """Preset GenericAcpClient configured for Antigravity (`agy acp --stdio`)."""

    def __init__(
        self,
        agy_exe: Optional[str] = None,
        auto_start: bool = True,
        dangerously_skip_permissions: bool = False,
    ) -> None:
        self.agy_exe = agy_exe or shutil.which("agy") or os.path.expanduser("~/.local/bin/agy")
        cmd = [self.agy_exe, "acp", "--stdio"]
        if dangerously_skip_permissions:
            cmd.append("--dangerously-skip-permissions")
        super().__init__(
            cmd,
            auto_start=auto_start,
            default_mode="auto",
            auto_approve=dangerously_skip_permissions,
            name="agy",
            system_prompt_meta_builder=lambda prompt: {"antigravity": {"systemPrompt": {"append": prompt}}},
        )

    def list_models(self) -> List[Dict[str, str]]:
        if self.available_models:
            return list(self.available_models)
        try:
            proc = subprocess.run([self.agy_exe, "models"], capture_output=True, text=True, timeout=15.0)
            if proc.returncode == 0:
                parsed: List[Dict[str, str]] = []
                for line in proc.stdout.splitlines():
                    clean = line.strip()
                    if not clean or clean.startswith("Fetching"):
                        continue
                    parts = clean.split(None, 1)
                    if len(parts) == 2 and "-" in parts[0]:
                        parsed.append({"modelId": parts[0].strip(), "name": parts[1].strip()})
                if parsed:
                    self.available_models = parsed
                    return list(parsed)
        except Exception as e:
            logger.debug("agy models CLI fallback failed: %s", e)
        return []
