#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="${REPO_ROOT}/plugins/codex-subagent"

echo "[1/3] Updating plugin absolute paths in hooks.json and mcp_config.json to ${PLUGIN_DIR} ..."
cat > "${PLUGIN_DIR}/hooks.json" <<EOF
{
  "codex-subagent-interceptor": {
    "enabled": true,
    "PreInvocation": [
      {
        "type": "command",
        "command": "python3 ${PLUGIN_DIR}/bin/codex_hook_intercept.py",
        "timeout": 25
      }
    ]
  }
}
EOF

cat > "${PLUGIN_DIR}/mcp_config.json" <<EOF
{
  "mcpServers": {
    "codex-subagent-control": {
      "command": "python3",
      "args": [
        "-u",
        "${PLUGIN_DIR}/bin/codex_mcp_server.py"
      ]
    }
  }
}
EOF

echo "[2/3] Linking plugin and codex subagent definition into ~/.gemini/config/ ..."
mkdir -p "${HOME}/.gemini/config/plugins" "${HOME}/.gemini/config/agents"
ln -sfn "${PLUGIN_DIR}" "${HOME}/.gemini/config/plugins/codex-subagent"
ln -sfn "${PLUGIN_DIR}/agents/codex.md" "${HOME}/.gemini/config/agents/codex.md"

echo "[3/3] Validating plugin with agy ..."
if command -v agy >/dev/null 2>&1; then
  agy plugin validate "${PLUGIN_DIR}"
elif [[ -x "${HOME}/.local/bin/agy" ]]; then
  "${HOME}/.local/bin/agy" plugin validate "${PLUGIN_DIR}"
fi

echo "✔ Installed codex-subagent plugin globally!"
