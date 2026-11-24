#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="${REPO_ROOT}/plugins/codex-subagent"
GLOBAL_CONFIG="${AGY_CODEX_GLOBAL_CONFIG:-${HOME}/.gemini/config}"
mode=preInvocation
for arg in "$@"; do
  case "$arg" in
    --mode=preInvocation|--mode=sdkExecutor) mode="${arg#--mode=}" ;;
    -h|--help) printf 'Usage: %s [--mode=preInvocation|--mode=sdkExecutor]\n' "$0"; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$arg" >&2; exit 2 ;;
  esac
done

python3 "${PLUGIN_DIR}/bin/codex_profiles.py" --install-mode "$mode"
mkdir -p "${GLOBAL_CONFIG}/plugins"
ln -sfn "${PLUGIN_DIR}" "${GLOBAL_CONFIG}/plugins/codex-subagent"

if command -v agy >/dev/null 2>&1; then
  agy plugin validate "${PLUGIN_DIR}"
elif [[ -x "${HOME}/.local/bin/agy" ]]; then
  "${HOME}/.local/bin/agy" plugin validate "${PLUGIN_DIR}"
fi
printf 'Installed codex-subagent with %s.\n' "$mode"
